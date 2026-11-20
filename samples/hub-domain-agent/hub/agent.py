"""Hub implementation: self-answer, tool selection, and owned specialist sessions."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import AsyncIterator
from contextlib import aclosing, asynccontextmanager
from typing import Any

import anyio

from common.protocol import AI_SCOPE, sse_events
from common.runtime import Runtime
from common.runtime import create_app as runtime_app
from hub.config import HubConfig
from hub.prompts import DOMAIN_DESCRIPTIONS, HUB_PROMPT

logger = logging.getLogger(__name__)


class HubAgent(Runtime):
    def __init__(self, config: HubConfig) -> None:
        super().__init__(config, role="hub", prompt=HUB_PROMPT)
        self.sessions: dict[str, str] = {}
        self.ready: set[str] = set()

    def owned_sessions(self) -> dict[str, str]:
        return self.sessions

    async def auth(self) -> dict[str, str]:
        self.clients()
        token = await self.credential.get_token(AI_SCOPE)
        return {
            "Authorization": "Bearer " + token.token,
            "Foundry-Features": "HostedAgents=V1Preview",
            "x-ms-debug-latency-enabled": "true",
        }

    def domain_base(self, role: str) -> str:
        return self.config.endpoint + "/agents/" + self.config.domains[role].name + "/endpoint"

    async def ensure_domain(self, role: str) -> str:
        if role in self.ready:
            return self.sessions[role]
        assert self.session_id is not None
        # Record ownership before I/O, including indeterminate/cancelled CREATEs.
        sid = self.sessions.setdefault(role, role + "_" + hashlib.sha256(self.session_id.encode()).hexdigest()[:40])
        auth = await self.auth()
        response = await self.http.post(
            self.domain_base(role) + "/sessions",
            params={"api-version": "v1"},
            headers=auth,
            json={
                "agent_session_id": sid,
                "version_indicator": {"type": "version_ref", "agent_version": self.config.domains[role].version},
            },
        )
        if response.status_code == 409:
            response = await self.http.get(
                self.domain_base(role) + "/sessions/" + sid, params={"api-version": "v1"}, headers=auth
            )
        response.raise_for_status()
        resource = response.json()
        if (
            resource.get("status") not in {"active", "idle"}
            or resource["version_indicator"]["agent_version"] != self.config.domains[role].version
        ):
            raise RuntimeError("Domain not ready or pinned version mismatch")
        self.ready.add(role)
        logger.info(
            "DOMAIN_READY role=%s hub_session=%s domain_session=%s start_type=%s",
            role,
            self.session_id,
            sid,
            response.headers.get("x-ms-debug-latency-session-start-type", "unknown"),
        )
        return sid

    async def cleanup(self) -> None:
        errors = []
        for role, sid in list(self.sessions.items()):
            try:
                auth = await self.auth()
                response = await self.http.delete(
                    self.domain_base(role) + "/sessions/" + sid, params={"api-version": "v1"}, headers=auth
                )
                if response.status_code != 404:
                    response.raise_for_status()
                del self.sessions[role]
                self.ready.discard(role)
            except Exception as error:
                errors.append(error)
        if errors:
            raise ExceptionGroup("Owned Domain cleanup failed; IDs retained for explicit cleanup", errors)

    @asynccontextmanager
    async def prefetch(self):
        tasks: dict[str, asyncio.Task] = {}
        try:
            yield tasks
        finally:
            # Starlette disconnect uses level cancellation; own/drain speculation.
            with anyio.CancelScope(shield=True):
                for task in tasks.values():
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks.values(), return_exceptions=True)

    async def domain_reply(self, role: str, text: str, tasks: dict[str, asyncio.Task]) -> AsyncIterator[str]:
        sid = await tasks[role] if role in tasks else await self.ensure_domain(role)
        auth = await self.auth()
        done = False
        async with self.http.stream(
            "POST",
            self.domain_base(role) + "/protocols/invocations",
            params={"api-version": "v1", "agent_session_id": sid},
            headers=auth,
            json={"input": text},
        ) as response:
            response.raise_for_status()
            if not response.headers.get("content-type", "").startswith("text/event-stream"):
                raise RuntimeError("Expected Domain SSE response")
            logger.info(
                "DOMAIN_RESPONSE role=%s hub_session=%s domain_session=%s restore_id=%s history_messages=%s",
                role,
                self.session_id,
                sid,
                response.headers.get("x-sample-restore-id"),
                response.headers.get("x-sample-history-messages"),
            )
            async for event in sse_events(response.aiter_lines()):
                if done:
                    raise RuntimeError("Unexpected Domain event after done")
                if event.get("type") == "output_audio_transcription.delta":
                    delta = event.get("delta")
                    if not isinstance(delta, str):
                        raise RuntimeError("Invalid Domain text delta")
                    yield delta
                elif event.get("type") == "done":
                    done = True
                elif event.get("type") == "error":
                    raise RuntimeError("Domain returned an error")
        if not done:
            raise RuntimeError("Domain stream ended without done")

    async def answer(self, text: str) -> AsyncIterator[str]:
        async with self.prefetch() as tasks:
            options = {
                "tools": [
                    {
                        "type": "function",
                        "name": name,
                        "description": description,
                        "strict": True,
                        "parameters": {
                            "type": "object",
                            "properties": {},
                            "required": [],
                            "additionalProperties": False,
                        },
                    }
                    for name, description in DOMAIN_DESCRIPTIONS.items()
                ],
                "tool_choice": "auto",
                "parallel_tool_calls": False,
            }
            calls, completed, spoken = [], False, False
            async with aclosing(self.model_events(text, **options)) as events:
                async for event in events:
                    if event.type == "response.output_item.added" and event.item.type == "function_call":
                        role = event.item.name
                        if role in self.config.domains and not tasks and not spoken and role not in self.ready:
                            tasks[role] = asyncio.create_task(self.ensure_domain(role), name="domain-prefetch-" + role)
                    elif event.type == "response.output_text.delta":
                        spoken = True
                        yield event.delta
                    elif event.type == "response.output_item.done" and event.item.type == "function_call":
                        calls.append(event.item)
                    elif event.type == "response.completed":
                        completed = True
                    elif event.type in {"response.failed", "response.incomplete", "error"}:
                        raise RuntimeError("Model stream failed or incomplete")
            if not completed:
                raise RuntimeError("Truncated model stream")
            self.last_route = "self"
            if calls:
                if len(calls) != 1 or spoken or calls[0].name not in self.config.domains:
                    raise RuntimeError("Expected one registered tool and no spoken preamble")
                call = calls[0]
                if json.loads(call.arguments) != {}:
                    raise RuntimeError("Domain tool arguments must be the empty object")
                if tasks and call.name not in tasks:
                    raise RuntimeError("Final tool does not match speculative session acquisition")
                self.last_route = call.name
                logger.info("HUB_ROUTE session_id=%s turn=%s route=%s", self.session_id, self.turn, call.name)
                async with aclosing(self.domain_reply(call.name, text, tasks)) as deltas:
                    async for delta in deltas:
                        yield delta


def create_app(config: HubConfig, **kwargs: Any):
    return runtime_app(HubAgent(config), **kwargs)
