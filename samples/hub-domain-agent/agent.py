"""Snapshot-safe Hosted HTTP Hub and Domain agents (Invocations2.0.0)."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import uuid
from collections.abc import AsyncIterator
from contextlib import aclosing, asynccontextmanager
from dataclasses import dataclass, replace
from time import perf_counter
from typing import Any
from urllib.parse import urlsplit

import anyio
import httpx
from azure.ai.agentserver.core import AgentSessionContext
from azure.ai.agentserver.invocations import InvocationAgentServerHost
from azure.ai.projects.aio import AIProjectClient
from azure.identity.aio import DefaultAzureCredential
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse

from prompts import DOMAIN_DESCRIPTIONS, PROMPTS

logger = logging.getLogger(__name__)
TURN_TIMEOUT_SECONDS = 90
HISTORY_MESSAGES = 12
AI_SCOPE = "https://ai.azure.com/.default"


def project_endpoint(value: str) -> str:
    """Validate the project URL before sending any credentials."""
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or not re.fullmatch(r"/api/projects/[^/]+/?", parsed.path)
    ):
        raise ValueError("PROJECT_ENDPOINT must be an HTTPS Foundry /api/projects/<project> URL")
    return value.rstrip("/")


@dataclass(frozen=True)
class Domain:
    name: str
    version: str


@dataclass(frozen=True)
class Config:
    endpoint: str
    model: str
    role: str
    domains: dict[str, Domain]
    warmup: bool = False

    @classmethod
    def from_env(cls) -> Config:
        role = os.environ.get("AGENT_ROLE", "hub")
        if role not in PROMPTS:
            raise ValueError("AGENT_ROLE must be hub, billing, or booking")
        domains = {}
        if role == "hub":
            for name in DOMAIN_DESCRIPTIONS:
                agent = os.environ[name.upper() + "_DOMAIN_AGENT"]
                version = os.environ[name.upper() + "_DOMAIN_VERSION"]
                if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9-]{0,62}", agent) or not version.isdecimal():
                    raise ValueError("Domain agent names and pinned numeric versions are required")
                domains[name] = Domain(agent, version)
        return cls(
            project_endpoint(os.environ["PROJECT_ENDPOINT"]),
            os.environ["MODEL_DEPLOYMENT"],
            role,
            domains,
            role != "hub" and os.environ.get("DOMAIN_MODEL_WARMUP", "true").lower() == "true",
        )


def sse(payload: dict[str, Any]) -> bytes:
    return ("data: " + json.dumps(payload, ensure_ascii=False) + "\n\n").encode()


async def sse_events(lines: AsyncIterator[str]) -> AsyncIterator[dict[str, Any]]:
    """Parse complete SSE frames; a truncated final frame is not success."""
    data: list[str] = []
    async for line in lines:
        if line.startswith("data:"):
            data.append(line[5:].lstrip())
        elif not line and data:
            value = json.loads("\n".join(data))
            if not isinstance(value, dict):
                raise ValueError("Expected an SSE JSON object")
            yield value
            data.clear()
    if data:
        raise RuntimeError("Truncated Domain SSE frame")


class Agent:
    """One exclusive Hosted session/process, with only completed-turn history."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.boot_id = uuid.uuid4().hex
        self.restore_id: str | None = None
        self.session_id: str | None = None
        self.captured = False
        self.history: list[dict[str, str]] = []
        self.sessions: dict[str, str] = {}
        self.ready: set[str] = set()
        self.turn = 0
        self.last_route: str | None = None
        self.lock = asyncio.Lock()
        self.credential: Any = None
        self.project: Any = None
        self.model: Any = None
        self.http: httpx.AsyncClient | None = None
        self.warmup_task: asyncio.Task | None = None

    def clients(self) -> None:
        if self.credential is None:
            self.credential = DefaultAzureCredential()
        if self.project is None:
            self.project = AIProjectClient(endpoint=self.config.endpoint, credential=self.credential)
        if self.model is None:
            self.model = self.project.get_openai_client(max_retries=0, timeout=TURN_TIMEOUT_SECONDS)
        if self.http is None:
            self.http = httpx.AsyncClient(timeout=TURN_TIMEOUT_SECONDS)

    async def close_clients(self) -> None:
        for field in ("http", "model", "project", "credential"):
            client = getattr(self, field)
            if client is not None:
                closer = client.aclose if field == "http" else client.close
                await closer()
                setattr(self, field, None)

    async def warmup(self) -> None:
        try:
            self.clients()
            async with asyncio.timeout(15):
                await self.model.responses.create(
                    model=self.config.model, instructions=PROMPTS[self.config.role], input="Hello", store=False
                )
            logger.info("MODEL_WARMUP role=%s completed=true", self.config.role)
        except Exception as error:
            logger.warning("MODEL_WARMUP role=%s completed=false error_type=%s", self.config.role, type(error).__name__)

    def start_warmup(self) -> None:
        if self.config.warmup:
            self.warmup_task = asyncio.create_task(self.warmup(), name="domain-model-warmup")

    async def quiesce(self) -> None:
        if self.warmup_task is not None:
            self.warmup_task.cancel()
            async with asyncio.timeout(20):
                await asyncio.gather(self.warmup_task, return_exceptions=True)
            self.warmup_task = None

    async def before_snapshot(self) -> None:
        if self.lock.locked() or self.history or self.sessions or self.session_id is not None:
            raise RuntimeError("Refusing reusable snapshot containing customer activity or bindings")
        await self.quiesce()
        await self.close_clients()
        self.captured = True
        logger.info("SNAPSHOT_BEFORE role=%s boot_id=%s clients_closed=true", self.config.role, self.boot_id)

    async def after_restore(self, context: AgentSessionContext) -> None:
        if not self.captured:
            raise RuntimeError("Restore requires a successful capture hook")
        if self.session_id is not None and self.session_id != context.session_id:
            raise RuntimeError("Cannot bind this process to another session")
        if self.restore_id == context.restore_id:
            return
        await self.quiesce()
        await self.close_clients()
        overrides = context.session_env_overrides
        self.config = replace(
            self.config,
            endpoint=project_endpoint(overrides.get("PROJECT_ENDPOINT", self.config.endpoint)),
            model=overrides.get("MODEL_DEPLOYMENT", self.config.model),
        )
        try:
            self.clients()  # Fresh credentials, model/HTTP clients; never reuse captured sockets.
        except BaseException:
            await self.close_clients()
            raise
        self.session_id, self.restore_id = context.session_id, context.restore_id
        self.start_warmup()
        logger.info(
            "SNAPSHOT_AFTER role=%s boot_id=%s restore_id=%s session_id=%s clients_rebuilt=true",
            self.config.role,
            self.boot_id,
            self.restore_id,
            self.session_id,
        )

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

    async def cleanup_domains(self) -> None:
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
        self.clients()
        async with self.prefetch() as tasks:
            arguments: dict[str, Any] = {}
            if self.config.role == "hub":
                arguments.update(
                    tools=[
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
                    tool_choice="auto",
                    parallel_tool_calls=False,
                )
            calls, completed, spoken = [], False, False
            stream = await self.model.responses.create(
                model=self.config.model,
                instructions=PROMPTS[self.config.role],
                input=[*self.history, {"role": "user", "content": text}],
                stream=True,
                store=False,
                **arguments,
            )
            async with stream:
                async for event in stream:
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

    async def stream(self, text: str) -> AsyncIterator[bytes]:
        try:
            async with self.lock, asyncio.timeout(TURN_TIMEOUT_SECONDS):
                self.turn += 1
                started = perf_counter()
                reply = ""
                async with aclosing(self.answer(text)) as deltas:
                    async for delta in deltas:
                        if not reply and delta:
                            logger.info(
                                "FIRST_TEXT session_id=%s turn=%s elapsed_ms=%.2f",
                                self.session_id,
                                self.turn,
                                (perf_counter() - started) * 1000,
                            )
                        reply += delta
                        yield sse({"type": "output_audio_transcription.delta", "delta": delta})
                if not reply:
                    raise RuntimeError("Empty model/Domain answer")
                self.history.extend([{"role": "user", "content": text}, {"role": "assistant", "content": reply}])
                del self.history[:-HISTORY_MESSAGES]
                yield sse({"type": "output_audio_transcription.done", "text": reply})
                yield sse({"type": "done"})
        except Exception as error:
            logger.error(
                "TURN_FAILED role=%s session_id=%s error_type=%s",
                self.config.role,
                self.session_id,
                type(error).__name__,
            )
            yield sse({"type": "error", "error": {"message": "Agent request failed; inspect server diagnostics"}})

    async def invoke(self, request: Request) -> Response:
        try:
            body = await request.json()
        except ValueError:
            return JSONResponse({"error": "Expected a JSON object"}, status_code=400)
        if not isinstance(body, dict):
            return JSONResponse({"error": "Expected a JSON object"}, status_code=400)
        sid = request.state.session_id
        if self.session_id is not None and self.session_id != sid:
            return JSONResponse({"error": "Process already bound to another session"}, status_code=409)
        self.session_id = sid
        if body.get("action") in {"status", "cleanup"}:
            async with self.lock:
                if body["action"] == "cleanup":
                    await self.cleanup_domains()
                return JSONResponse(
                    {
                        "role": self.config.role,
                        "session_id": self.session_id,
                        "boot_id": self.boot_id,
                        "restore_id": self.restore_id,
                        "captured": self.captured,
                        "sessions": self.sessions,
                        "history_messages": len(self.history),
                        "turn": self.turn,
                        "last_route": self.last_route,
                    }
                )
        text = body.get("input") or body.get("message")
        if not isinstance(text, str) or not text.strip():
            return JSONResponse({"error": "Expected nonempty input"}, status_code=400)
        logger.info(
            "INVOKE role=%s session_id=%s restore_id=%s captured=%s history_messages=%s",
            self.config.role,
            self.session_id,
            self.restore_id,
            self.captured,
            len(self.history),
        )
        return StreamingResponse(
            self.stream(text),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                "x-sample-restore-id": self.restore_id or "none",
                "x-sample-history-messages": str(len(self.history)),
            },
        )

    async def shutdown(self) -> None:
        await self.quiesce()
        try:
            async with asyncio.timeout(20):
                await self.cleanup_domains()
        finally:
            await self.close_clients()


def create_app(config: Config, **kwargs: Any) -> tuple[InvocationAgentServerHost, Agent]:
    server, runtime = InvocationAgentServerHost(**kwargs), Agent(config)
    server.invoke_handler(runtime.invoke)
    server.before_snapshot_handler(runtime.before_snapshot)
    server.after_restore_handler(runtime.after_restore)
    server.shutdown_handler(runtime.shutdown)
    return server, runtime


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("azure").setLevel(logging.WARNING)
    server, runtime = create_app(Config.from_env())
    runtime.start_warmup()
    try:
        await server.run_async()
    finally:
        await runtime.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
