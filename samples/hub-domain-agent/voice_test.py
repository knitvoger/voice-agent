"""Serial two-turn WAV test of direct Voice Live → Hub/Domain; no deployments or pool writes."""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import time
import wave
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit

import httpx
from azure.identity.aio import DefaultAzureCredential
from dotenv import load_dotenv
from websockets.asyncio.client import connect

from common.records import write_report
from smoke import auth, cleanup, manifest

CUSTOMER_TIMEOUT_SECONDS = 300
CLEANUP_TIMEOUT_SECONDS = 120


def audio(path: Path) -> bytes:
    with wave.open(str(path), "rb") as wav:
        if (wav.getframerate(), wav.getnchannels(), wav.getsampwidth(), wav.getcomptype()) != (24000, 1, 2, "NONE"):
            raise ValueError("Input must be uncompressed 24kHz mono 16-bit PCM WAV: " + str(path))
        frames = wav.readframes(wav.getnframes())
    if not frames:
        raise ValueError("Empty WAV")
    return frames


def connection_url(endpoint: str, deployment: dict[str, Any], role: str, api_version: str) -> str:
    parsed = urlsplit(endpoint)
    if (
        parsed.scheme != "wss"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path != "/voice-live/realtime"
    ):
        raise ValueError("Use a wss://<account>.services.ai.azure.com/voice-live/realtime URL")
    project = urlsplit(deployment["project_endpoint"]).path.rsplit("/", 1)[-1]
    params = {
        "api-version": api_version,
        "agent-name": deployment["agents"][role]["name"],
        "agent-version": deployment["agents"][role]["version"],
        "agent-project-name": project,
        "features": "hosted_session_warmup:false,hosted_session_shared:false,warmup_llm:false",
    }
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(params), ""))


class Trace:
    """One receiver records IDs in wire order and correlates all response events by ID."""

    def __init__(self) -> None:
        self.session_id: str | None = None
        self.hosted_session_id: str | None = None
        self.updated = False
        self.error: str | None = None
        self.closed = False
        self.changed = asyncio.Event()
        self.turns: list[dict[str, Any]] = []
        self._text_heads: dict[str, str] = {}

    def record(self, event: dict[str, Any], now: float) -> None:
        kind = event.get("type", "")
        if kind == "error" or kind == "conversation.item.input_audio_transcription.failed":
            raise RuntimeError("Voice Live returned an error; no customer retry")
        if kind == "session.created":
            self.session_id = event["session"]["id"]
        if kind == "session.updated":
            self.updated = True
            self.hosted_session_id = (event["session"].get("agent") or {}).get("session_id")
        if kind in {
            "input_audio_buffer.committed",
            "conversation.item.input_audio_transcription.completed",
            "input_audio_buffer.speech_stopped",
        }:
            item_id = event.get("item_id")
            turn = next((value for value in self.turns if value.get("input_item_id") == item_id and item_id), None)
            if turn is None and item_id:
                turn = next((value for value in self.turns if not value.get("input_item_id")), None)
                if turn is not None:
                    turn["input_item_id"] = item_id
            if turn is not None and kind == "input_audio_buffer.speech_stopped":
                turn.setdefault("speech_stopped_at", now)
        if kind == "response.created":
            turn = next((value for value in self.turns if not value.get("response_id")), None)
            if turn is None:
                raise RuntimeError("Response without a pending submitted turn")
            if not turn.get("original_audio_sent"):
                raise RuntimeError("Response arrived before all original WAV audio was sent")
            response_id = event["response"].get("id")
            if (
                not isinstance(response_id, str)
                or not response_id
                or any(value.get("response_id") == response_id for value in self.turns)
            ):
                raise RuntimeError("Missing or duplicate response ID")
            turn["response_id"] = response_id
        rid = event.get("response_id") or (event.get("response") or {}).get("id")
        turn = next((value for value in self.turns if value.get("response_id") == rid and rid), None)
        if turn is not None:
            if kind in {"response.audio.delta", "response.output_audio.delta"}:
                chunk = base64.b64decode(event["delta"], validate=True)
                if chunk:
                    turn.setdefault("first_audio_at", now)
                    turn["audio_bytes"] += len(chunk)
            if kind in {
                "response.audio_transcript.delta",
                "response.output_audio_transcript.delta",
                "response.output_text.delta",
                "response.text.delta",
            } and event.get("delta"):
                turn.setdefault("first_text_at", now)
                head = (self._text_heads.get(rid, "") + event["delta"])[:512]
                self._text_heads[rid] = head
                turn["model_error_reply"] = "error calling model" in head.lower()
            if kind == "response.done":
                turn["status"] = event["response"]["status"]
                if turn["status"] not in {"completed", "cancelled"}:
                    raise RuntimeError("Voice response failed or incomplete")
        self.changed.set()

    async def receive(self, socket: Any) -> None:
        try:
            async for message in socket:
                self.record(json.loads(message), time.perf_counter())
        except Exception as error:
            self.error = type(error).__name__
        finally:
            self.closed = True
            self.changed.set()

    async def wait(self, predicate: Any) -> None:
        async with asyncio.timeout(90):
            while not predicate():
                if self.error:
                    raise RuntimeError(self.error)
                if self.closed:
                    raise RuntimeError("WebSocketClosed")
                self.changed.clear()
                if not predicate():
                    await self.changed.wait()


async def send_audio(socket: Any, frames: bytes, turn: dict[str, Any]) -> None:
    # 50ms chunks; trailing quiet lets server VAD end the turn. No explicit commit/response.create.
    for original, content in ((True, frames), (False, bytes(48000))):
        for offset in range(0, len(content), 2400):
            await socket.send(
                json.dumps(
                    {
                        "type": "input_audio_buffer.append",
                        "audio": base64.b64encode(content[offset : offset + 2400]).decode(),
                    }
                )
            )
            if original and offset + 2400 >= len(content):
                turn["original_audio_sent"] = True
            await asyncio.sleep(0.05)


def validate_turn_ids(turns: list[dict[str, Any]]) -> None:
    for field in ("input_item_id", "response_id"):
        values = [turn.get(field) for turn in turns]
        if not all(isinstance(value, str) and value for value in values) or len(set(values)) != len(values):
            raise RuntimeError("Both turns require nonempty, distinct " + field + " values")


async def drain_owned(task: asyncio.Task) -> None:
    """Await owned, internally bounded teardown even through repeated cancellation."""
    cancelled = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancelled = error
    task.result()
    if cancelled is not None:
        raise cancelled


async def cleanup_customer(deployment: dict[str, Any], role: str, sid: str, credential: Any) -> None:
    async with asyncio.timeout(CLEANUP_TIMEOUT_SECONDS), httpx.AsyncClient(timeout=90) as http:
        await cleanup(http, deployment, role, sid, await auth(credential))


async def customer(
    deployment: dict[str, Any],
    args: Any,
    credential: Any,
    frames: list[bytes],
    result: dict[str, Any],
    checkpoint: Callable[[], None],
) -> None:
    trace = Trace()
    result["success"] = result["cleanup_complete"] = False
    result["turns"] = trace.turns
    voice_complete = False
    try:
        async with asyncio.timeout(CUSTOMER_TIMEOUT_SECONDS):
            headers = await auth(credential)
            url = connection_url(args.endpoint, deployment, args.role, args.api_version)
            async with connect(url, additional_headers=headers, max_size=16 * 1024 * 1024) as socket:
                await exchange(socket, trace, args, frames, result, checkpoint)
            voice_complete = True
    finally:
        result.update(session_id=trace.session_id, hosted_session_id=trace.hosted_session_id)
        for turn in trace.turns:
            for label, field in (("client_ttfa_ms", "first_audio_at"), ("client_ttft_ms", "first_text_at")):
                if field in turn and "speech_stopped_at" in turn:
                    turn[label] = (turn[field] - turn["speech_stopped_at"]) * 1000
        try:
            checkpoint()
        finally:
            if trace.hosted_session_id:
                task = asyncio.create_task(cleanup_customer(deployment, args.role, trace.hosted_session_id, credential))
                try:
                    await drain_owned(task)
                    result["cleanup_complete"] = True
                    result["success"] = voice_complete
                except BaseException as error:
                    result["success"] = False
                    result["cleanup_error_type"] = type(error).__name__
                    raise
                finally:
                    checkpoint()


async def exchange(
    socket: Any, trace: Trace, args: Any, frames: list[bytes], result: dict[str, Any], checkpoint: Callable[[], None]
) -> None:
    receiver = asyncio.create_task(trace.receive(socket))
    try:
        await trace.wait(lambda: trace.session_id is not None)
        result["session_id"] = trace.session_id
        checkpoint()
        await socket.send(
            json.dumps(
                {
                    "type": "session.update",
                    "session": {
                        "modalities": ["text", "audio"],
                        "input_audio_format": "pcm16",
                        "input_audio_sampling_rate": 24000,
                        "output_audio_format": "pcm16",
                        "input_audio_transcription": {"model": "azure-speech", "language": "en-US"},
                        "voice": {"type": "azure-standard", "name": args.voice},
                        "turn_detection": {
                            "type": "azure_semantic_vad_multilingual",
                            "create_response": True,
                            "interrupt_response": True,
                        },
                    },
                }
            )
        )
        await trace.wait(lambda: trace.updated)
        if not trace.hosted_session_id:
            raise RuntimeError("Missing Hosted session ID; cannot verify scoped cleanup")
        result["hosted_session_id"] = trace.hosted_session_id
        checkpoint()  # Durable parent IDs before any original audio is sent.
        for index, content in enumerate(frames):
            turn: dict[str, Any] = {"turn": index + 1, "audio_bytes": 0, "original_audio_sent": False}
            trace.turns.append(turn)
            sender = asyncio.create_task(send_audio(socket, content, turn))
            try:
                if index == 0:
                    await trace.wait(lambda: turn.get("first_audio_at") is not None)
                else:
                    await trace.wait(lambda: turn.get("status") is not None and turn["audio_bytes"] > 0)
            finally:
                sender.cancel()
                outcomes = await asyncio.gather(sender, return_exceptions=True)
                if isinstance(outcomes[0], Exception):
                    raise outcomes[0]
            if not turn["original_audio_sent"]:
                raise RuntimeError("Response arrived before all original WAV audio was sent")
        await trace.wait(lambda: all(value.get("status") for value in trace.turns))
        if trace.error or any(
            value["status"] not in {"completed", "cancelled"} or value.get("model_error_reply") for value in trace.turns
        ):
            raise RuntimeError("Voice test failed; retain original samples")
        if trace.turns[-1]["status"] != "completed":
            raise RuntimeError("Final voice turn did not complete")
        validate_turn_ids(trace.turns)
    finally:

        async def shutdown():
            async with asyncio.timeout(CLEANUP_TIMEOUT_SECONDS):
                try:
                    await socket.close()
                finally:
                    try:
                        await receiver
                    finally:
                        if not receiver.done():
                            receiver.cancel()
                            await asyncio.gather(receiver, return_exceptions=True)
                        result["websocket_close_code"] = socket.close_code
                        if trace.error:
                            result["receiver_error"] = trace.error
                        checkpoint()

        await drain_owned(asyncio.create_task(shutdown()))
        if trace.error or socket.close_code not in {1000, 1001}:
            raise RuntimeError("Voice receiver error or abnormal WebSocket close")


async def run(args: Any) -> None:
    deployment = manifest(args.deployment)
    frames = [audio(args.turn1), audio(args.turn2)]
    args.output.mkdir(parents=True, exist_ok=False)
    report: dict[str, Any] = {
        "role": args.role,
        "agent": deployment["agents"][args.role],
        "audio_sha256": [hashlib.sha256(path.read_bytes()).hexdigest() for path in (args.turn1, args.turn2)],
        "metric_source": "client speech_stopped receipt to first output receipt; NOT native TTFA",
        "turn2_policy": "immediately_after_turn1_first_nonempty_audio",
        "results": [],
    }
    output = args.output / "result.json"

    def checkpoint():
        write_report(output, report)

    checkpoint()
    async with DefaultAzureCredential() as credential:
        for index in range(args.customers):
            result = {"customer": index + 1, "started_at": time.time(), "success": False, "cleanup_complete": False}
            report["results"].append(result)
            checkpoint()
            try:
                await customer(deployment, args, credential, frames, result, checkpoint)
                print(f"Customer {index + 1}/{args.customers}: audio received for both turns")
            except BaseException as error:
                result["success"] = False
                result["error_type"] = type(error).__name__
                raise
            finally:
                result["finished_at"] = time.time()
                checkpoint()


def main() -> None:
    load_dotenv(Path(__file__).with_name(".env"))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment", type=Path, default=Path("deployment.json"))
    parser.add_argument("--role", choices=("hub", "billing", "booking"), default="hub")
    parser.add_argument(
        "--endpoint", default=os.getenv("VOICE_LIVE_ENDPOINT"), required=not os.getenv("VOICE_LIVE_ENDPOINT")
    )
    parser.add_argument("--api-version", default=os.getenv("VOICE_LIVE_API_VERSION", "2026-07-15"))
    parser.add_argument("--voice", default=os.getenv("VOICE_NAME", "en-US-JennyNeural"))
    parser.add_argument("--turn1", type=Path, required=True)
    parser.add_argument("--turn2", type=Path, required=True)
    parser.add_argument("--customers", type=int, default=1)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.customers <= 20:
        parser.error("Use 1–20 strictly serial customers")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
