"""Real client orchestration with controlled HTTP/WebSocket I/O; no Azure calls."""

import asyncio
import base64
import json
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import httpx
import pytest

import smoke
import voice_test
from common.records import write_report


def deployment():
    return {
        "project_endpoint": "https://sample.invalid/api/projects/test",
        "agents": {role: {"name": role + "-agent", "version": "3"} for role in ("hub", "billing", "booking")},
    }


class Credential:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def get_token(self, _):
        return NS(token="offline-token")


class Socket:
    """A provider seam with explicit event gates, not wall-clock scheduling."""

    def __init__(self, *, close_code=1000, late_error=False):
        self.queue = asyncio.Queue()
        self.close_code = close_code
        self.late_error = late_error
        self.started = False
        self.queue.put_nowait({"type": "session.created", "session": {"id": "voice-owned"}})

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def __aiter__(self):
        while (event := await self.queue.get()) is not None:
            yield json.dumps(event)

    async def send(self, payload):
        assert json.loads(payload)["type"] == "session.update"
        self.queue.put_nowait({"type": "session.updated", "session": {"agent": {"session_id": "hosted-owned"}}})

    async def close(self):
        if self.late_error:
            self.queue.put_nowait({"type": "error"})
        self.queue.put_nowait(None)


def arguments(tmp_path):
    return NS(
        endpoint="wss://sample.invalid/voice-live/realtime",
        role="billing",
        api_version="preview",
        voice="en-US-JennyNeural",
        output=tmp_path / "voice-results",
        customers=2,
        turn1=tmp_path / "one.wav",
        turn2=tmp_path / "two.wav",
        deployment=tmp_path / "deployment.json",
    )


def install_provider(monkeypatch, socket, *, bad_ids=None, early=False, before_audio=None):
    monkeypatch.setattr(voice_test, "connect", lambda *_, **__: socket)

    async def send_audio(_, content, turn):
        index = turn["turn"]
        if before_audio is not None:
            before_audio(index)
        turn["original_audio_sent"] = not early
        if index == 2:
            socket.queue.put_nowait({"type": "response.done", "response": {"id": "response-1", "status": "cancelled"}})
        input_id = "input-1" if bad_ids == "duplicate_input" else "input-" + str(index)
        if bad_ids != "missing_input":
            socket.queue.put_nowait({"type": "input_audio_buffer.committed", "item_id": input_id})
        response_id = "response-1" if bad_ids == "duplicate_response" else "response-" + str(index)
        socket.queue.put_nowait({"type": "response.created", "response": {"id": response_id}})
        socket.queue.put_nowait(
            {"type": "response.audio.delta", "response_id": response_id, "delta": base64.b64encode(b"\0\0").decode()}
        )
        if index == 2:
            socket.queue.put_nowait({"type": "response.done", "response": {"id": response_id, "status": "completed"}})
        await asyncio.Event().wait()  # Represents trailing quiet; cancellation must drain it.

    monkeypatch.setattr(voice_test, "send_audio", send_audio)


async def test_voice_checkpoints_ids_before_audio_and_starts_t2_without_waiting_for_t1_done(tmp_path, monkeypatch):
    output = tmp_path / "result.json"
    result = {"success": False, "cleanup_complete": False}
    socket = Socket()

    def before_audio(index):
        record = json.loads(output.read_text())
        assert record["session_id"] == "voice-owned" and record["hosted_session_id"] == "hosted-owned"
        assert record["success"] is False
        if index == 2:
            assert result["turns"][0]["first_audio_at"] and "status" not in result["turns"][0]

    install_provider(monkeypatch, socket, before_audio=before_audio)
    cleanup = AsyncMock()
    monkeypatch.setattr(voice_test, "cleanup_customer", cleanup)
    await voice_test.customer(
        deployment(), arguments(tmp_path), Credential(), [b"one", b"two"], result, lambda: write_report(output, result)
    )
    assert result["success"] and result["cleanup_complete"] and result["websocket_close_code"] == 1000
    assert [turn["status"] for turn in result["turns"]] == ["cancelled", "completed"]
    cleanup.assert_awaited_once()
    assert cleanup.call_args.args[2] == "hosted-owned"


@pytest.mark.parametrize("bad_ids", ["missing_input", "duplicate_input", "duplicate_response"])
async def test_voice_rejects_invalid_ids_before_reporting_success(tmp_path, monkeypatch, bad_ids):
    install_provider(monkeypatch, Socket(), bad_ids=bad_ids)
    cleanup = AsyncMock()
    monkeypatch.setattr(voice_test, "cleanup_customer", cleanup)
    result = {}
    with pytest.raises(RuntimeError):
        await voice_test.customer(
            deployment(), arguments(tmp_path), Credential(), [b"one", b"two"], result, lambda: None
        )
    assert not result["success"] and result["cleanup_complete"]
    cleanup.assert_awaited_once()


async def test_voice_rejects_a_response_before_original_audio_is_fully_sent(tmp_path, monkeypatch):
    install_provider(monkeypatch, Socket(), early=True)
    monkeypatch.setattr(voice_test, "cleanup_customer", AsyncMock())
    result = {}
    with pytest.raises(RuntimeError):
        await voice_test.customer(
            deployment(), arguments(tmp_path), Credential(), [b"one", b"two"], result, lambda: None
        )
    assert not result["success"] and not result["turns"][0]["original_audio_sent"]
    assert len(result["turns"]) == 1


@pytest.mark.parametrize("close_code,late_error", [(1000, True), (1006, False)])
async def test_voice_rejects_final_receiver_errors_and_abnormal_closes(tmp_path, monkeypatch, close_code, late_error):
    install_provider(monkeypatch, Socket(close_code=close_code, late_error=late_error))
    monkeypatch.setattr(voice_test, "cleanup_customer", AsyncMock())
    result = {}
    with pytest.raises(RuntimeError, match="receiver error or abnormal"):
        await voice_test.customer(
            deployment(), arguments(tmp_path), Credential(), [b"one", b"two"], result, lambda: None
        )
    assert not result["success"] and result["cleanup_complete"] and result["websocket_close_code"] == close_code
    if late_error:
        assert result["receiver_error"] == "RuntimeError"


async def test_success_is_set_only_after_owned_session_cleanup_finishes(tmp_path, monkeypatch):
    install_provider(monkeypatch, Socket())
    started, release = asyncio.Event(), asyncio.Event()
    result = {}

    async def cleanup(*_):
        started.set()
        await release.wait()

    monkeypatch.setattr(voice_test, "cleanup_customer", cleanup)
    task = asyncio.create_task(
        voice_test.customer(deployment(), arguments(tmp_path), Credential(), [b"one", b"two"], result, lambda: None)
    )
    await asyncio.wait_for(started.wait(), 5)
    assert not result["success"] and not result["cleanup_complete"]
    release.set()
    await task
    assert result["success"] and result["cleanup_complete"]


async def test_repeated_cancellation_does_not_abandon_owned_cleanup(tmp_path, monkeypatch):
    install_provider(monkeypatch, Socket())
    started, release, stopped = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def cleanup(*_):
        started.set()
        try:
            await release.wait()
        finally:
            stopped.set()

    monkeypatch.setattr(voice_test, "cleanup_customer", cleanup)
    result = {}
    task = asyncio.create_task(
        voice_test.customer(deployment(), arguments(tmp_path), Credential(), [b"one", b"two"], result, lambda: None)
    )
    await asyncio.wait_for(started.wait(), 5)
    task.cancel()
    await asyncio.sleep(0)  # Deliver cancellation; readiness/release are explicit gates.
    task.cancel()
    await asyncio.sleep(0)
    assert not stopped.is_set() and not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert stopped.is_set() and not result["success"]


async def test_customer_deadline_does_not_cover_session_cleanup(tmp_path, monkeypatch):
    install_provider(monkeypatch, Socket())
    started, release = asyncio.Event(), asyncio.Event()
    timers = []
    real_timeout = asyncio.timeout

    def timeout(seconds):
        timer = real_timeout(None)
        if seconds == voice_test.CUSTOMER_TIMEOUT_SECONDS:
            timers.append(timer)
        return timer

    monkeypatch.setattr(voice_test, "asyncio", NS(**{name: getattr(asyncio, name) for name in dir(asyncio)}))
    monkeypatch.setattr(voice_test.asyncio, "timeout", timeout)

    async def cleanup(*_):
        started.set()
        await release.wait()

    monkeypatch.setattr(voice_test, "cleanup_customer", cleanup)
    result = {}
    task = asyncio.create_task(
        voice_test.customer(deployment(), arguments(tmp_path), Credential(), [b"one", b"two"], result, lambda: None)
    )
    await asyncio.wait_for(started.wait(), 5)
    with pytest.raises(RuntimeError, match="finished"):
        timers[0].reschedule(asyncio.get_running_loop().time() - 1)
    release.set()
    await task
    assert result["success"] and result["cleanup_complete"]


async def test_customer_timeout_during_audio_still_drains_receiver_and_cleans_parent(tmp_path, monkeypatch):
    socket = Socket()
    monkeypatch.setattr(voice_test, "connect", lambda *_, **__: socket)
    audio_started = asyncio.Event()
    timer = asyncio.timeout(None)
    real_timeout = asyncio.timeout
    monkeypatch.setattr(voice_test, "asyncio", NS(**{name: getattr(asyncio, name) for name in dir(asyncio)}))
    monkeypatch.setattr(
        voice_test.asyncio, "timeout", lambda seconds: timer if seconds == 300 else real_timeout(seconds)
    )

    async def send_audio(*_):
        audio_started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(voice_test, "send_audio", send_audio)
    cleanup = AsyncMock()
    monkeypatch.setattr(voice_test, "cleanup_customer", cleanup)
    result = {}
    task = asyncio.create_task(
        voice_test.customer(deployment(), arguments(tmp_path), Credential(), [b"one", b"two"], result, lambda: None)
    )
    await asyncio.wait_for(audio_started.wait(), 5)
    timer.reschedule(asyncio.get_running_loop().time() - 1)
    with pytest.raises(TimeoutError):
        await task
    cleanup.assert_awaited_once()
    assert result["cleanup_complete"] and not result["success"] and result["websocket_close_code"] == 1000


async def test_unavailable_hub_is_not_treated_as_successful_child_cleanup():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ExceptionGroup, match="Cleanup incomplete"):
            await smoke.cleanup(client, deployment(), "hub", "owned", {})
    assert len(requests) == 1 and requests[0].method == "GET"


async def test_cleanup_has_an_independent_deadline(monkeypatch):
    started = asyncio.Event()
    timer = asyncio.timeout(None)
    monkeypatch.setattr(voice_test, "asyncio", NS(timeout=lambda _: timer))

    async def cleanup(*_):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(voice_test, "cleanup", cleanup)
    task = asyncio.create_task(voice_test.cleanup_customer(deployment(), "billing", "owned", Credential()))
    await asyncio.wait_for(started.wait(), 5)
    timer.reschedule(asyncio.get_running_loop().time() - 1)
    with pytest.raises(TimeoutError):
        await task


async def test_cleanup_failure_stops_the_cohort_and_persists_failure(tmp_path, monkeypatch):
    args = arguments(tmp_path)
    args.deployment.write_text(json.dumps(deployment()))
    args.turn1.write_bytes(b"one")
    args.turn2.write_bytes(b"two")
    monkeypatch.setattr(voice_test, "audio", lambda _: b"original-pcm")
    monkeypatch.setattr(voice_test, "DefaultAzureCredential", Credential)
    install_provider(monkeypatch, Socket())
    cleanup = AsyncMock(side_effect=RuntimeError("controlled cleanup failure"))
    monkeypatch.setattr(voice_test, "cleanup_customer", cleanup)
    with pytest.raises(RuntimeError, match="controlled cleanup"):
        await voice_test.run(args)
    report = json.loads((args.output / "result.json").read_text())
    assert len(report["results"]) == 1  # No second billed customer or replacement sample.
    result = report["results"][0]
    assert not result["success"] and not result["cleanup_complete"]
    assert result["hosted_session_id"] == "hosted-owned" and result["error_type"] == "RuntimeError"


async def test_smoke_persists_planned_id_before_create_and_keeps_it_after_failure(tmp_path, monkeypatch):
    output = tmp_path / "smoke.json"
    requests = []
    deleted = False

    def handler(request):
        nonlocal deleted
        requests.append(request)
        record = json.loads(output.read_text())
        assert record["session_id"]
        if request.method == "POST":
            assert json.loads(request.content)["agent_session_id"] == record["session_id"]
            assert record["cleanup_complete"] is False
            return httpx.Response(500)
        if request.method == "DELETE":
            deleted = True
            return httpx.Response(204)
        return httpx.Response(404 if deleted else 200, json={"version_indicator": {"agent_version": "3"}})

    real_client = httpx.AsyncClient
    monkeypatch.setattr(smoke, "httpx", NS(AsyncClient=lambda **_: real_client(transport=httpx.MockTransport(handler))))
    monkeypatch.setattr(smoke, "DefaultAzureCredential", Credential)
    with pytest.raises(httpx.HTTPStatusError):
        await smoke.run(deployment(), "billing", ["one", "two"], output)
    record = json.loads(output.read_text())
    assert record["session_id"] and record["cleanup_complete"]
    assert [request.method for request in requests] == ["POST", "GET", "DELETE", "GET"]


def test_failed_atomic_report_write_leaves_the_previous_record_intact(tmp_path):
    output = tmp_path / "report.json"
    write_report(output, {"session_id": "owned"})
    with pytest.raises(TypeError):
        write_report(output, {"invalid": object()})
    assert json.loads(output.read_text()) == {"session_id": "owned"}
    assert list(tmp_path.iterdir()) == [output]


async def test_audio_completion_tracks_original_frames_separately_from_trailing_silence(monkeypatch):
    sent, trailing = [], asyncio.Event()
    release = asyncio.Event()
    turn = {"original_audio_sent": False}

    async def send(payload):
        content = base64.b64decode(json.loads(payload)["audio"])
        sent.append(content)
        if len(sent) == 3:
            assert turn["original_audio_sent"]
            trailing.set()
            await release.wait()

    async def yield_only(_):
        await asyncio.sleep(0)

    monkeypatch.setattr(voice_test, "asyncio", NS(sleep=yield_only))
    task = asyncio.create_task(voice_test.send_audio(NS(send=send), b"a" * 2400 + b"b" * 10, turn))
    await asyncio.wait_for(trailing.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sent[:2] == [b"a" * 2400, b"b" * 10] and turn["original_audio_sent"]
