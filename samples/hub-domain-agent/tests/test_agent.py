"""Real runtime, SSE and SDK routes; only model, identity and Domain network are substituted."""

import asyncio
import json
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock

import anyio
import httpx
import pytest
from azure.ai.agentserver.core import AgentSessionContext
from openai import AsyncOpenAI
from starlette.testclient import TestClient

import agent

ENDPOINT = "https://sample.invalid/api/projects/test"


class Stream:
    def __init__(self, events):
        self.events = events
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True

    async def __aiter__(self):
        for event in self.events:
            yield event


def direct(text="Hello!"):
    return [NS(type="response.output_text.delta", delta=text), NS(type="response.completed")]


def tool(role="billing", arguments="{}"):
    item = NS(type="function_call", id="call-one", name=role, arguments=arguments)
    return [
        NS(type="response.output_item.added", item=item),
        NS(type="response.output_item.done", item=item),
        NS(type="response.completed"),
    ]


def config(role="hub"):
    domains = {name: agent.Domain(name + "-agent", "3") for name in ("billing", "booking")} if role == "hub" else {}
    return agent.Config(ENDPOINT, "text-model", role, domains)


@pytest.fixture
async def runtime():
    value = agent.Agent(config())
    value.session_id = "hub-one"
    value.credential = NS(get_token=AsyncMock(return_value=NS(token="test-token")), close=AsyncMock())
    value.project = NS(close=AsyncMock())
    value.model = NS(responses=NS(create=AsyncMock()), close=AsyncMock())
    value.http = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: pytest.fail("Unexpected external request")))
    yield value
    await value.close_clients()


async def collect(runtime, text="Question"):
    raw = b"".join([part async for part in runtime.stream(text)])
    return [json.loads(frame[6:]) for frame in raw.decode().strip().split("\n\n")]


def install_domain(runtime, *, payload=None, create_status=200, version="3"):
    requests = []

    async def handler(request):
        requests.append(request)
        if request.url.path.endswith("/sessions"):
            return httpx.Response(
                create_status, json={"status": "active", "version_indicator": {"agent_version": version}}
            )
        if request.method == "DELETE":
            return httpx.Response(204)
        content = (
            payload
            if payload is not None
            else agent.sse({"type": "output_audio_transcription.delta", "delta": "30 yuan"})
            + agent.sse({"type": "done"})
        )
        return httpx.Response(200, content=content, headers={"content-type": "text/event-stream"})

    runtime.http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return requests


async def test_self_answer_streams_and_commits_completed_history_without_domain(runtime):
    streams = [Stream(direct()), Stream(direct("How can I help?"))]
    runtime.model.responses.create.side_effect = streams
    assert (await collect(runtime, "Hello"))[-1]["type"] == "done"
    await collect(runtime, "What can you do?")
    assert runtime.last_route == "self" and not runtime.sessions
    assert len(runtime.history) == 4
    assert runtime.model.responses.create.call_args_list[1].kwargs["input"][:2] == runtime.history[:2]
    assert all(stream.closed for stream in streams)
    assert runtime.model.responses.create.call_args.kwargs["parallel_tool_calls"] is False


@pytest.mark.parametrize("role", ["billing", "booking"])
async def test_delegate_preserves_raw_input_and_reuses_version_pinned_domain(runtime, role):
    requests = install_domain(runtime)
    runtime.model.responses.create.side_effect = [Stream(tool(role)), Stream(tool(role))]
    original = "  Explain those extra charges, please!  "
    assert (await collect(runtime, original))[-1]["type"] == "done"
    sid = runtime.sessions[role]
    await collect(runtime, "And the calculation?")
    creates = [request for request in requests if request.url.path.endswith("/sessions")]
    invokes = [request for request in requests if request.url.path.endswith("/invocations")]
    assert len(creates) == 1 and len(invokes) == 2
    assert json.loads(creates[0].content)["version_indicator"]["agent_version"] == "3"
    assert json.loads(invokes[0].content) == {"input": original}
    assert all(request.url.params["agent_session_id"] == sid for request in invokes)
    assert runtime.last_route == role and len(runtime.history) == 4


@pytest.mark.parametrize(
    "events",
    [
        tool(arguments='{"question":"rewritten"}'),
        tool(arguments="[]"),
        tool(arguments="null"),
        tool(arguments="not-json"),
        tool("unknown"),
        tool()[:-1],
        [*direct("Preamble")[:-1], *tool()],
        [*tool()[:-1], *tool()],
        [NS(type="response.failed")],
        [NS(type="response.incomplete")],
    ],
)
async def test_invalid_or_incomplete_tool_output_never_dispatches_business_input(runtime, events):
    requests = install_domain(runtime)
    runtime.model.responses.create.return_value = Stream(events)
    result = await collect(runtime)
    assert result[-1]["type"] == "error"
    assert not runtime.history
    assert not any(request.url.path.endswith("/invocations") for request in requests)


@pytest.mark.parametrize(
    "payload",
    [
        agent.sse({"type": "output_audio_transcription.delta", "delta": "Partial"}),
        b'data: {"type":"done"}',
        agent.sse({"type": "error"}),
        agent.sse({"type": "done"}),
        agent.sse({"type": "done"}) + agent.sse({"type": "output_audio_transcription.delta", "delta": "late"}),
    ],
)
async def test_bad_domain_stream_does_not_commit_history(runtime, payload):
    install_domain(runtime, payload=payload)
    runtime.model.responses.create.return_value = Stream(tool())
    assert (await collect(runtime))[-1]["type"] == "error"
    assert runtime.history == []


async def test_prefetch_overlaps_model_stream_but_business_waits_for_completion(runtime):
    created, finish_model = asyncio.Event(), asyncio.Event()
    requests = []

    async def handler(request):
        requests.append(request)
        if request.url.path.endswith("/sessions"):
            created.set()
            return httpx.Response(200, json={"status": "active", "version_indicator": {"agent_version": "3"}})
        assert finish_model.is_set()
        return httpx.Response(
            200,
            content=agent.sse({"type": "output_audio_transcription.delta", "delta": "Answer"})
            + agent.sse({"type": "done"}),
            headers={"content-type": "text/event-stream"},
        )

    class Gated(Stream):
        async def __aiter__(self):
            yield self.events[0]
            await finish_model.wait()
            for event in self.events[1:]:
                yield event

    runtime.http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    runtime.model.responses.create.return_value = Gated(tool())
    task = asyncio.create_task(collect(runtime))
    await asyncio.wait_for(created.wait(), 5)
    assert len(requests) == 1
    finish_model.set()
    assert (await task)[-1]["type"] == "done"
    assert len(requests) == 2


@pytest.mark.parametrize("level_cancel", [False, True])
async def test_cancellation_drains_prefetch_retains_owned_id_and_does_not_commit(runtime, level_cancel):
    started, cancelled = asyncio.Event(), asyncio.Event()

    async def handler(_):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    class Blocked(Stream):
        async def __aiter__(self):
            yield self.events[0]
            await asyncio.Event().wait()

    runtime.http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    runtime.model.responses.create.return_value = Blocked(tool())
    if level_cancel:
        async with anyio.create_task_group() as group:
            group.start_soon(collect, runtime)
            await asyncio.wait_for(started.wait(), 5)
            group.cancel_scope.cancel()
    else:
        task = asyncio.create_task(collect(runtime))
        await asyncio.wait_for(started.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert cancelled.is_set() and "billing" in runtime.sessions
    assert runtime.history == []


@pytest.mark.parametrize("version,create_status", [("99", 200), ("3", 403), ("3", 500)])
async def test_domain_create_failure_retains_owned_session_and_does_not_dispatch(runtime, version, create_status):
    requests = install_domain(runtime, version=version, create_status=create_status)
    runtime.model.responses.create.return_value = Stream(tool())
    assert (await collect(runtime))[-1]["type"] == "error"
    assert "billing" in runtime.sessions and "billing" not in runtime.ready
    assert all(not request.url.path.endswith("/invocations") for request in requests)


async def test_cleanup_deletes_exact_owned_sessions_only_and_retains_failed_ids(runtime):
    runtime.sessions = {"billing": "owned-billing", "booking": "owned-booking"}
    runtime.ready = {"billing", "booking"}
    urls = []

    def handler(request):
        urls.append(str(request.url))
        return httpx.Response(500 if "owned-billing" in str(request.url) else 404)

    runtime.http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(ExceptionGroup):
        await runtime.cleanup_domains()
    assert runtime.sessions == {"billing": "owned-billing"} and runtime.ready == {"billing"}
    assert len(urls) == 2 and all("owned-" in url for url in urls)


async def test_snapshot_closes_all_clients_and_restore_rebuilds_once(runtime, monkeypatch):
    runtime.session_id = None
    old_model, old_credential, old_http = runtime.model, runtime.credential, runtime.http
    await runtime.before_snapshot()
    assert runtime.captured and runtime.model is runtime.project is runtime.credential is runtime.http is None
    old_model.close.assert_awaited_once()
    old_credential.close.assert_awaited_once()
    assert old_http.is_closed
    credential = NS(close=AsyncMock())
    model = NS(close=AsyncMock())
    project = NS(get_openai_client=MagicMock(return_value=model), close=AsyncMock())
    monkeypatch.setattr(agent, "DefaultAzureCredential", lambda: credential)
    factory = MagicMock(return_value=project)
    monkeypatch.setattr(agent, "AIProjectClient", factory)
    context = AgentSessionContext(
        "restored-session", "restore-1", {"PROJECT_ENDPOINT": "https://new.invalid/api/projects/new"}
    )
    await runtime.after_restore(context)
    await runtime.after_restore(context)
    assert runtime.session_id == "restored-session" and runtime.restore_id == "restore-1"
    assert runtime.history == []
    factory.assert_called_once_with(endpoint="https://new.invalid/api/projects/new", credential=credential)
    with pytest.raises(RuntimeError, match="another session"):
        await runtime.after_restore(AgentSessionContext("other-session", "restore-2"))


@pytest.mark.parametrize("state", ["history", "binding", "session", "busy"])
async def test_capture_rejects_customer_state(runtime, state):
    runtime.session_id = None
    if state == "history":
        runtime.history.append({"role": "user", "content": "fixture"})
    if state == "binding":
        runtime.sessions["billing"] = "owned"
    if state == "session":
        runtime.session_id = "bound"
    if state == "busy":
        await runtime.lock.acquire()
    with pytest.raises(RuntimeError):
        await runtime.before_snapshot()
    assert not runtime.captured


async def test_warmup_is_cancelled_and_drained_before_capture(runtime):
    runtime.session_id = None
    started, stopped = asyncio.Event(), asyncio.Event()

    async def warmup():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    runtime.warmup_task = asyncio.create_task(warmup())
    await started.wait()
    await runtime.before_snapshot()
    assert stopped.is_set() and runtime.warmup_task is None and runtime.captured


def test_real_sdk_snapshot_routes_and_invoke_status(monkeypatch):
    monkeypatch.setenv("FOUNDRY_PROJECT_ENDPOINT", ENDPOINT)
    credential = NS(close=AsyncMock())
    project = NS(get_openai_client=MagicMock(return_value=NS(close=AsyncMock())), close=AsyncMock())
    monkeypatch.setattr(agent, "DefaultAzureCredential", lambda: credential)
    monkeypatch.setattr(agent, "AIProjectClient", lambda **_: project)
    server, runtime = agent.create_app(config(), configure_observability=None)
    with TestClient(server) as client:
        assert client.post("/_before_snapshot", json={}).status_code == 200
        body = {"session_context": {"session_id": "restored-one", "restore_id": "restore-one"}}
        assert client.post("/_after_restore", json=body).status_code == 200
        response = client.post("/invocations", json={"action": "status"})
        assert response.status_code == 200
        assert response.json()["captured"] and response.json()["restore_id"] == "restore-one"
        assert response.json()["history_messages"] == 0
        assert client.post("/invocations", json=[]).status_code == 400
    assert runtime.model is None


async def test_real_openai355_stream_parser_and_runtime_contract(runtime):
    events = [
        {
            "type": "response.output_text.delta",
            "delta": "Hello!",
            "item_id": "message-one",
            "output_index": 0,
            "content_index": 0,
            "logprobs": [],
            "sequence_number": 1,
        },
        {
            "type": "response.completed",
            "sequence_number": 2,
            "response": {
                "id": "response-one",
                "object": "response",
                "created_at": 0,
                "model": "model",
                "status": "completed",
                "output": [],
                "parallel_tool_calls": False,
                "tool_choice": "auto",
                "tools": [],
            },
        },
    ]

    def handler(request):
        assert json.loads(request.content)["store"] is False
        return httpx.Response(
            200, content=b"".join(agent.sse(value) for value in events), headers={"content-type": "text/event-stream"}
        )

    runtime.model = AsyncOpenAI(
        api_key="offline",
        base_url="https://model.invalid/v1",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    assert (await collect(runtime))[-1]["type"] == "done"
    assert runtime.history[-1]["content"] == "Hello!"


async def test_consumer_closing_after_first_delta_closes_model_and_rolls_back(runtime):
    model_stream = Stream(direct("Partial"))
    runtime.model.responses.create.return_value = model_stream
    output = runtime.stream("Question")
    assert b"Partial" in await anext(output)
    await output.aclose()
    assert model_stream.closed and not runtime.history and not runtime.lock.locked()


async def test_final_tool_mismatch_never_dispatches_customer_input(runtime):
    requests = install_domain(runtime)
    events = [tool("billing")[0], *tool("booking")[1:]]
    runtime.model.responses.create.return_value = Stream(events)
    assert (await collect(runtime))[-1]["type"] == "error"
    assert not any(request.url.path.endswith("/invocations") for request in requests)
