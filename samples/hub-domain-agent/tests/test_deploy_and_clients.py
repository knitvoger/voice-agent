"""Deployment serialization, ownership guards and exact voice-event correlation; no cloud traffic."""

import base64
import json
import wave
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from azure.core.exceptions import ResourceNotFoundError

import common.runtime as shared
import deploy
import export_native_query
import smoke
import voice_test
from billing.agent import BillingAgent
from common.config import Config, project_endpoint

ENDPOINT = "https://sample.invalid/api/projects/test"
IMAGE = "registry.invalid/sample@sha256:" + "a" * 64
IMAGES = {
    "hub": IMAGE,
    "billing": "registry.invalid/billing@sha256:" + "b" * 64,
    "booking": "registry.invalid/booking@sha256:" + "c" * 64,
}


def deployment():
    return {
        "project_endpoint": ENDPOINT,
        "agents": {role: {"name": role + "-agent", "version": "3"} for role in ("hub", "billing", "booking")},
    }


def test_typed_definition_serializes_snapshot_protocol_and_domain_versions():
    body = deploy.definition(ENDPOINT, IMAGE, "my-deployment", "hub", deployment()["agents"]).as_dict()
    assert body["enable_memory_snapshot"] is True
    assert body["protocol_versions"] == [{"protocol": "invocations", "version": "2.0.0"}]
    assert body["session_configuration"] == {"idle_timeout_seconds": 120}
    assert body["cpu"] == "1" and body["memory"] == "2Gi"
    assert body["container_configuration"]["image"] == IMAGE
    environment = body["environment_variables"]
    assert environment["BILLING_DOMAIN_AGENT"] == "billing-agent"
    assert environment["BILLING_DOMAIN_VERSION"] == environment["BOOKING_DOMAIN_VERSION"] == "3"


@pytest.mark.parametrize("role", ["billing", "booking"])
def test_specialist_definition_uses_its_own_image_without_role_switch_or_hub_bindings(role):
    body = deploy.definition(ENDPOINT, IMAGES[role], "model", role, {}).as_dict()
    assert body["enable_memory_snapshot"] and body["container_configuration"]["image"] == IMAGES[role]
    assert "AGENT_ROLE" not in body["environment_variables"]
    assert "DOMAIN_SPECIALTY" not in body["environment_variables"]
    assert "BILLING_DOMAIN_AGENT" not in body["environment_variables"]


def test_create_deploys_domains_before_hub_and_preserves_partial_manifest(tmp_path):
    client = NS(agents=MagicMock())
    client.agents.get.side_effect = ResourceNotFoundError("not found")
    definitions = {}

    def create(**kwargs):
        definitions[kwargs["agent_name"]] = kwargs["definition"]
        return NS(version="1")

    def version(**kwargs):
        return NS(as_dict=lambda: {"status": "active"}, definition=definitions[kwargs["agent_name"]])

    client.agents.create_version.side_effect = create
    client.agents.get_version.side_effect = version
    result = deploy.create(client, ENDPOINT, IMAGES, "model", "sample", tmp_path / "deployment.json")
    assert list(result["agents"]) == ["billing", "booking", "hub"]
    hub = definitions[result["agents"]["hub"]["name"]].as_dict()
    assert hub["environment_variables"]["BILLING_DOMAIN_AGENT"] == result["agents"]["billing"]["name"]
    assert hub["environment_variables"]["BILLING_DOMAIN_VERSION"] == "1"
    assert all(
        call.kwargs["metadata"]["voiceLiveInvocationState"] == "session_self_managed"
        for call in client.agents.create_version.call_args_list
    )
    assert not client.agents.update.called and not client.agents.delete.called


def test_failed_create_keeps_planned_names_and_accepted_versions(tmp_path):
    client = NS(agents=MagicMock())
    client.agents.get.side_effect = ResourceNotFoundError("not found")
    body = deploy.definition(ENDPOINT, IMAGES["billing"], "model", "billing", {})
    client.agents.get_version.return_value = NS(as_dict=lambda: {"status": "active"}, definition=body)
    client.agents.create_version.side_effect = [NS(version="1"), RuntimeError("controlled failure")]
    output = tmp_path / "partial.json"
    with pytest.raises(RuntimeError, match="controlled failure"):
        deploy.create(client, ENDPOINT, IMAGES, "model", "sample", output)
    record = json.loads(output.read_text())
    assert set(record["planned_names"]) == {"hub", "billing", "booking"}
    assert set(record["agents"]) == {"billing"}


@pytest.mark.parametrize(
    "image,prefix,idle",
    [("image:latest", "sample", 120), (IMAGE, "../bad", 120), (IMAGE, "sample", 119), (IMAGE, "sample", 3601)],
)
def test_invalid_deployment_inputs_do_not_write_cloud_resources(tmp_path, image, prefix, idle):
    client = NS(agents=MagicMock())
    with pytest.raises(ValueError):
        deploy.create(
            client, ENDPOINT, {**IMAGES, "hub": image}, "model", prefix, tmp_path / "deployment.json", idle=idle
        )
    client.agents.create_version.assert_not_called()


def test_existing_agent_is_not_overwritten(tmp_path):
    client = NS(agents=MagicMock())
    with pytest.raises(RuntimeError, match="existing agent"):
        deploy.create(client, ENDPOINT, IMAGES, "model", "sample", tmp_path / "deployment.json")
    client.agents.create_version.assert_not_called()


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://sample/api/projects/p",
        "https://user:password@sample/api/projects/p",
        "https://sample/",
        "https://sample/api/projects/p?query=1",
    ],
)
def test_project_endpoint_rejects_unsafe_or_wrong_shapes(endpoint):
    with pytest.raises(ValueError):
        project_endpoint(endpoint)


async def test_cleanup_refuses_mismatched_version_without_deleting():
    calls = []

    def handler(request):
        calls.append(request.method)
        return httpx.Response(200, json={"version_indicator": {"agent_version": "99"}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ExceptionGroup):
            await smoke.cleanup(client, deployment(), "billing", "owned", {})
    assert calls == ["GET"]


def test_voice_url_pins_role_and_disables_voice_live_pools_only():
    url = voice_test.connection_url("wss://sample.invalid/voice-live/realtime", deployment(), "hub", "version")
    query = parse_qs(urlsplit(url).query)
    assert query["agent-name"] == ["hub-agent"] and query["agent-version"] == ["3"]
    assert query["agent-project-name"] == ["test"]
    assert query["features"] == ["hosted_session_warmup:false,hosted_session_shared:false,warmup_llm:false"]
    assert "model" not in query


@pytest.mark.parametrize(
    "url",
    ["ws://sample/voice-live/realtime", "wss://sample/api/projects/p", "wss://sample/voice-live/realtime?token=secret"],
)
def test_voice_url_rejects_unsafe_endpoint(url):
    with pytest.raises(ValueError):
        voice_test.connection_url(url, deployment(), "hub", "version")


def test_late_t1_events_are_not_attributed_to_t2():
    trace = voice_test.Trace()
    trace.turns.append({"turn": 1, "audio_bytes": 0})
    trace.record({"type": "input_audio_buffer.committed", "item_id": "input-one"}, 1)
    trace.record({"type": "response.created", "response": {"id": "response-one"}}, 2)
    trace.turns.append({"turn": 2, "audio_bytes": 0})
    trace.record({"type": "conversation.item.input_audio_transcription.completed", "item_id": "input-one"}, 3)
    trace.record(
        {"type": "response.audio.delta", "response_id": "response-one", "delta": base64.b64encode(b"\0\0").decode()}, 4
    )
    trace.record({"type": "input_audio_buffer.committed", "item_id": "input-two"}, 5)
    trace.record({"type": "response.created", "response": {"id": "response-two"}}, 6)
    trace.record({"type": "response.done", "response": {"id": "response-one", "status": "cancelled"}}, 7)
    assert trace.turns[0]["audio_bytes"] == 2 and trace.turns[0]["status"] == "cancelled"
    assert trace.turns[1]["audio_bytes"] == 0 and trace.turns[1]["input_item_id"] == "input-two"
    assert trace.turns[1]["response_id"] == "response-two"


def test_voice_trace_rejects_unsolicited_and_failed_responses():
    trace = voice_test.Trace()
    with pytest.raises(RuntimeError, match="pending"):
        trace.record({"type": "response.created", "response": {"id": "unsolicited"}}, 1)
    trace.turns.append({"turn": 1, "audio_bytes": 0})
    trace.record({"type": "response.created", "response": {"id": "response-one"}}, 2)
    with pytest.raises(RuntimeError, match="failed"):
        trace.record({"type": "response.done", "response": {"id": "response-one", "status": "failed"}}, 3)


def test_model_error_reply_is_flagged_even_when_text_is_split():
    trace = voice_test.Trace()
    trace.turns.append({"turn": 1, "audio_bytes": 0, "response_id": "response-one"})
    for delta in ("Error call", "ing model: unavailable"):
        trace.record({"type": "response.audio_transcript.delta", "response_id": "response-one", "delta": delta}, 1)
    assert trace.turns[0]["model_error_reply"]
    assert "Error" not in json.dumps(trace.turns)  # No transcript content in result artifacts.


async def test_wait_surfaces_receiver_failure_without_hanging():
    trace = voice_test.Trace()
    trace.error = "controlled failure"
    with pytest.raises(RuntimeError, match="controlled failure"):
        await trace.wait(lambda: False)


def test_audio_validation_requires_24khz_mono_pcm(tmp_path):
    for rate in (16000, 24000):
        path = tmp_path / f"{rate}.wav"
        with wave.open(str(path), "wb") as writer:
            writer.setparams((1, 2, rate, 0, "NONE", "not compressed"))
            writer.writeframes(b"\0\0" * 100)
        if rate == 24000:
            assert voice_test.audio(path) == b"\0\0" * 100
        else:
            with pytest.raises(ValueError):
                voice_test.audio(path)


async def test_capture_close_failure_is_not_reported_as_success():
    runtime = BillingAgent(Config(ENDPOINT, "model"), warmup=False)
    runtime.model = NS(close=AsyncMock(side_effect=RuntimeError("close failed")))
    with pytest.raises(RuntimeError, match="close failed"):
        await runtime.before_snapshot()
    assert not runtime.captured


async def test_restore_failure_closes_partial_clients_and_keeps_retry_possible(monkeypatch):
    runtime = BillingAgent(Config(ENDPOINT, "model"), warmup=False)
    runtime.captured = True
    credential = NS(close=AsyncMock())
    project = NS(get_openai_client=MagicMock(side_effect=RuntimeError("controlled")), close=AsyncMock())
    monkeypatch.setattr(shared, "DefaultAzureCredential", lambda: credential)
    monkeypatch.setattr(shared, "AIProjectClient", lambda **_: project)
    with pytest.raises(RuntimeError, match="controlled"):
        await runtime.after_restore(NS(session_id="one", restore_id="restore-one", session_env_overrides={}))
    assert runtime.restore_id is None and runtime.session_id is None
    credential.close.assert_awaited_once()
    project.close.assert_awaited_once()


async def test_turn_timeout_becomes_error_without_committing_history(monkeypatch):
    runtime = BillingAgent(Config(ENDPOINT, "model"), warmup=False)
    monkeypatch.setattr(shared, "TURN_TIMEOUT_SECONDS", 0)
    runtime.model = NS(responses=NS(create=AsyncMock()))
    runtime.credential, runtime.project, runtime.http = object(), object(), object()
    frames = [frame async for frame in runtime.stream("Question")]
    assert json.loads(frames[-1][6:])["type"] == "error"
    assert runtime.history == []


def native_report():
    return {
        "results": [
            {
                "success": True,
                "session_id": "session-one",
                "started_at": 1794995233,
                "turns": [
                    {"turn": 1, "response_id": "response-one", "input_item_id": "input-one"},
                    {"turn": 2, "response_id": "response-two", "input_item_id": "input-two"},
                ],
            }
        ]
    }


def test_native_query_preserves_exact_expected_ids_and_audits_missing_rows():
    query = export_native_query.build_query(native_report())
    assert '"session-one", "response-one", "input-one", 1' in query
    assert '"session-one", "response-two", "input-two", 2' in query
    assert "kind=leftouter" in query and "metric_rows=count()" in query
    assert "input_item_id == metric_input_id" in query and "isnotnull(native_ttfa)" in query
    assert "expected_samples=count()" in query and "missing_samples=countif(isnull(metric_rows))" in query
    assert "client_ttfa" not in query


@pytest.mark.parametrize("bad", ["failed", "missing_id", "duplicate", "empty"])
def test_native_export_does_not_silently_drop_invalid_customers_or_turns(bad):
    report = native_report()
    if bad == "failed":
        report["results"][0]["success"] = False
    if bad == "missing_id":
        del report["results"][0]["turns"][0]["input_item_id"]
    if bad == "duplicate":
        report["results"][0]["turns"][1]["response_id"] = "response-one"
    if bad == "empty":
        report["results"] = []
    with pytest.raises(ValueError):
        export_native_query.build_query(report)
