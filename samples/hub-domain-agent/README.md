---
ai-usage: ai-assisted
---

# Snapshot-enabled Hub and Domain Hosted Agents

Connect **Voice Live directly to a Hosted Hub agent**. The Hub answers general
questions itself, or selects a Billing/Booking Domain agent whose answer streams
back through the Hub. This is application-owned routing, **not** a managed Voice
Agent subagent/handoff configuration. It does not create an Agent of kind `voice`.

```text
Voice Live (speech recognition/synthesis)
  └─ Hosted Hub (memory snapshot enabled)
       ├─ answers greetings/general questions
       ├─ Hosted Billing Domain (memory snapshot enabled)
       └─ Hosted Booking Domain (memory snapshot enabled)
```

All three roles use one source-built image and HTTP Invocations `2.0.0`. Each
customer gets an exclusive Hub session. Domain sessions are created lazily,
version-pinned, and reused for that customer's later turns. Billing and Booking
are read-only simulations: no customer data, transactions, or external business
system. All instructions and generated test speech are English.

## Behavior worth inspecting

- The Hub uses the Responses API with two strict, **empty-argument** function
  tools. It forwards the **original invocation input**, not model-rewritten text.
- On the first tool-name event, it starts acquiring the selected Domain session
  while continuing to read the model stream. It **does not send customer input**
  until the entire stream completes and exactly one registered empty-argument
  call has been validated. Invalid, mixed, truncated, or failed outputs fail.
- Domain output streams directly; there is no final Hub summarization LLM call.
  T2 reuses the Domain session but **still runs the Hub routing LLM**.
- Cancellation drains speculative tasks, closes streams, and does not commit an
  incomplete turn. An indeterminate CREATE remains in the exact owned-ID map
  for cleanup. A speculative but unused session can incur allocation cost.
- Completed turns are stored in a bounded **process-local** history. This is
  not durable conversation storage or a guarantee of context retention after
  sandbox reclamation/restart. Production persistence needs a separate design.

See the [OpenAI function-calling streaming guide](https://developers.openai.com/api/docs/guides/function-calling#streaming)
for tool-name versus completed-call events. SDK behavior is pinned here; the
sample uses OpenAI **3.55.0**, with no private parser/schema-cache monkeypatch.

## Prerequisites and deployment contract

1. Python **3.13**, Docker, Azure CLI sign-in (`az login`) or another
   `DefaultAzureCredential` identity.
2. An existing Foundry project supporting Hosted Agents, **memory snapshots**,
   HTTP Invocations, and direct Voice Live/Hosted integration in the intended
   subscription/region. Snapshot access can require separate preview enrollment;
   `allow_preview=True` sends headers but does **not** grant that access.
3. An existing Responses/function-calling text deployment, such as
   `gpt-5.4-mini`. Supply its deployment name. For EU processing, provision an
   eligible EU Data Zone deployment through your normal workflow; the sample
   does not create a model or change routing. Data Zone is not a single-region pin.
4. An existing container registry and platform image-pull access to it. The
   image must support `linux/amd64`. No registry, infrastructure, RBAC, network,
   quota, Cosmos, or service feature flag is provisioned by this sample.
5. Your operator identity needs Agent/session creation, read, invocation and
   scoped deletion permissions. The **Hosted identities**, not just your CLI
   identity, need model inference access; the Hub identity also needs permission
   to create/read/delete and invoke the two Domain sessions in this project.
   Ask the project administrator to grant the appropriate project-scoped roles.
   `deployment.json` records the returned identities. Treat authorization as a
   rollout dependency; an ACTIVE snapshot is not a successful business smoke.
6. Voice tests need Voice Live access on the selected resource and the chosen
   speech/voice capabilities. Optional WAV generation also needs Azure Speech
   TTS access. Calls, allocated sandboxes, and optional warmup/TTS incur charges;
   snapshot-specific billing must be confirmed with the platform owner.

The serving SDK owns health and snapshot lifecycle endpoints. Before capture,
the application cancels/drains its optional warmup and closes HTTP/model/project/
credential clients; it refuses a reusable snapshot with customer history or
Domain bindings. After restore it rebuilds clients and records boot/restore/
session IDs. These callbacks are necessary in addition to the definition flag.

## Setup

From this directory:

```bash
python3.13 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
cp .env.example .env
az login
```

PowerShell: use `py -3.13 -m venv .venv`, `.venv\Scripts\Activate.ps1`, and
`Copy-Item .env.example .env`. Keep this environment separate from other samples.

Set `.env` to your project, model deployment, immutable image reference, and
Voice Live endpoint. Never put tokens, keys, or tenant-specific credentials in
source. The container receives only explicit application configuration, not
your local `.env`, CLI profile, or result files.

## Build and publish the image

Use your existing registry and a new tag:

```bash
az acr login --name <existing-registry>
docker build --platform linux/amd64 -t <registry>.azurecr.io/hub-domain-agent:<new-tag> .
docker push <registry>.azurecr.io/hub-domain-agent:<new-tag>
docker buildx imagetools inspect <registry>.azurecr.io/hub-domain-agent:<new-tag>
```

Set `AGENT_IMAGE` in `.env` to the immutable `image@sha256:...` reference. If the
tag points to an OCI index, select its **linux/amd64 runtime manifest**, not the
attestation manifest. The local build does not publish or deploy automatically.

## Create the agents

```bash
python deploy.py --output deployment.json
```

This creates uniquely named Billing, Booking, then Hub agents, waits for each
version's snapshot capture to become ACTIVE, reads back the definition, and
pins the returned Domain versions in the Hub environment. Old Agents/versions
are never updated or deleted. If creation fails partway, keep the deployment
record: it contains planned names and accepted versions. There are no automatic
business/deployment retries or silent snapshot-disable fallback.

The essential configuration in [`deploy.py`](deploy.py) is:

```python
definition = models.HostedAgentDefinition(
    cpu="1",
    memory="2Gi",
    enable_memory_snapshot=True,
    container_configuration=models.ContainerConfiguration(image=image),
    protocol_versions=[models.ProtocolVersionRecord(protocol="invocations", version="2.0.0")],
    session_configuration=models.SessionConfiguration(idle_timeout_seconds=120),
    environment_variables=environment,
)
project.agents.create_version(
    agent_name=name,
    definition=definition,
    metadata={
        "voiceLiveCompatible": "true",
        "voiceLiveInvocationState": "session_self_managed",
    },
)
```

Snapshot enablement belongs to the **Hosted definition**, not Voice Live session
settings or arbitrary metadata. This sample intentionally uses the supported
Projects SDK creation path; it does not invent an unverified `azure.yaml` property.
No per-customer snapshot registration is needed: new sessions of the enabled
version use the platform's captured initialization state.

Defaults: **1 vCPU / 2 GiB**, **120-second sandbox idle timeout**, Hub model warmup
off, Domain startup/after-restore best-effort model warmup on. Warmup never enters
customer history and is independent of readiness. Set `DOMAIN_MODEL_WARMUP=false`
*before creating versions* if you want to test without it; record that change.

## Validate and test

Follow **[TESTING.md](TESTING.md)** in order: offline tests, optional local health
check, authorized deployed text smoke, routing checks, then optional audio
smoke/serial TTFA comparison. The included scripts do not start a benchmark when
you deploy or import them. Do not equate local hook tests with real platform
capture/restore or IAM validation.

Files:

| File | Purpose |
| --- | --- |
| `agent.py`, `prompts.py` | Shared snapshot-safe Hub/Billing/Booking runtime and simulated facts. |
| `deploy.py` | Typed snapshot-enabled definitions, unique versions, and readback manifest. |
| `smoke.py` | Two text turns against a pinned role, lifecycle status, exact-ID cleanup. |
| `generate_audio.py` | Optional real Azure TTS generation of Hello/Billing/Booking WAVs. |
| `voice_test.py` | Paced WAV input, audio/ID recording, immediate T2, scoped cleanup. |
| `export_native_query.py` | Exact-ID KQL for an authorized native-telemetry operator; no automatic query. |
| `tests/` | Offline tests of real application/SDK boundaries with external services substituted. |

## Pools and cleanup

**Memory snapshots and standby pools are different features.** Standby keeps
allocated sandboxes ready; snapshots provide an initialized process image.
This sample enables snapshots but **does not configure platform standby**.
The optional voice driver requests Voice Live warm/shared/service-model pools
off using preview feature controls; confirm effective service flags with the
operator if collecting a baseline. If the service rejects preview controls,
stop and use an explicitly supported configuration; do not silently drop them.

If separately authorized, configure platform standby on each Hosted Agent's
endpoint (`agent_endpoint.standby_pool.size`), wait for readiness, verify actual
claims, and restore the old capacities. A 403 saying standby is not enabled is an
environment enrollment blocker, not permission to change RBAC/service flags or
fall back to another region. No standby benchmark or free-warm-pool claim is
implied here.

Both clients delete only their exact owned test session IDs (including the
Hub's deterministically named children); they keep the deployed Agents/versions.
Keep failed cleanup evidence and inspect those IDs explicitly. If a process is
terminated abruptly, use recorded parent IDs and the same deterministic child-ID
formula, not a prefix/list-wide delete. Idle reclaim is a fallback, not proof of
successful cleanup. Deleting a Hub alone need not delete its child sessions.

When finished, inspect names/versions in `deployment.json`, verify no other
workload uses them and any standby target is zero, then remove **only those
sample Agents** through your normal Foundry lifecycle procedure. This removes
their versions/context; there is no automatic Agent deletion in this sample.

Do not treat historical latency figures from a different image, SDK, question,
model route, or warmup policy as a measured result for this portable sample.
