# Test procedure

Commands below run from `samples/hub-domain-agent` with its `.venv` active.
Offline tests do not need Azure credentials. **Deployment, TTS, text smoke and
voice tests make real calls and incur charges**; run only in your authorized
project. No test creates infrastructure, grants roles, changes pools, or changes
shared service configuration.

## 1. Offline application and SDK checks

```bash
python -m pytest tests -q
python -m ruff check .
python -m ruff format --check .
```

These exercise the real routing, streaming, snapshot hooks, SDK lifecycle
routes, OpenAI 3.55 stream parser, version serialization, cancellation and scoped
cleanup. Model/identity/Domain network boundaries are substituted. They do not
prove Azure IAM, container pull access, platform snapshot capture, or Voice Live.

Optional local container health check (no Azure credentials or model calls):

```bash
docker build -f billing/Dockerfile -t billing-agent:local .
docker run --rm --name billing-agent-local \
  -p 127.0.0.1:8088:8088 \
  -e PROJECT_ENDPOINT=https://example.invalid/api/projects/example \
  -e MODEL_DEPLOYMENT=example-model \
  -e DOMAIN_MODEL_WARMUP=false billing-agent:local
```

In another terminal, `curl --fail http://127.0.0.1:8088/readiness` must return 200.
Use Ctrl+C in the container terminal to stop it. Do not expose the local SDK
server to an untrusted network; Azure platform authentication is not reproduced
by this localhost check.

Build Booking with `-f booking/Dockerfile` and its own tag; its entry point is
fixed to Booking. Build Hub with `-f hub/Dockerfile` and its own tag. A local Hub
also needs `BILLING_DOMAIN_AGENT`, `BILLING_DOMAIN_VERSION`, `BOOKING_DOMAIN_AGENT`
and `BOOKING_DOMAIN_VERSION`; placeholder references suffice for health-only
checks, but do not prove Domain connectivity. Do not pass `AGENT_ROLE`: it is
not a runtime switch in any image.

## 2. Capture and deployed two-turn smoke

Build/publish all three images and run `deploy.py` as described in README. Confirm all three
versions have `enable_memory_snapshot=true` and their respective image/model/protocol
and idle settings. Verify the identities' required permissions before business
testing; capture ACTIVE alone is insufficient.

```bash
python smoke.py --role billing --output results/billing-smoke.json
python smoke.py --role hub --output results/hub-billing-smoke.json
```

Each command creates **one new session**, sends the two distinct Billing text
questions, and deletes its owned parent/child IDs even on failure. Each result
path must be new; never overwrite/retry a failed sample to manufacture a pass.

Inspect result `turns[*].status`:

- `captured=true`, nonempty `restore_id`; the same restore/session ID across
  T1/T2 of the same customer.
- First completed turn has `history_messages=2`, then4; a new customer starts
  from empty history, not another customer's history.
- Hub `last_route=billing`; one Billing session reused, no Booking session.
- Answers explain the synthetic increase: 15 excess calls × 2 yuan = 30 yuan,
  99→129 yuan. This is **not** a real account or refund operation.
- `cleanup_complete=true`. On cleanup failure, retain IDs/results and perform
  narrowly scoped operator cleanup rather than deleting by prefix.

In Foundry container log streams, correlate `SNAPSHOT_BEFORE`, `SNAPSHOT_AFTER`,
`INVOKE`, `HUB_ROUTE`, `DOMAIN_READY` and `DOMAIN_RESPONSE`. Check distinct restore
IDs between independent customers, the same capture boot ID per version, and
Domain restore IDs/history0→2 across the two invocations. `status` reports history
*after* completion; invocation/response headers report it *before* that turn.
No customer text, token, or credentials are written to server application logs.

For snapshot failure, inspect platform/callback errors. Do not disable the flag
to hide the failure. For 401/403, verify the **Hosted caller identity**, not merely
the CLI user. Do not change global flags/RBAC/network settings as a test workaround.

## 3. Routing checks

Use fresh sessions and outputs for each case:

```bash
python smoke.py --role hub --turn1 "Hello!" --turn2 "What can you help me with?" \
  --output results/hub-self-smoke.json
python smoke.py --role hub \
  --turn1 "What is the status of the simulated reservation BK1001?" \
  --turn2 "What is the cancellation policy for that reservation?" \
  --output results/hub-booking-smoke.json
```

Expected:Hello→`self` with no Domain session; booking→`booking` with one reused
Booking session. Inspect actual route/status, not just plausible spoken text.
Cross-topic switches must re-evaluate the tools; follow-ups may use prior history.

## 4. Prepare identical English audio inputs

Supply your own24kHz, mono,16-bit PCM WAVs, or generate the sample's five inputs:

```bash
python generate_audio.py --speech-endpoint https://<speech-account>.cognitiveservices.azure.com --output audio
```

This optional step calls Azure TTS five times. Keep `audio/manifest.json` and
SHA256s. Do not regenerate audio between comparison arms. The scripts do not
upload test recordings to source control; `audio/` and `results/` are ignored.

## 5. One-customer Voice Live audio smoke

Confirm the direct Voice Live/Hosted integration and the preview API version
are enabled in your target. `VOICE_LIVE_ENDPOINT` is the resource's secure
WebSocket URL, not the Project URL or a managed Voice Agent connection URL.

```bash
python voice_test.py --role hub --turn1 audio/billing-1.wav --turn2 audio/billing-2.wav \
  --customers 1 --output results/voice-smoke
```

The driver selects the pinned Hosted Hub via `agent-name`, `agent-version` and
`agent-project-name`; sets English speech recognition/Jenny output; paces PCM;
and starts T2 immediately after T1's first nonempty audio. It keeps a concurrent
receiver running for cancellation/control events. There is no `response.done`
plus fixed-delay wait before T2 and no automatic greeting/setup business turn.
Expected T1 interruption/cancelled is not itself failure; T2 must complete.

Require audio and distinct response/input IDs for both turns, successful scoped
cleanup, and inspect any transport/model errors. A close-code1006 after output
must be retained as a lifecycle issue, not described as an error-free run.
The platform may merge interrupted T1 ASR with T2 input; Hub forwards the input
it actually receives unchanged. Do not assume the T2 invocation equals only
the second WAV transcript under immediate interruption.

## 6. Optional20-customer serial comparison

Run only after smoke passes and you explicitly want a billed benchmark:

```bash
# Hub answers directly: greeting, no Domain invocation.
python voice_test.py --role hub --turn1 audio/hello.wav --turn2 audio/hello.wav \
  --customers 20 --output results/hub-self-20

# Direct Billing: no routing Hub.
python voice_test.py --role billing --turn1 audio/billing-1.wav --turn2 audio/billing-2.wav \
  --customers 20 --output results/billing-direct-20

# Hub routes to Billing; both snapshot-enabled, Domain reused at T2.
python voice_test.py --role hub --turn1 audio/billing-1.wav --turn2 audio/billing-2.wav \
  --customers 20 --output results/hub-billing-20
```

Within each command, new customers run **strictly serially**, not concurrently.
Keep the same versions/model/audio/voice and warmup policy. The Hello arm answers
a different question:it is not a matched causal A/B control for Billing. Direct
Billing versus Hub→Billing does use the same WAV pair, but sequential cohorts
still do not control model destination/load; EU Data Zone can route to different
regions inside its zone.

Domain session acquisition lies inside Hub T1, while initial Hosted acquisition
in direct/Hub-only paths precedes audio. T2 reuses sessions but still pays for
the additional Hub routing LLM and Hosted HTTP hop. Never add/subtract unrelated
percentiles as an exact stage budget.

### Native TTFA P50/P90

`result.json` includes session/response/input-item IDs and client diagnostic
timings. **Client speech-stopped-event receipt→first audio receipt is not native
Voice Live TTFA** and must not be relabeled as such. Initial acquisition is not
included in either turn metric and should be reported separately.

For an authorized Voice Live service telemetry operator:

```bash
python export_native_query.py results/hub-billing-20/result.json \
  --output results/hub-billing-20/native-ttfa.kql
```

Run the exported query in the correct target's native telemetry database. Adapt
table/time-column names if that deployment's schema differs. The sample neither
grants telemetry access nor queries an internal cluster. Without native access,
report only the labeled client observations; do not claim native percentiles.

Before publishing native results, require **20 valid samples per turn**,
`valid_samples=expected_samples`, zero missing/duplicate/invalid rows, and exact
input-item matching. Native ingestion can lag; bounded re-querying of the **same
IDs** is valid, resending business requests or replacing slow/missing samples is
not. Preserve incomplete samples and disclose reduced sample counts if a bound
expires; a smaller-cohort percentile is not a complete20-customer result.

Report format:

| Path | Turn | Native samples | Native TTFA P50(ms) | P90(ms) |
| --- | --- | ---: | ---: | ---: |
| Voice Live→Hub | T1/T2 separately | actual | measured | measured |
| Voice Live→Billing | T1/T2 separately | actual | measured | measured |
| Voice Live→Hub→Billing | T1/T2 separately | actual | measured | measured |

Also report deployment/version/image, timestamp, audio hashes, effective pools,
snapshot proof, Domain model warmup, actual routing when available, errors,
cleanup outcome, and whether acquisition is outside/inside the reported metric.

## 7. Cleanup and readiness handoff

Both runners delete their exact owned sessions. Verify cleanup flags and retain
deployment manifests. Agents/images remain for reuse. After verifying ownership
and no other users, remove only the created sample agents via your organization's
normal lifecycle process; restore separately authorized pool changes first.

Validation layers must stay distinct:

| Check | What it establishes |
| --- | --- |
| Offline tests | Application behavior and SDK wire/serialization boundaries with external I/O replaced. |
| Local container readiness | Packaging/startup/health only. |
| Capture ACTIVE | Platform accepted and initialized that version's snapshot. |
| Real text/audio smoke | Target identity, model, route, protocol and restore functionality for those requests. |
| Authorized native metric audit | Exact measured TTFA for the retained requests, not a universal latency guarantee. |

This change provides the procedures and local tests; it does not assert a new
cloud deployment or benchmark has been run for these source-built images.
