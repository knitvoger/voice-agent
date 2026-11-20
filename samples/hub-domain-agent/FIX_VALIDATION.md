# Cleanup regression validation

This record covers the two cleanup regressions reported after commit `3c8b679`.
It follows invariant-first reproduction, negative controls, retry/terminal checks,
and a fresh final-diff audit. It is not a claim that the entire sample is defect-free.

## Contracts and owners

**H1 — restored Hub ownership.** Given a bound Hub session and previously created
deterministic Billing/Booking children, cleanup must check both exact child IDs
under the Hub identity even when the restored process has no in-memory bindings.
No cleanup operation creates sessions. A version mismatch or failed request must
remain observable and retain the affected ID for subsequent cleanup/review.
Owner: `HubAgent.cleanup`; the caller deletes the parent only after this succeeds.

Counterexample: restoring a Hub with an empty map caused cleanup to return without
issuing child GETs; the caller could delete the parent while both children remained.

**S1 — owned receiver termination.** If socket close fails or its shutdown deadline
expires, the receiver must be cancelled and drained before exchange exits. Shutdown
failure must not prevent the caller from attempting parent-session cleanup, nor
produce `success=true`. Normal shutdown still drains pending receiver events before
validating errors/close code. Owner: the exchange function that creates the receiver.

Counterexample: deadline cancellation during `socket.close()` entered a finalizer
that awaited the still-running receiver before cancelling it. The timeout had
already delivered its cancellation, so the shielded shutdown could wait forever.

## Supported paths and boundaries

The sample uses Python 3.13, Linux/amd64 Hosted images, the SDK's public snapshot
callbacks, two configured version-pinned Domains, and asyncio/websockets cooperative
cancellation. Tests exercise real callback/client orchestration with model, identity,
HTTP and WebSocket providers replaced at their I/O boundaries. They do not establish
deployed RBAC, platform restoration/storage semantics or native voice latency.

Legal failure/cancellation/retry paths are part of these contracts. Arbitrary
private-field tampering or coroutines that suppress cancellation indefinitely are
not promised by the underlying asyncio/WebSocket API. No new shared infrastructure,
feature flags, roles, quotas or cloud deployment are required by the local fixtures.

Other review findings remain tracked separately, including unresolved CREATE
outcomes and the customer deadline still enclosing socket teardown. In particular,
one 404 does not reconcile a server-side CREATE that may still be in progress;
these fixes must not be reported as resolving that separate finding.

## Mutation and release ledger

| State/resource | Write/commit point | Owner | Failure/cancellation/retry rule |
| --- | --- | --- | --- |
| Child ID map | Seed both deterministic IDs before child I/O; preserve existing bindings | Hub | Keep unresolved/failed IDs; retry checks the same IDs, never CREATE |
| Child sandbox | Version-guarded DELETE of an exact ID | Hub identity | Failed/version-mismatched DELETE cannot release ownership |
| Ready set | Remove each successfully cleaned binding | Hub | Failed binding remains; reconstructed IDs are not marked ready |
| Hub session/restore/history | Public restore binding; cleanup must not alter them | Runtime | No cross-session cleanup or history mutation |
| Receiver task | Created before event receive; release after completion/cancel+drain | Exchange | No owned receiver survives exchange exit; no late event dispatch |
| Shutdown task | Separately timed and shielded; caller awaits completion | Exchange | Close failure/timeout must terminate receiver, then propagate |
| Result/ownership report | Record actual close/error and cleanup outcomes | Client | Never claim success for failed shutdown; retain IDs for cleanup |
| Parent sandbox | Cleanup after exchange finishes, including failure | Client | Attempt once for the retained exact parent ID; stop failed cohort |

## Test matrix

The original regression cases were written before the production fix and first
run against `3c8b679`; the final matrix adds subsequent audit/cancellation cases.
Readiness/cancellation gates and explicit timer expiry establish event ordering;
wall-clock timeouts only guard against a hung test.

| Contract | Positive controls | Negative/failure controls | Follow-up assertions |
| --- | --- | --- | --- |
| H1 | Public restore with zero/one/two existing children | Wrong version, GET/DELETE failure; cancellation in GET, before DELETE commit and after DELETE commit, for both roles | Retry/idempotent cleanup; unchanged parent/history; no unrelated deletion or CREATE |
| S1 | Normal close and completed receiver | Close raises before/after receiver completion; close timeout; receiver wait timeout; caller cancellation followed by close completion | Receiver done; no late event handling; parent cleanup attempted; unsuccessful result; subsequent valid customer succeeds |

## Executed validation

- Before changing production code, eight focused cases ran on `3c8b679`: six
  failed on the missing child checks/unreleased receiver and two control cases
  passed. These were behavior failures, not test collection/startup failures.
- The final full offline suite passes **134 tests** on Linux with Python **3.13.12**.
- A fresh isolated **Python 3.13.0** environment installed directly from
  `requirements-dev.txt` also passes **134 tests**; no default interpreter or
  deployed configuration was replaced. Both runs use the sample's pinned SDKs.
- Ruff check, Ruff format check and `git diff --check` pass.
- All three unchanged Docker build contracts were rebuilt with the updated Hub
  source. Network-disabled containers (Python 3.13.15) returned readiness 200 and
  contained only their own business package plus `common/`. These prove packaging,
  not platform snapshots or production networking. Temporary containers were
  stopped/removed; local images and isolated test environments are retained.
- A fresh reviewer inspected the full diff and independently exercised six
  cancellation cases: GET, DELETE before commit and DELETE after commit for each
  Domain. Ownership/retry, unrelated-resource safety and absence of CREATE passed.
  No high-confidence defect newly introduced by these two fixes was found.

## Mechanical audit and remaining boundaries

Child ownership is seeded before the first await, bounded by the two configured
roles, and released only by the per-ID cleanup path. Existing bindings are not
overwritten. Public invoke locking serializes cleanup with turns; snapshot capture
of an unbound process remains empty and does not issue cleanup network calls.
No new background tasks/cache containers were introduced. Exchange still owns one
receiver and awaits its completion/cancellation before releasing that ownership;
normal close keeps final events observable before close-code/error validation.

The reviewer independently reproduced the **pre-existing unresolved-CREATE hole**:
remote acceptance -> local cancellation -> cleanup GET404 -> delayed remote commit
can leave a child after cleanup. That public failure remains open; it is not an
unsupported cancellation scenario or resolved by deterministic reconstruction.
The customer deadline still encloses socket teardown. The other review findings
remain tracked separately and are not closed by this change.

No cloud deployment, model/voice business traffic, image publication, RBAC, feature
flag, Cosmos/network or quota changes were performed. Actual platform restore and
authorization still require an authorized live smoke after publishing/deploying
new versions. Windows and untested Python patch versions were not validated.
The evidence establishes the listed two regression contracts on the executed
matrix, not universal cleanup correctness across unresolved provider outcomes.
