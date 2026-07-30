# Provider Admission Controller

**Status:** Draft specification  
**Author:** Hermes Orchestrator  
**Date:** 2026-07-29  
**Related:** Provider-request stall recovery (exit-74 chain), AI-VM single-slot coordination

---

## 1. Problem

Hermes makes provider API calls from **six distinct contexts** that share zero admission state:

| Context | Mechanism | Coordination |
|---|---|---|
| Gateway (primary chat) | `asyncio.to_thread` → worker thread | None |
| CLI (interactive / `-q`) | synchronous main-thread agent | None |
| Delegated subagent | `DaemonThreadPoolExecutor` thread | None |
| Kanban worker | `subprocess.Popen` (independent OS process) | None |
| Cron job | `ThreadPoolExecutor` in gateway | None |
| Auxiliary (titles, compression, MOA) | in-thread AIAgent | None |

A `--parallel 1` llama.cpp endpoint (or any single-slot local inference server) can receive concurrent requests from **all six contexts simultaneously** — one wins, the rest block on the server's internal queue or burn the full provider timeout. `ProviderRequestStalledError` (exit 74) is reactive, not preventive.

**Goal:** A host-wide, cross-process admission controller for scarce endpoints that:
- Prevents dispatch to a saturated endpoint (backpressure)
- Prioritizes interactive sessions over background work
- Bounds queue wait time
- Is crash-safe (process death releases capacity automatically)
- Integrates with the existing provider-stall recovery chain

## 2. Design: Hybrid SQLite Queue + Kernel `flock`

### Core idea
- **SQLite** holds scheduling metadata: enqueue order, priority (lane), state, queue deadline, and observability counters.
- **`fcntl.flock`** on a per-endpoint lock file is the authoritative generation-slot token. The kernel auto-releases it when the owning process dies or closes its file descriptor.
- The **provider attempt deadline starts only after acquiring the lock** — queue wait time has its own independent `admission_timeout`.

### Why not alternatives

| Approach | Problem |
|---|---|
| Plain `flock` | No queue metadata, FIFO/priority semantics, cancellation records, or observability |
| POSIX named semaphore | `sem_wait()` decrement persists on crash (`sem_post()` never fires); no ownership |
| SQLite lease only | Lease split-brain if a valid generation outlives its lease; stale work waits for lease expiry |
| Unix-domain broker | Adds a supervised daemon and protocol; larger rollout |
| HAProxy proxy | Queue happens *inside* the provider HTTP request, consuming the attempt deadline |

## 3. Queue Schema

A single SQLite database at a well-known host-wide path.

```sql
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS requests (
    id             TEXT PRIMARY KEY,           -- UUID
    seq            INTEGER UNIQUE,             -- monotonic enqueue order
    lane           TEXT NOT NULL,               -- 'interactive' or 'background'
    source         TEXT NOT NULL,               -- 'gateway', 'cli', 'cron', 'kanban', 'delegation', 'auxiliary'
    state          TEXT NOT NULL DEFAULT 'queued',  -- queued|running|done|cancelled|expired|abandoned
    enqueued_ms    INTEGER NOT NULL,            -- wall clock ms at enqueue
    queue_deadline_ms INTEGER,                  -- admission_timeout deadline (wall ms)
    owner_pid      INTEGER,                     -- PID that holds the flock
    owner_start_ms INTEGER,                     -- boot-time monotonic clock when PID started
    boot_id        TEXT,                        -- /proc/sys/kernel/random/boot_id at grant
    started_ms     INTEGER,                     -- when lock was acquired (wall ms)
    finished_ms    INTEGER,                     -- when lock was released (wall ms)
    cancel_reason  TEXT,                        -- why cancelled, if applicable
    endpoint_hash  TEXT NOT NULL                -- sha256 of normalized base_url
);

CREATE INDEX IF NOT EXISTS idx_queue ON requests(endpoint_hash, state, seq);

CREATE TABLE IF NOT EXISTS scheduler_state (
    endpoint_hash               TEXT PRIMARY KEY,
    consecutive_interactive     INTEGER NOT NULL DEFAULT 0
);
```

### Endpoint identity

A `--parallel 1` endpoint is identified by the **SHA-256 hash of its normalized base URL** (port-stripped, scheme-lowered, trailing-slash-stripped). This is computed once at config load time.

Existing normalization: `agent/backend_identity.py::normalize_base_url()` and `hermes_cli/route_identity.py::normalize_route_base_url()`.

### Lock file path

```
~/.hermes/admitslots/<sha256[:16]>.lock
```

The lock file is created on first use (`open(path, 'w').close()` inside the lock acquisition) and persists across restarts. Each endpoint gets one lock file regardless of how many profiles reference it.

## 4. Acquire Sequence (Critical Path)

```
┌─────────────────────────────────────────────┐
│ 1. INSERT queued row                         │  ◄— always succeeds (no admission yet)
│    id=uuid, seq=next, lane=..., source=...   │
│    queue_deadline_ms=enqueued_ms+timeout     │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│ 2. Wait loop                                 │  ◄— admission_timeout / cancel_event
│    Poll every 250ms (jittered)               │
│    Expire stale cancelled/expired rows       │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│ 3. Short BEGIN IMMEDIATE transaction          │  ◄— ~1ms
│    a) Expire cancelled/expired queued rows    │
│    b) Check admission_timeout — expire self   │
│    c) Select next eligible request            │
│       WHERE state='queued' AND endpoint_hash=X│
│       ORDER BY lane_priority, seq             │
│    d) Only if it is THIS request:             │
│       Attempt LOCK_EX|LOCK_NB on lock file    │
└──────────────────┬──────────────────────────┘
                   │
          ┌────────┴────────┐
          ▼                 ▼
    Lock busy?         Lock free?
    Rollback tx        ┌───────────────────────┐
    Retry loop          │ 4. Mark running        │
                        │    a) Atomically mark   │
                        │       stale 'running'   │
                        │       rows 'abandoned'  │
                        │    b) Mark this row     │
                        │       state='running'   │
                        │    c) Commit tx         │
                        │    d) KEEP flock open   │
                        └─────────┬─────────────┘
                                  │
                                  ▼
                   ┌─────────────────────────────┐
                   │ 5. Start provider attempt     │  ◄— deadline clock starts NOW
                   │    deadline timer             │
                   │    Fire HTTP request          │
                   └──────────────────────────────┘
```

### Key safety invariants

| Scenario | Behaviour |
|---|---|
| Crash before lock acquired | No capacity consumed; SQLite row stays queued (expired on next sweep) |
| Crash after `LOCK_EX` but before SQLite commit | Kernel releases `flock`. Row still `queued` — next waiter ignores it as lock-free |
| Crash after SQLite commit (state=`running`) | Kernel releases `flock`. Next waiter atomically marks stale `running` → `abandoned` and claims the lock |
| Long generation, process alive | Lock held; no other process can acquire. `flock` does not time out |
| Process stuck after cancellation | Lock held until process dies; `cancel()` marks row `cancelled` but lock can't be stolen — provider-stall recovery (exit 74) handles this |

**The `flock` is the sole capacity gating mechanism.** SQLite state is advisory metadata. A row marked `running` with a released `flock` means the previous holder crashed — the next waiter reclaims it.

## 5. Fairness Policy

Two lanes:

| Lane | Source examples | Priority |
|---|---|---|
| `interactive` | gateway, CLI (interactive), `/steer` | Higher |
| `background` | cron, kanban, delegation, auxiliary, CLI (`-q`) | Lower |

**Rule:** FIFO within each lane by monotonic `seq`. To prevent starvation of background by sustained interactive traffic:

1. Track `consecutive_interactive` in `scheduler_state`.
2. If `consecutive_interactive >= 3` AND at least one background request is queued → **force the oldest background request**.
3. Reset `consecutive_interactive` to 0 on any background grant.
4. Otherwise, grant the front of the interactive queue.

**Additionally:** Any request whose queue wait exceeds `max_queue_age` (configurable, default 120s) is promoted to the front of the interactive lane regardless of original lane. This bounds worst-case background latency.

## 6. Two-Clock Model

| Clock | Starts | Purpose | On expiry |
|---|---|---|---|
| `admission_timeout` | Enqueue | Cap queue wait time | Mark row `expired`, notify caller |
| `provider_attempt_timeout` | Lock grant | Cap generation time | `ProviderRequestStalledError` (exit 74) |

**These are independent.** Queue wait never consumes generation time. The total end-to-end timeline:

```
enqueue ─────┬───── grant ───────────────┬───── complete
             │  (admission_timeout)       │  (provider_attempt_timeout)
             └── queue wait ──────────────┘  └── generation ──
```

Optional outer `workflow_deadline` may cap the sum, but it is distinct from retry-attempt accounting.

## 7. Integration Seam

### Primary: `agent/transports/base.py` (abstract `Transport`)

Add two lifecycle hooks to the `Transport` base class:

```python
class Transport:
    def admit(self, endpoint_hash: str, lane: str, source: str,
              admission_timeout: float, cancel_event: threading.Event | None) -> AdmissionToken:
        """Block until capacity is granted or admission_timeout expires.
        Returns a token that MUST be released via release()."""
        ...

    def release(self, token: AdmissionToken) -> None:
        """Release the slot. Idempotent."""
        ...
```

Every concrete transport (`ChatCompletionsTransport`, `BedrockTransport`, etc.) inherits this. The pattern is zero-cost when the endpoint is not a local/admission-controlled one: a no-op implementation returns immediately.

### Secondary: `agent/provider_request_watchdog.py`

The `ProviderRequestMonitor` already holds `provider` + `model` identity. Extend it to:
- Accept `admission_token` at construction
- Call `transport.release()` in `complete()`, `fail()`, and `cancel()`
- This guarantees release in all terminal paths

### Tertiary: Kanban worker spawn (`hermes_cli/kanban_db.py`)

Add a pre-flight admission check before `subprocess.Popen` for any worker targeting a local endpoint. If admission is denied (queue full, timeout), mark the task as `blocked` with `--reason="admission:timeout"` rather than burning a subprocess and the full provider timeout.

## 8. Configuration

Under `providers.custom[].admission` in `config.yaml`:

```yaml
providers:
  custom:
    - name: ai-vm
      base_url: http://192.168.23.42:8080/v1
      models: [chat]
      admission:
        enabled: true                  # default false (opt-in per endpoint)
        max_slots: 1                   # flock files created for this many slots
        admission_timeout: 30.0        # max seconds in queue before expiry
        interactive_burst: 3           # max consecutive interactive before forced background
        max_queue_age: 120             # seconds after which any queued request is promoted
```

When `admission.enabled` is false (default), the admit/release hooks are no-ops — zero overhead for cloud endpoints.

## 9. CLI: `hermes admitslots`

```text
hermes admitslots status <endpoint>    # Show slot state, queue depth, active holder
hermes admitslots queue <endpoint>     # List queued requests (id, lane, source, age)
hermes admitslots flush <endpoint>     # Cancel all queued (maintenance)
hermes admitslots drain <endpoint>     # Block new enqueues, let current finish
hermes admitslots reset <endpoint>     # Reap stale abandoned rows, re-create lock file
```

## 10. Observability

### Lifecycle events (structured, keyed by admission ID)

- `admission.enqueued` — id, lane, source, endpoint_hash, queue_deadline
- `admission.granted` — id, queue_wait_seconds
- `admission.completed` — id, service_time_seconds
- `admission.cancelled` — id, cancel_reason, queue_wait_seconds
- `admission.expired` — id, queue_wait_seconds, reason (timeout)
- `admission.abandoned` — id (stale running row reaped)
- `admission.degraded` — id, reason (flock-only mode when SQLite unavailable)

### Gauges (exposed for Prometheus / `/metrics`)

- `hermes_admission_queue_depth{lane, endpoint_hash}`
- `hermes_admission_active_slots{endpoint_hash}`
- `hermes_admission_oldest_queued_age_seconds{endpoint_hash}`
- `hermes_admission_consecutive_interactive{endpoint_hash}`

### Histograms

- `hermes_admission_queue_wait_seconds{lane, endpoint_hash}`
- `hermes_admission_service_time_seconds{endpoint_hash}`

### Cross-check against llama.cpp

- `llamacpp:requests_processing` / `llamacpp:requests_deferred` should remain near 1 and 0 respectively when the external gate is working
- Alert if llama.cpp reports deferred > 0 while external queue depth is 0 → indicates a bypass or release-before-stream-completion bug

## 11. Rollout Plan

### Phase 1 — Instrument (safe, observable)

1. Add the admission DB schema, lock file management, and `AdmissionController` class.
2. Instrument the provider-call entry point with observe-only "would-admit" logging.
3. Expose CLI `hermes admitslots status`.
4. Deploy to production. Verify:
   - Queue depth tracks actual concurrency.
   - No false positives (no blocking yet).
   - No throughput regression.
5. **Deliverable:** `hermes admitslots status` shows live concurrency, no behaviour change.

### Phase 2 — Gate background callers

1. Enable admission enforcement for `lane=background` sources (cron, kanban, delegation, auxiliary).
2. Interactive sessions remain un-gated.
3. Verify queue depth drops. Monitor `admission.expired` events for background tasks.
4. **Deliverable:** Background workers no longer contend with interactive sessions for the single slot.

### Phase 3 — Full enforcement

1. Enable admission for `lane=interactive` (gateway, CLI).
2. Monitor queue depth, `admission_timeout` expiry rates, and end-to-end latency.
3. Tune `interactive_burst` and `max_queue_age` based on observed ratios.
4. **Deliverable:** All provider calls to admitted endpoints go through the hybrid queue.

### Phase 4 — Hardening

1. Chaos test: `SIGKILL` active holder, kill queued waiter, cancel during selection and generation, SQLite busy/restart/corruption, system reboot.
2. Add `hermes admitslots drain` for graceful endpoint maintenance.
3. Add llama.cpp deferred-requests alert.
4. **Deliverable:** Crash-safe, verified under fault injection.

## 12. Testing Strategy

### Unit tests (pytest, hermetic)

| Test | What it verifies |
|---|---|
| `test_single_acquire_release` | Acquire succeeds, release frees for next acquirer |
| `test_concurrent_acquire` | Two processes/threads: one gets lock, other blocks |
| `test_admission_timeout` | Queue wait exceeds timeout → row expired |
| `test_cancellation_while_queued` | Cancel() atomically removes from queue |
| `test_cancel_grant_race` | Cancel arrives just after grant → release without HTTP |
| `test_fairness_burst` | 4 interactive + 1 background → background forced after 3 interactive |
| `test_promotion_by_age` | Background request exceeds max_queue_age → promoted to front |
| `test_crash_recovery` | Simulate crash after `LOCK_EX` / after SQLite commit — verify next waiter reclaims |
| `test_degraded_flock_only` | SQLite unavailable → explicit degrade to plain flock |
| `test_schema_migration` | Schema version bump is backward-compatible |
| `test_cli_status` | `hermes admitslots status` returns valid JSON |
| `test_noop_when_disabled` | `admission.enabled: false` → zero overhead |

### Integration tests

| Test | What it verifies |
|---|---|
| `test_gateway_admission` | Gateway conversation thread acquires and releases |
| `test_kanban_worker_admission` | Kanban subprocess blocks on admission before spawning |
| `test_cron_admission` | Cron job enqueues, waits, acquires |
| `test_mixed_contexts` | Gateway + cron + kanban all target same endpoint concurrently |

### Chaos tests (manual / test harness)

| Scenario | Expected behaviour |
|---|---|
| `kill -9` active process | `flock` auto-released; next waiter reclaims within one poll cycle |
| Kill queued process | Row stays queued; expires on admission_timeout |
| SQLite database deleted mid-operation | Admission degrades to `flock`-only; re-create DB on next opportunity |
| System reboot | Boot ID mismatch cleans stale rows on next admission |

## 13. Files to Create / Modify

### New files

| File | Purpose |
|---|---|
| `agent/admission_controller.py` | Core `AdmissionController` class: SQLite queue, flock acquire/release, fairness policy |
| `tests/agent/test_admission_controller.py` | Unit and integration tests |
| `hermes_cli/admitslots_cli.py` | `hermes admitslots` subcommand |

### Modified files

| File | Change |
|---|---|
| `agent/transports/base.py` | Add `admit()` / `release()` lifecycle hooks to `Transport` (no-op by default) |
| `agent/provider_request_watchdog.py` | Accept `admission_token`, call release in terminal paths |
| `agent/transports/chat_completions.py` | Call `admit()` before `client.chat.completions.create()`, `release()` in `finally` |
| `hermes_cli/kanban_db.py` | Pre-flight admission check before `subprocess.Popen` on local endpoints |
| `hermes_cli/config.py` | Add admission section to config schema/DEFAULT_CONFIG |
| `cli.py` | Register `hermes admitslots` subcommand |
| `gateway/run.py` | Wire admission for gateway agent runs |
| `cron/scheduler.py` | Wire admission for cron job runs |
| `tools/delegate_tool.py` | Wire admission for delegated subagents |

## Appendix A: Lock File Lifecycle

```python
_LOCK_DIR = get_hermes_home() / "admitslots"

def _lock_path(endpoint_hash: str) -> Path:
    return _LOCK_DIR / f"{endpoint_hash[:16]}.lock"

# On first use:
_LOCK_DIR.mkdir(parents=True, exist_ok=True)
lock_path = _lock_path(endpoint_hash)
lock_path.touch(mode=0o644, exist_ok=True)

# Acquire:
fd = os.open(lock_path, os.O_RDWR | os.O_CLOEXEC)
try:
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    os.close(fd)
    # ... retry loop with jittered sleep ...
```

### Why `O_CLOEXEC`
Prevents the lock file descriptor from leaking to subprocesses spawned by the agent (e.g., Kanban worker subprocess). Without it, the subprocess inherits the lock and the parent releasing would not fully free the lock until both parent and child close.

### Cleanup
Lock files persist on disk — they are empty marker files (~0 bytes). `hermes admitslots reset` can recreate them if corrupted. Stale `running` rows in SQLite are cleaned on next acquisition (atomically marked `abandoned`).

## Appendix B: Lane Classification

```python
def classify_lane(source: str, is_interactive: bool) -> str:
    """Classify a request into 'interactive' or 'background' lane."""
    if is_interactive:
        return "interactive"
    # These are always background:
    return "background"
```

The `is_interactive` flag is propagated from the session context. Gateway chat sessions are interactive; CLI `-q` queries, cron jobs, kanban workers, delegated subagents, and auxiliary tasks are background.

## Appendix C: Cancellation Contract

```
While queued:        atomic UPDATE state='cancelled' WHERE id=? AND state='queued'
Grant/cancel race:   After grant, before HTTP: check cancel_event. If set → release + done.
During generation:   Propagate to HTTP stream (httpx client.close() / cancel()). 
                     Lock released in finally.
Process death:       Kernel releases flock. Row → abandoned on next acquisition.
```

## Appendix D: SQLite Contention

The `BEGIN IMMEDIATE` transaction in step 3 is the only write transaction on the hot path. It runs:
- At most once per poll interval per waiting process
- Duration: ~1ms (a few UPDATE/SELECT statements, no generation work)

With N concurrent waiters each polling every 250ms, the expected contention rate on a host with ≤10 concurrent Hermes processes is:
- Write attempts: ~40/second (10 processes × 4 polls/sec)
- SQLite WAL handles this with zero contention in practice

If contention becomes measurable, increase poll interval to 500ms or switch to an in-memory SQLite with WAL fallback.
