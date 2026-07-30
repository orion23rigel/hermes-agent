# Provider Admission Controller (Corrected Specification)

**Status:** Implementation-ready specification
**Author:** Hermes Orchestrator (reviewed & corrected by Claude subagent)
**Date:** 2026-07-29
**Related:** Provider-request stall recovery (exit-74 chain), AI-VM single-slot coordination

---

## Table of Contents

1. Problem
2. Design: Hybrid SQLite + Kernel `flock`
3. Queue Schema
4. Acquire Sequence (Critical Path)
5. Fairness Policy
6. Two-Clock Model
7. Integration Seam
8. Configuration
9. CLI: `hermes admitslots`
10. Observability
11. Rollout Plan
12. Testing Strategy
13. Files to Create / Modify
14. Windows Compatibility (Design Notes)

---

## 1. Problem

Hermes makes provider API calls from **six distinct contexts** that share zero admission state:

| Context | Mechanism | Coordination |
|---------|-----------|--------------|
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

---

## 2. Design: Hybrid SQLite Queue + Kernel `flock`

### Core idea

- **SQLite** holds scheduling metadata: enqueue order, priority (lane), state, queue deadline, and observability counters.
- **`fcntl.flock`** on a per-endpoint lock file is the authoritative generation-slot token. The kernel auto-releases it when the owning process dies or closes its file descriptor.
- The **provider attempt deadline starts only after acquiring the lock** — queue wait time has its own independent `admission_timeout`.

### Why not alternatives

| Approach | Problem |
|----------|---------|
| Plain `flock` | No queue metadata, FIFO/priority semantics, cancellation records, or observability |
| POSIX named semaphore | `sem_wait()` decrement persists on crash (`sem_post()` never fires); no ownership |
| SQLite lease only | Lease split-brain if a valid generation outlives its lease; stale work waits for lease expiry |
| Unix-domain broker | Adds a supervised daemon and protocol; larger rollout. Best option if a gateway-hosted thread is acceptable (see §2.1) |
| HAProxy proxy | Queue happens *inside* the provider HTTP request, consuming the attempt deadline |
| Filesystem atomic renames | Rename(2) race conditions under concurrent directory listing; NOACID; priority hard to implement; no built-in query |

### 2.1 Unix-Domain Broker (reserved for future work)

A single-process broker (running as a thread in the gateway or a lightweight supervisor process) would provide:
- Zero-poll scheduling (condition variable wakeup)
- Full control over priority with in-memory data structures
- Atomic cancel/grant (no race between SQLite transaction and cancel UPDATE)

The hybrid design was chosen for Phase 1/2 to avoid the broker's deployment complexity. A migration path to a broker-based design should be evaluated if polling overhead becomes measurable or if `max_slots > 1` is ever required.

---

## 3. Queue Schema

A single SQLite database at a well-known host-wide path.

```sql
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 0;                -- fail-fast; retry loop in application

CREATE TABLE IF NOT EXISTS requests (
    id             TEXT PRIMARY KEY,    -- UUID
    rowid          INTEGER NOT NULL,    -- auto-increment ROWID via INTEGER PRIMARY KEY
    lane           TEXT NOT NULL,       -- 'interactive' or 'background'
    source         TEXT NOT NULL,       -- 'gateway', 'cli', 'cron', 'kanban', 'delegation', 'auxiliary'
    state          TEXT NOT NULL DEFAULT 'queued',  -- queued|running|done|cancelled|expired|abandoned
    enqueued_ms    INTEGER NOT NULL,    -- wall clock ms at enqueue
    queue_deadline_ms INTEGER,          -- admission_timeout deadline (wall ms)
    owner_pid      INTEGER,             -- PID that holds the flock
    owner_start_ms INTEGER,             -- time.monotonic_ns() when process started (NOT wall clock)
    boot_id        TEXT,                -- /proc/sys/kernel/random/boot_id at grant
    started_ms     INTEGER,             -- when lock was acquired (wall ms)
    finished_ms    INTEGER,             -- when lock was released (wall ms)
    cancel_reason  TEXT,                -- why cancelled, if applicable
    cancel_requested INTEGER DEFAULT 0, -- 1 when a cross-process cancel arrived (see §4.4)
    endpoint_hash  TEXT NOT NULL        -- sha256 of normalized base_url (full 64-char hex)
);

CREATE INDEX IF NOT EXISTS idx_queue ON requests(endpoint_hash, state, rowid);
CREATE INDEX IF NOT EXISTS idx_ep_hash ON requests(endpoint_hash);
CREATE INDEX IF NOT EXISTS idx_cleanup ON requests(state, finished_ms);
```

### Changes from v1

| Change | Rationale |
|--------|-----------|
| Removed `seq INTEGER UNIQUE` | Replaced by `rowid` (auto-increment INTEGER PRIMARY KEY). No race on generation. |
| Added `cancel_requested INTEGER` | Solves the cancel-vs-grant race (§4.4). |
| `owner_start_ms` uses `time.monotonic_ns()` | Consistent with Linux monotonic clock; paired with `boot_id` for disambiguation. |
| `endpoint_hash` is full 64-char SHA-256 | Eliminates 64-bit collision domain from `[:16]` truncation. |
| `busy_timeout = 0` | Fail fast on lock contention; retry in application loop. |
| Added index on `(state, finished_ms)` | Enables efficient row-purge queries. |

### Endpoint identity

A `--parallel 1` endpoint is identified by the **SHA-256 hash of its normalized base URL** (scheme-lowered, hostname-lowered, port retained, trailing-slash-stripped via `urlunsplit`). **One single normalization function** is used: `hermes_cli/route_identity.py::normalize_route_base_url()`. The hash is computed once at config load time and stored in the config entry's `BackendIdentity` — the admission controller never re-hashes.

> **Important:** `agent/backend_identity.py::normalize_base_url()` does not exist. Only `hermes_cli/route_identity.py::normalize_route_base_url()` is the canonical normalization. The admission controller **must** use the pre-computed hash from the config entry, not re-normalize from each call site.

### Lock file path

```
~/.hermes/admitslots/<sha256_full>.lock
```

The lock file is created on first use with `Path.touch(mode=0o644, exist_ok=True)`. **Do NOT use `open(path, 'w').close()`** — it truncates an existing file and can race with concurrent `touch` from another process.

### Lock file lifecycle

```python
_LOCK_DIR = get_hermes_home() / "admitslots"

def _lock_path(endpoint_hash: str) -> Path:
    return _LOCK_DIR / f"{endpoint_hash}.lock"

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

**Why `O_CLOEXEC`:** Prevents the lock file descriptor from leaking to subprocesses spawned by the agent (e.g., Kanban worker subprocesses). Without it, the subprocess inherits the lock and the parent releasing would not fully free the lock until both parent and child close.

**Cleanup:** Lock files persist on disk — they are empty marker files (~0 bytes). `hermes admitslots reset` recreates them if corrupted. **Stale lock files for removed endpoints must be cleaned manually** — add `hermes admitslots prune` in Phase 4, or auto-clean on config reload.

---

## 4. Acquire Sequence (Critical Path)

### 4.1 Full Sequence

```
┌──────────────────────────────────────────────┐
│ 1. INSERT queued row                          │  ◄— always succeeds (no admission yet)
│    id=uuid, rowid=AUTO, lane=..., source=...  │
│    queue_deadline_ms=enqueued_ms+timeout      │
│    cancel_requested=0                         │
└──────────────────┬───────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────┐
│ 2. Wait loop                                  │  ◄— admission_timeout / cancel_event
│    Poll every 250ms (jittered ±50ms)          │
│    Local cancel_event check (threading.Event) │
│    If set, UPDATE own row to 'cancelled'      │
│    (no transaction — best-effort metadata)    │
│    If own row's cancel_requested==1 → break   │
│    If admission_timeout exceeded → UPDATE     │
│    own row to 'expired', return               │
└──────────────────┬───────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────┐
│ 3. Write transaction (BEGIN IMMEDIATE)         │  ◄— ~1ms, busy_timeout=0
│    a) Reap stale 'running' rows:               │
│       - boot_id mismatch → mark 'abandoned'    │
│       - pid not alive via procfs → 'abandoned' │
│    b) Expire 'cancelled'/'expired' queued rows │
│       (including self — may short-circuit)     │
│    c) Select next eligible request:            │
│       Dynamic priority computation:            │
│       - Age-promoted background (queue wait    │
│         ≥ max_queue_age) treated as interactive│
│       - ORDER BY effective_lane, rowid         │
│    d) If selected row is THIS request:         │
│       Attempt LOCK_EX|LOCK_NB on lock file     │
│    e) If selected row is NOT this request,     │
│       AND its flock is free (LOCK_NB test):    │
│       → Mark stale row 'abandoned',            │
│         re-query (goto c)                      │
│    f) If selected row has cancel_requested=1:  │
│       → Mark it 'cancelled', re-query (goto c) │
└──────────────────┬───────────────────────────┘
                   │
          ┌────────┴────────┐
          ▼                 ▼
    Lock busy?         Lock free?
    Rollback tx        ┌─────────────────────────┐
    Sleep 250ms         │ 4. Mark running          │
    Goto 2              │    a) Re-check cancel_re-│
                        │       quested flag AFTER │
                        │       lock acquire       │
                        │    b) If flag set:        │
                        │       release flock       │
                        │       mark 'cancelled'    │
                        │       commit, return      │
                        │    c) Mark this row       │
                        │       state='running'     │
                        │       owner_pid=getpid(), │
                        │       owner_start_ms=now, │
                        │       boot_id=read_boot() │
                        │       started_ms=now_wall │
                        │    d) Commit tx           │
                        │    e) KEEP flock open     │
                        └─────────┬───────────────┘
                                  │
                                  ▼
                   ┌──────────────────────────────┐
                   │ 5. Start provider attempt      │  ◄— deadline clock starts NOW
                   │    ProviderRequestMonitor.     │
                   │      begin_attempt()           │
                   │    Fire HTTP request           │
                   └───────────────────────────────┘
```

### 4.2 Key safety invariants

| Scenario | Behaviour |
|----------|-----------|
| Crash before lock acquired | No capacity consumed; SQLite row stays queued (expired on next sweep or by admission_timeout) |
| Crash after `LOCK_EX` but before SQLite commit | Kernel releases `flock`. Row still `queued`. Next waiter detects free lock (step 3e), marks row `abandoned`, re-queries, claims the lock for itself |
| Crash after SQLite commit (state=`running`) | Kernel releases `flock`. Next waiter detects stale `running` by `boot_id` mismatch or PID check (step 3a), marks `abandoned`, and claims the lock |
| Long generation, process alive | Lock held; no other process can acquire. `flock` does not time out |
| Process stuck after cancellation | Lock held until process dies. `cancel_requested=1` flag set. If the stuck process is still generating, provider-stall recovery (exit 74) handles it |

**The `flock` is the sole capacity gating mechanism.** SQLite state is advisory metadata. A row marked `running` with a released `flock` means the previous holder crashed — the next waiter reclaims it.

### 4.3 SQLite Contention Management

The `BEGIN IMMEDIATE` transaction in step 3 is the only write transaction on the hot path:
- Duration: ~1ms (a few UPDATE/SELECT statements, no generation work)
- `PRAGMA busy_timeout = 0` ensures `SQLITE_BUSY` is returned immediately if another writer is active
- The retry loop at the application level handles backoff

With N concurrent waiters each polling every 250ms, the expected contention rate:
- Write attempts: ~40/second (10 processes × 4 polls/sec)
- SQLite WAL handles this with zero measurable contention in practice

If contention becomes measurable (monitor `admission.enqueued` / `admission.granted` ratio), increase poll interval to 500ms or implement the Unix-domain broker alternative.

### 4.4 Cancel-vs-Grant Race Resolution

The `cancel_requested` column eliminates the race between a cross-process cancel and the grant transaction:

1. **Cross-process cancel** (`hermes admitslots flush` or API cancel):
   ```sql
   UPDATE requests SET cancel_reason=?, cancel_requested=1 WHERE id=? AND state IN ('queued', 'running');
   ```
   This UPDATE always targets `cancel_requested=1` regardless of `state`. If the row is `queued`, the usual `WHERE state='queued'` check makes the cancel a no-op on the old contract. By targeting **both** `queued` and `running`, we ensure the flag is set even if the grant transaction is concurrent.

2. **Inside the grant transaction** (step 4b), **after** acquiring the flock:
   ```sql
   SELECT cancel_requested, cancel_reason FROM requests WHERE id=?;
   ```
   If `cancel_requested == 1`, the grantor immediately releases the flock and marks the row `cancelled`, then returns to the caller with a cancellation error. The flock was held for only microseconds.

3. **In-process cancel** (threading.Event):
   The wait loop (step 2) checks `cancel_event.is_set()` before each poll iteration. If set, the loop breaks without entering a transaction, and the caller is notified immediately.

---

## 5. Fairness Policy

### Lane Classification

```python
def classify_lane(source: str, is_interactive: bool) -> str:
    """Classify a request into 'interactive' or 'background' lane.
    
    Classification rules by context:
    - Gateway human chat sessions:        interactive
    - Gateway API server (automated):     background  (must be explicitly set)
    - CLI (interactive, no -q):           interactive
    - CLI (-q / quiet mode):              background
    - Cron jobs:                          background
    - Kanban workers:                     background
    - Delegated subagents:                background
    - Auxiliary tasks (titles, etc.):     INHERIT from triggering session
    """
    if is_interactive:
        return "interactive"
    return "background"
```

**Important:** The `source` field is logged for observability but the `is_interactive` flag is the sole determinant of lane assignment. The table below maps `source` → expected `is_interactive` for the **default** case; individual sessions can override.

| Source | Default is_interactive | Rationale |
|--------|----------------------|-----------|
| `gateway` | `True` for human chat; `False` for API server | API server sessions are automated |
| `cli` | `True` unless `-q` flag | Quiet mode = script/headless |
| `cron` | `False` | Always scheduled, never interactive |
| `kanban` | `False` | Worker subprocess |
| `delegation` | `False` | Subagent in background |
| `auxiliary` | Inherited from caller | e.g., interactive session's title generation is interactive-flagged |

### Two lanes

| Lane | Source examples | Priority |
|------|----------------|----------|
| `interactive` | gateway (human), CLI (no `-q`), `/steer` | Higher |
| `background` | cron, kanban, delegation, auxiliary, CLI (`-q`), API server | Lower |

### Fairness Rules

FIFO within each lane by monotonic `rowid`. To prevent starvation of background by sustained interactive traffic:

1. Track `consecutive_interactive_grants` in the `scheduler_state` table.
2. If `consecutive_interactive_grants >= interactive_burst` (default 3) AND at least one background request is queued → **force the oldest background request** (treat it as interactive for ordering).
3. Reset `consecutive_interactive_grants` to 0 on any background grant.
4. Otherwise, grant the front of the interactive queue.

**Additionally:** Any request whose queue wait exceeds `max_queue_age` (configurable, default 120s) is promoted to the front of the interactive lane regardless of original lane. This bounds worst-case background latency.

### Dynamic Priority Computation (for step 3c)

The SELECT query must compute priority dynamically at query time:

```sql
WITH dynamic_lane AS (
  SELECT *,
    CASE WHEN ? - enqueued_ms >= ? THEN 0        -- age-promoted background
         WHEN lane = 'interactive' THEN 0        -- natural interactive
         ELSE 1                                   -- natural background
    END AS effective_priority
  FROM requests
  WHERE state='queued' AND endpoint_hash=?
)
SELECT * FROM dynamic_lane
ORDER BY effective_priority ASC, rowid ASC
LIMIT 1;
```

### Note on Phase 2 Rollout

During Phase 2 (gate background only), interactive traffic bypasses the queue. The `consecutive_interactive_grants` counter does not increment because interactive never enters the queue. The fairness policy is effectively disabled until Phase 3. This is correct — Phase 2's goal is to prevent background from interfering with interactive, not to schedule between them.

---

## 6. Two-Clock Model

| Clock | Starts | Purpose | On expiry |
|-------|--------|---------|-----------|
| `admission_timeout` | Enqueue | Cap queue wait time | Mark row `expired`, notify caller |
| `provider_attempt_timeout` | Lock grant | Cap generation time | `ProviderRequestStalledError` (exit 74) |

**These are independent.** Queue wait never consumes generation time. The total end-to-end timeline:

```
enqueue ─────┬───── grant ───────────────┬───── complete
             │  (admission_timeout)       │  (provider_attempt_timeout)
             └── queue wait ──────────────┘  └── generation ──
```

**ProviderRequestMonitor integration:** The caller MUST call `ProviderRequestMonitor.begin_attempt()` **after** the lock is granted (step 5), not before. This ensures:
- The `provider_attempt_timeout` deadline starts from lock grant, not from enqueue.
- Retry attempts get fresh deadlines via `begin_attempt()`.
- The `ProviderRequestMonitor` constructor can be called at any time (it only stores config, doesn't start the clock).

Optional outer `workflow_deadline` may cap the sum, but it is distinct from retry-attempt accounting.

### Important: `admission_timeout: 0` behavior

If `admission_timeout` is set to `0`, the request **never queues** — it attempts the flock immediately (LOCK_NB) and, if the lock is busy, returns immediately with an `admission.timeout` error. This is useful for auxiliary tasks that can use a fallback if the primary endpoint is busy.

---

## 7. Integration Seam

### 7.1 Primary: `AdmissionClient` wrapper (new class, NOT on Transport)

**Do NOT add admission hooks to `ProviderTransport`.** The transport is a message-format converter, not a lifecycle manager. Instead, introduce an `AdmissionClient` that wraps the HTTP client call.

```python
class AdmissionClient:
    """Cross-process admission gate for scarce endpoints.
    
    Wraps the HTTP client call with acquire/release semantics.
    No-op when admission is disabled for the endpoint.
    """

    def __init__(self, endpoint_hash: str, config: AdmissionConfig):
        self._endpoint_hash = endpoint_hash
        self._config = config
        self._controller = AdmissionController(config)

    def admit(self, lane: str, source: str,
              admission_timeout: float,
              cancel_event: threading.Event | None) -> AdmissionToken:
        """Block until capacity is granted or admission_timeout expires.
        
        Returns a token that MUST be released via release().
        The token is bound to the lock fd — do not copy across threads/processes.
        """
        ...

    def release(self, token: AdmissionToken) -> None:
        """Release the slot. Idempotent (safe to call multiple times)."""
        ...

    @contextmanager
    def acquire(self, lane: str, source: str,
                admission_timeout: float,
                cancel_event: threading.Event | None):
        """Context manager combining admit + release."""
        token = self.admit(lane, source, admission_timeout, cancel_event)
        try:
            yield token
        finally:
            self.release(token)
```

### 7.2 Secondary: `agent/provider_request_watchdog.py`

The `ProviderRequestMonitor` already holds `provider` + `model` identity. Extend it to:
- Accept `admission_token` at construction
- Call `admission_client.release()` in `complete()`, `fail()`, and `cancel()`
- This guarantees release in all terminal paths

```python
class ProviderRequestMonitor:
    def __init__(self, ..., admission_client=None, admission_token=None):
        ...
        self._admission_client = admission_client
        self._admission_token = admission_token

    def complete(self) -> bool:
        ...
        finally:
            self._release_admission()

    def fail(self, error) -> bool:
        ...
        finally:
            self._release_admission()

    def cancel(self, error) -> bool:
        ...
        finally:
            self._release_admission()

    def _release_admission(self):
        if self._admission_client and self._admission_token:
            self._admission_client.release(self._admission_token)
            self._admission_token = None
```

### 7.3 Tertiary: Kanban worker admission

**Problem:** Kanban workers are child processes. The parent (dispatcher) should NOT hold the admission slot on behalf of the child — the parent doesn't make provider calls, and `O_CLOEXEC` prevents fd inheritance.

**Correct approach:** The kanban worker subprocess calls `admission_client.admit()` itself after booting. The pre-flight check in the parent is **optional** (a quick LOCK_NB test to put the task in `blocked` state if the slot is clearly saturated, avoiding subprocess overhead). But the authoritative admission happens inside the worker.

```python
# Pre-flight (parent side, optional optimization):
if admission_client.is_slot_busy(endpoint_hash):
    mark_task_blocked(task_id, reason="admission:slot_busy")
    return

# Worker side (inside subprocess after boot):
with admission_client.acquire(lane='background', source='kanban',
                               admission_timeout=30.0):
    # make the provider call
    ...
```

### 7.4 Quaternary: Auxiliary task deadlock prevention

Auxiliary tasks (titles, compression, MOA) triggered by an interactive session that holds the admission slot must **not** deadlock by queuing behind themselves.

**Solution:** Pass the interactive session's `AdmissionToken` to the auxiliary task's `AdmissionClient`. When a token is provided, the `admit()` call skips the queue and tries `LOCK_NB` immediately (the token is the same lock fd — it's already held). If the call fails (lock already held by someone else? shouldn't happen), fall back to the auxiliary fallback provider.

```python
# In interactive session:
token = admission_client.admit('interactive', 'gateway', ...)
# Inside the turn, when launching auxiliary:
aux_result = run_auxiliary_task(..., inherit_token=token)

# In auxiliary task:
def run_auxiliary_task(..., inherit_token=None):
    if inherit_token:
        # Lock is already held by the parent; no-op acquire
        yield  # skip admission entirely
        ...
    else:
        with admission_client.acquire(...):
            ...
```

### 7.5 Files to modify

| File | Change |
|------|--------|
| **New:** `agent/admission_controller.py` | Core `AdmissionController` + `AdmissionClient` classes |
| **New:** `tests/agent/test_admission_controller.py` | Unit and integration tests |
| **New:** `hermes_cli/admitslots_cli.py` | `hermes admitslots` subcommand |
| `agent/provider_request_watchdog.py` | Accept `admission_client` + `admission_token`, release in terminal paths |
| `run_agent.py` | Instantiate `AdmissionClient` per AIAgent (or share via config); call `acquire()` before `chat.completions.create()` |
| `hermes_cli/kanban_db.py` | Add optional pre-flight admission check (LOCK_NB test) before `subprocess.Popen` |
| `hermes_cli/config.py` | Add admission section to config schema / DEFAULT_CONFIG |
| `cli.py` | Register `hermes admitslots` subcommand |
| `gateway/run.py` | Wire admission for gateway agent runs; asyncio→threading.Event bridging |
| `cron/scheduler.py` | Wire admission for cron job runs |
| `tools/delegate_tool.py` | Wire admission for delegated subagents |
| `agent/auxiliary_client.py` | Wire admission for auxiliary tasks; inherit token from caller |

---

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
        max_slots: 1                   # FUTURE: only 1 is supported in v1
        admission_timeout: 30.0        # max seconds in queue before expiry; 0 = fail-fast
        interactive_burst: 3           # max consecutive interactive before forced background
        max_queue_age: 120             # seconds after which any queued request is promoted
```

When `admission.enabled` is false (default), the admit/release hooks are no-ops — zero overhead for cloud endpoints.

### Config resolution

The endpoint hash is computed once at config load, stored in the `ProviderProfile` or the provider config entry. The `AdmissionClient` retrieves it from the config entry by provider name; it never re-computes the hash from a raw URL.

---

## 9. CLI: `hermes admitslots`

```
hermes admitslots status <endpoint>    # Show slot state, queue depth, active holder
hermes admitslots queue <endpoint>     # List queued requests (id, lane, source, age)
hermes admitslots flush <endpoint>     # Cancel all queued (sets cancel_requested=1)
hermes admitslots drain <endpoint>     # Block new enqueues, let current finish
hermes admitslots reset <endpoint>     # Reap stale abandoned rows, purge old done rows
hermes admitslots prune                # Remove lock files for unknown endpoints
hermes admitslots purge <days>         # DELETE rows with finished_ms older than <days> days
```

---

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

---

## 11. Rollout Plan

### Phase 1 — Instrument (safe, observable)

1. Add the admission DB schema, lock file management, and `AdmissionController` class.
2. Instrument the provider-call entry point with observe-only "would-admit" logging.
3. Expose CLI `hermes admitslots status`.
4. **Benchmark:** Measure poll overhead at 10 concurrent processes, 250ms interval — validate ≤1% CPU increase.
5. Deploy to production. Verify:
   - Queue depth tracks actual concurrency.
   - No false positives (no blocking yet).
   - No throughput regression.
6. **Deliverable:** `hermes admitslots status` shows live concurrency, no behaviour change.

### Phase 2 — Gate background callers

1. Enable admission enforcement for `lane=background` sources (cron, kanban, delegation, auxiliary).
2. Interactive sessions remain un-gated.
3. Verify queue depth drops. Monitor `admission.expired` events for background tasks.
4. **Note:** The fairness policy (`consecutive_interactive` counter) is inactive during Phase 2. This is correct.
5. **Deliverable:** Background workers no longer contend with interactive sessions for the single slot.

### Phase 3 — Full enforcement

1. Enable admission for `lane=interactive` (gateway, CLI).
2. Monitor queue depth, `admission_timeout` expiry rates, and end-to-end latency.
3. Tune `interactive_burst` and `max_queue_age` based on observed ratios.
4. **Deliverable:** All provider calls to admitted endpoints go through the hybrid queue.

### Phase 4 — Hardening

1. Chaos test: `SIGKILL` active holder, kill queued waiter, cancel during selection and generation, SQLite busy/restart/corruption, system reboot.
2. Add `hermes admitslots drain` for graceful endpoint maintenance.
3. Add `hermes admitslots prune` and `purge` for lock file and row cleanup.
4. Add llama.cpp deferred-requests alert.
5. Verify cancel-vs-grant race under concurrent `flush` + grant (use thread barriers).
6. **Deliverable:** Crash-safe, verified under fault injection.

### Phase 5 (Future) — Unix-domain broker evaluation

If polling overhead or `max_slots > 1` requirements justify it, implement a broker-based scheduler:
- Running as a thread in the gateway process (no new daemon).
- All 6 contexts connect via a well-known Unix socket.
- The broker manages an in-memory priority queue with `threading.Condition` wakeup.
- Fall back to the hybrid SQLite+flock design if the broker is unavailable.

---

## 12. Testing Strategy

### Unit tests (pytest, hermetic)

| Test | What it verifies |
|------|------------------|
| `test_single_acquire_release` | Acquire succeeds, release frees for next acquirer |
| `test_concurrent_acquire` | Two processes/threads: one gets lock, other blocks |
| `test_admission_timeout` | Queue wait exceeds timeout → row expired |
| `test_cancellation_while_queued` | Cancel() atomically removes from queue; cancel_requested set |
| `test_cancel_grant_race` | **Use thread barriers:** cancel arrives exactly during grant transaction → lock released without HTTP call |
| `test_fairness_burst` | 4 interactive + 1 background → background forced after 3 interactive |
| `test_promotion_by_age` | Background request exceeds max_queue_age → promoted to front |
| `test_crash_recovery_flock_only` | Simulate crash after `LOCK_EX` / after SQLite commit — verify next waiter reclaims (step 3e path) |
| `test_crash_recovery_full` | Simulate crash with `state='running'` and dead PID — verify reclamation via PID check |
| `test_cancel_grant_race_cross_process` | Concurrent `flush` + grant: verify cancel wins, no silent swallow |
| `test_degraded_flock_only` | SQLite unavailable → explicit degrade to plain flock (LOCK_NB only, no queue) |
| `test_schema_migration` | Schema version bump is backward-compatible |
| `test_cli_status` | `hermes admitslots status` returns valid JSON |
| `test_noop_when_disabled` | `admission.enabled: false` → zero overhead |
| `test_seq_atomic_insert` | Concurrent inserts produce unique rowids, no constraint violation |
| `test_auxiliary_no_deadlock` | Auxiliary task with inherited token completes without re-entering queue |
| `test_row_purge` | Purge of old done rows succeeds, doesn't affect queued rows |
| `test_admission_timeout_zero` | admission_timeout=0 → immediate LOCK_NB, no queue wait |
| `test_sqlite_corruption` | Delete/adversarially corrupt .db file mid-operation → degrade path works |

### Integration tests

| Test | What it verifies |
|------|------------------|
| `test_gateway_admission` | Gateway conversation thread acquires and releases |
| `test_kanban_worker_admission` | Kanban subprocess blocks on admission before provider call (inside worker) |
| `test_cron_admission` | Cron job enqueues, waits, acquires |
| `test_mixed_contexts` | Gateway + cron + kanban all target same endpoint concurrently |
| `test_kanban_preflight` | Pre-flight check in parent avoids spawning child when slot busy |

### Chaos tests (manual / test harness)

| Scenario | Expected behaviour |
|----------|-------------------|
| `kill -9` active process after LOCK_EX, before commit | `flock` auto-released; step 3(e) detects free lock, reclaims. |
| `kill -9` active process after commit (state=running) | `flock` auto-released; step 3(a) detects stale PID, marks abandoned. |
| Kill queued process | Row stays queued; expires on admission_timeout. |
| Flush while grant is in progress | cancel_requested set; step 4(b) detects flag, releases lock. |
| SQLite database deleted mid-operation | Admission degrades to `flock`-only (LOCK_NB); re-create DB on next opportunity. |
| SQLite database corrupted mid-operation | `BEGIN IMMEDIATE` raises `DatabaseError`; degrade to flock-only. |
| System reboot | Boot ID mismatch cleans stale rows on next admission (step 3a). |

---

## 13. Files to Create / Modify

### New files

| File | Purpose |
|------|---------|
| `agent/admission_controller.py` | Core `AdmissionController` + `AdmissionClient` classes: SQLite queue, flock acquire/release, fairness policy |
| `tests/agent/test_admission_controller.py` | Unit and integration tests |
| `hermes_cli/admitslots_cli.py` | `hermes admitslots` subcommand |

### Modified files

| File | Change |
|------|--------|
| `agent/provider_request_watchdog.py` | Accept `admission_client`, `admission_token`; call release in all terminal paths |
| `run_agent.py` | Create/shared `AdmissionClient`; call `acquire()` before provider HTTP call |
| `hermes_cli/kanban_db.py` | Optional pre-flight LOCK_NB test before `subprocess.Popen` |
| `hermes_cli/config.py` | Add admission section to config schema / DEFAULT_CONFIG |
| `cli.py` | Register `hermes admitslots` subcommand |
| `gateway/run.py` | Wire admission for gateway agent runs; asyncio→threading.Event bridge |
| `cron/scheduler.py` | Wire admission for cron job runs |
| `tools/delegate_tool.py` | Wire admission for delegated subagents |
| `agent/auxiliary_client.py` | Wire admission for auxiliary tasks; inherit token from caller |

### Files NOT to modify (rationale)

| File | Not changed because |
|------|-------------------|
| `agent/transports/base.py` | Transport is a message formatter, not a lifecycle manager. Admission hooks go on `AdmissionClient`. |
| `agent/transports/chat_completions.py` | Same as above — transport doesn't make HTTP calls directly. |
| `agent/backend_identity.py` | Hash is computed once at config load; no re-normalization in admission path. |

---

## 14. Windows Compatibility (Design Notes)

The current design requires `fcntl.flock` (Unix). For Windows support (Phase 5), these equivalences apply:

| Unix | Windows Equivalent |
|------|-------------------|
| `fcntl.flock(fd, LOCK_EX)` | `msvcrt.locking(fd, msvcrt.LK_LK, 1)` |
| `fcntl.flock(fd, LOCK_NB)` | `msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)` |
| `os.O_CLOEXEC` | Not needed; Windows handles via `subprocess.Popen(close_fds=True)` |
| `/proc/sys/kernel/random/boot_id` | `GetTickCount64()` for boot epoch (or `os.popen('systeminfo | findstr "Boot Time"')`) |
| `/proc/{pid}/stat` | `psutil.pid_exists()` (cross-platform) |

The `AdmissionController` should have a `_platform_lock()` abstraction:

```python
import platform

def _platform_lock(fd, blocking=True):
    """Cross-platform file lock acquisition."""
    if platform.system() == 'Windows':
        import msvcrt
        mode = msvcrt.LK_LK if blocking else msvcrt.LK_NBLCK
        msvcrt.locking(fd, mode, 1)
    else:
        import fcntl
        flags = fcntl.LOCK_EX
        if not blocking:
            flags |= fcntl.LOCK_NB
        fcntl.flock(fd, flags)
```

---

## Appendix A: Row Retention / Purge Policy

To prevent unbounded growth of the `requests` table:

```sql
-- Run periodically (every 1000 inserts or via "hermes admitslots purge"):
DELETE FROM requests
WHERE state IN ('done', 'cancelled', 'expired', 'abandoned')
  AND finished_ms IS NOT NULL
  AND finished_ms < ? - (7 * 86400 * 1000);  -- 7 days retention
```

The purge is best-effort — never block critical path for cleanup. Run inside a `BEGIN IMMEDIATE` with `busy_timeout=100` and skip if contention.

---

## Appendix B: Boot ID and Stale Row Reclamation

```python
import os

_CACHED_BOOT_ID: str | None = None

def _read_boot_id() -> str:
    """Read /proc/sys/kernel/random/boot_id, caching once per process."""
    global _CACHED_BOOT_ID
    if _CACHED_BOOT_ID is None:
        try:
            _CACHED_BOOT_ID = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        except OSError:
            _CACHED_BOOT_ID = ""  # non-Linux fallback
    return _CACHED_BOOT_ID

def _owner_is_alive(owner_pid: int, owner_start_ms: int) -> bool:
    """Check if a process is the original lock holder (by PID + creation time)."""
    if owner_pid is None or owner_start_ms is None:
        return False
    try:
        import psutil
        proc = psutil.Process(owner_pid)
        # Compare creation time in ms precision
        proc_create_ms = int(proc.create_time() * 1000)
        return abs(proc_create_ms - owner_start_ms) < 1000  # within 1s tolerance
    except (psutil.NoSuchProcess, psutil.AccessDenied, ImportError):
        return False

def _reap_stale_running(cursor, endpoint_hash: str, now_mono: int, now_wall: int) -> None:
    """Mark stale 'running' rows as 'abandoned'."""
    current_boot_id = _read_boot_id()
    
    for row in cursor.execute(
        "SELECT id, owner_pid, owner_start_ms, boot_id FROM requests "
        "WHERE endpoint_hash=? AND state='running'",
        (endpoint_hash,)
    ):
        stale = False
        # 1. Boot ID mismatch → different boot epoch
        if current_boot_id and row["boot_id"] and row["boot_id"] != current_boot_id:
            stale = True
        # 2. PID not alive
        if not stale and row["owner_pid"] is not None:
            if not _owner_is_alive(row["owner_pid"], row["owner_start_ms"]):
                stale = True
        
        if stale:
            cursor.execute(
                "UPDATE requests SET state='abandoned', finished_ms=? WHERE id=?",
                (now_wall, row["id"])
            )
```

---

## Appendix C: Cancellation Contract

```
While queued (in-process):   cancel_event.set(); UPDATE row state='cancelled' (best-effort)
While queued (cross-process): UPDATE requests SET cancel_reason=?, cancel_requested=1
                              WHERE id=? AND state IN ('queued', 'running')
Grant/cancel race (post-lock): Re-check cancel_requested flag. If set → release flock + return.
During generation:            Propagate to HTTP stream (httpx client.close() / cancel()).
                              Lock released in finally via ProviderRequestMonitor.
Process death:                Kernel releases flock. Row → abandoned on next acquisition (boot_id/PID check).
```

---

## Appendix D: Lock File Management

```python
_LOCK_DIR = get_hermes_home() / "admitslots"

def _lock_path(endpoint_hash: str) -> Path:
    return _LOCK_DIR / f"{endpoint_hash}.lock"

# On first use:
_LOCK_DIR.mkdir(parents=True, exist_ok=True)
lock_path = _lock_path(endpoint_hash)
lock_path.touch(mode=0o644, exist_ok=True)  # NEVER use open(path,'w').close()

# Acquire (non-blocking):
fd = os.open(lock_path, os.O_RDWR | os.O_CLOEXEC)
try:
    if platform.system() == 'Windows':
        import msvcrt
        msvcrt.locking(fd.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except (BlockingIOError, OSError):
    os.close(fd)
    # ... retry loop ...

# Release:
if platform.system() == 'Windows':
    msvcrt.locking(fd.fileno(), msvcrt.LK_UNLCK, 1)
else:
    fcntl.flock(fd, fcntl.LOCK_UN)
os.close(fd)
```

### Advisory locking note

`flock` is **advisory** — it only works if all consumers use the same mechanism. A rogue process that calls the endpoint without going through admission will bypass the lock entirely. This is acceptable because:
1. All Hermes provider calls go through `run_agent.py` → `AdmissionClient`.
2. External scripts that hit the endpoint directly are outside the admission scope.
3. The `max_slots=1` token is a scheduling optimization, not a security mechanism.

---

## Appendix E: Lane Classification Reference

```python
def classify_lane(source: str, is_interactive: bool) -> str:
    """Classify a request into 'interactive' or 'background' lane.
    
    The is_interactive flag is the sole determinant.
    source is logged for observability but does not affect lane assignment.
    """
    if is_interactive:
        return "interactive"
    return "background"

# Context → default is_interactive mapping:
CONTEXT_DEFAULTS = {
    'gateway_human':  True,   # chat session with a human
    'gateway_api':    False,  # API server / automated caller
    'cli_interactive': True,  # no -q flag
    'cli_quiet':      False,  # -q flag
    'cron':           False,
    'kanban':         False,
    'delegation':     False,
    'auxiliary':      None,   # INHERIT from caller's is_interactive
}
```
