# Provider Admission Controller — FINAL Specification

**Status:** Implementation-ready (reviewed: original + orchestrator self-review + Claude Code external review)  
**Author:** Hermes Orchestrator  
**Date:** 2026-07-29  
**Related:** Provider-request stall recovery (exit-74 chain), AI-VM single-slot coordination

---

## Revision History

| Version | Changes |
|---|---|
| v1 (original) | Draft spec at `docs/design/admission-controller.md` |
| v2 (self-corrected) | 8 issues fixed: AUTOINCREMENT, N-slot files, 4-step cancel, PID reuse, retry-span lock, circuit breaker, lane classification note, O_CLOEXEC enforcement |
| v3 (Claude review) | 30+ issues found; critical races and architecture fixes applied below |

**All earlier drafts archived:** `docs/design/admission-controller.md` (v1), `corrected-spec-review.md` (v2).

---

## 1. Problem

Hermes makes provider API calls from **six distinct contexts** with zero shared admission state (see v1 §1). A `--parallel 1` llama.cpp endpoint can receive concurrent requests from all six simultaneously — the first one fills the slot, the other five burn their provider timeouts waiting in the server's internal queue. `ProviderRequestStalledError` (exit 74) is reactive, not preventive.

**Goal:** Host-wide, cross-process admission control for scarce endpoints.

---

## 2. Architecture: Two-Layer Design

### Layer 1 — Capacity token: `fcntl.flock` on slot lock files
- Each slot has its own lock file at `~/.hermes/admitslots/<hash>/slot-{N}.lock`
- `LOCK_EX` is the authoritative gating mechanism; the kernel auto-releases on process death
- For `max_slots=N`, acquire scans files 0..N-1 with `LOCK_EX|LOCK_NB` until one succeeds

### Layer 2 — Queue metadata: SQLite
- Enqueue order, priority, state, deadlines, observability
- SQLite is **advisory** — the flock is the sole capacity gate
- SQLite row marked `running` + released flock = holder crashed → next waiter reclaims

### Why not other approaches (see v1 §2 for full table)

Hybrid design is the **minimal correct solution**. SQLite is already a Hermes dependency; it adds no new surface. A filesystem-only queue re-invents ACID badly. A Unix-domain broker adds deployment complexity. Server-internal queue consumes the provider timeout.

---

## 3. Queue Schema

File: `~/.hermes/admitslots.db`

```sql
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 250;        -- match poll interval; see §3.1

CREATE TABLE IF NOT EXISTS requests (
    id                TEXT PRIMARY KEY,                       -- UUID
    rowid             INTEGER NOT NULL,                       -- SQLite autoincrement (use as monotonic seq)
    lane              TEXT NOT NULL,                          -- 'interactive' | 'background'
    source            TEXT NOT NULL,                          -- 'gateway' | 'cli' | 'cron' | 'kanban' | 'delegation' | 'auxiliary'
    state             TEXT NOT NULL DEFAULT 'queued',          -- queued | running | done | cancelled | expired | abandoned
    enqueued_ms       INTEGER NOT NULL,                       -- wall clock ms at enqueue
    queue_deadline_ms INTEGER,                                -- admission_timeout deadline (wall ms)
    owner_pid         INTEGER,                                -- PID that holds the flock
    owner_start_ms    INTEGER,                                -- /proc/self/stat field 22 converted to ms (catches PID reuse)
    boot_id           TEXT,                                   -- /proc/sys/kernel/random/boot_id at grant
    started_ms        INTEGER,                                -- when lock was acquired (wall ms)
    finished_ms       INTEGER,                                -- when lock was released (wall ms)
    cancel_reason     TEXT,                                   -- why cancelled, if applicable
    endpoint_hash     TEXT NOT NULL,                          -- sha256 of normalized base_url
    slot_index        INTEGER,                                -- which slot (0..max_slots-1) was acquired
    cancel_requested  INTEGER NOT NULL DEFAULT 0              -- 1 = external cancel requested; checked at grant time
);

CREATE INDEX IF NOT EXISTS idx_queue ON requests(endpoint_hash, state, rowid);
CREATE INDEX IF NOT EXISTS idx_expire ON requests(queue_deadline_ms);
CREATE INDEX IF NOT EXISTS idx_cleanup ON requests(endpoint_hash, state) WHERE state IN ('done','cancelled','expired','abandoned');

CREATE TABLE IF NOT EXISTS endpoints (
    endpoint_hash               TEXT PRIMARY KEY,
    base_url                    TEXT NOT NULL,
    max_slots                   INTEGER NOT NULL DEFAULT 1,
    admission_timeout           REAL NOT NULL DEFAULT 30.0,
    interactive_burst           INTEGER NOT NULL DEFAULT 3,
    max_queue_age               REAL NOT NULL DEFAULT 120.0,
    circuit_breaker_threshold   INTEGER NOT NULL DEFAULT 5,
    circuit_breaker_cooldown    REAL NOT NULL DEFAULT 300.0,
    consecutive_timeout_count   INTEGER NOT NULL DEFAULT 0,
    blocked_until_ms            INTEGER
);

-- Schema version tracking for migrations
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL
);
INSERT OR IGNORE INTO schema_version (version, applied_at) VALUES (1, CAST(strftime('%s','now') AS INTEGER) * 1000);
```

### Design note: no `scheduler_state` table

`consecutive_interactive` and circuit breaker state live on the `endpoints` table. All per-endpoint scheduler state is one row, no joins needed. The `consecutive_interactive` counter tracks **consecutive interactive grants**, not completions — with `max_slots=1` they're equivalent; if `max_slots > 1` is later added, the semantics must be revisited.

### Endpoint identity

The endpoint is identified by **SHA-256 of its normalized base URL**. Normalization uses **one** function exclusively: `hermes_cli/route_identity.py::normalize_route_base_url()`. The hash is computed once at config load time and stored in the `BackendIdentity` dataclass. The admission controller receives the pre-computed hash — it never re-normalizes.

### Lock file path

```
~/.hermes/admitslots/<sha256[:16]>/slot-{0..max_slots-1}.lock
```

Created on first use: `Path.touch(mode=0o644, exist_ok=True)`. NEVER use `open(path, 'w').close()` — it truncates the file between another process's open and flock call, which can corrupt the lock.

---

## 4. Acquire Sequence (Critical Path)

```
┌──────────────────────────────────────────────────┐
│ 1. INSERT queued row (outside any transaction)   │
│    id=uuid, lane=..., source=...,                │
│    queue_deadline_ms=enqueued_ms+timeout         │
└─────────────────────┬────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────┐
│ 2. Wait loop (admission_timeout / cancel_event)   │
│    Poll every 250ms ±50ms (jittered)             │
│    Check cancel_event after each failed attempt   │
│    On cancel set: UPDATE cancel_requested=1       │
│    Return None (admission cancelled)              │
└─────────────────────┬────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────┐
│ 3. BEGIN IMMEDIATE (busy_timeout=250ms → if      │
│    SQLITE_BUSY, fall through and retry loop)      │
│                                                   │
│    a) Clean stale rows:                           │
│       UPDATE requests SET state='abandoned'       │
│       WHERE endpoint_hash=? AND state='running'   │
│       AND _is_holder_dead(owner_pid,              │
│            owner_start_ms, boot_id)=TRUE          │
│                                                   │
│    b) Expire timed-out queued rows:               │
│       UPDATE requests SET state='expired'         │
│       WHERE endpoint_hash=? AND state='queued'    │
│       AND queue_deadline_ms < now_ms              │
│                                                   │
│    c) Check circuit breaker:                      │
│       IF blocked_until_ms > now_ms:               │
│         UPDATE self state='expired'               │
│         reason='circuit_breaker'; commit; return  │
│                                                   │
│    d) Select next eligible request:               │
│       SELECT id, lane, enqueued_ms                │
│       FROM requests                               │
│       WHERE endpoint_hash=? AND state='queued'    │
│       AND queue_deadline_ms > now_ms              │
│       ORDER BY                                    │
│         -- age promotion: aged → interactive → fg │
│         CASE WHEN (? - enqueued_ms) > ?           │
│           THEN 0 ELSE 1 END ASC,                  │
│         -- lane: interactive first                │
│         CASE WHEN lane='interactive'              │
│           THEN 0 ELSE 1 END ASC,                  │
│         -- then by rowid (FIFO within group)      │
│         rowid ASC                                 │
│       LIMIT 1                                     │
│                                                   │
│    e) If selected row is NOT this request:        │
│       → CRITICAL: check if the selected row's     │
│         holder crashed (lock is free). Test with  │
│         LOCK_NB on any slot. If free:             │
│         - Mark stale row 'abandoned'              │
│         - Re-SELECT (goto d)                      │
│       → Otherwise: rollback, retry loop           │
│                                                   │
│    f) If selected row IS this request:            │
│       Check cancel_requested flag:                │
│         IF cancel_requested = 1 →                 │
│           UPDATE state='cancelled'; emit event    │
│           commit; close any opened fd; return     │
│                                                   │
│    g) Attempt LOCK_EX|LOCK_NB on each slot:       │
│       for slot in 0..max_slots-1:                 │
│         fd = os.open(lock_path, O_RDWR|O_CLOEXEC) │
│         try: fcntl.flock(fd, LOCK_EX|LOCK_NB)     │
│         except BlockingIOError: close(fd); cont.  │
│         else: slot_index = slot; break            │
│                                                   │
│    h) If all slots busy: rollback, retry loop     │
└─────────────────────┬────────────────────────────┘
                      │
           ┌──────────┴──────────┐
           ▼                     ▼
    All slots busy?         Slot free!
    Rollback tx              ┌────────────────────────────┐
    Retry loop               │ 4. Mark this row running    │
                             │    a) UPDATE state='running'│
                             │       owner_pid=os.getpid() │
                             │       owner_start_ms=...    │
                             │       boot_id=...           │
                             │       slot_index=slot_idx   │
                             │       started_ms=now_ms     │
                             │    b) UPDATE endpoints      │
                             │       consecutive_interactive  │
                             │       += (lane='interactive') │
                             │       OR = 0 (lane='bg')    │
                             │    c) Commit tx             │
                             │    d) KEEP flock fd open    │
                             └─────────┬──────────────────┘
                                       │
                                       ▼
                        ┌──────────────────────────────────┐
                        │ 5. Start provider attempt          │
                        │    deadline timer starts NOW       │
                        │    Lock held across ALL retries    │
                        │    within provider_attempt_timeout │
                        └──────────────────────────────────┘
```

### Critical: The "next waiter reclaims" crash gap (v3 §1.1)

If process A crashes after `LOCK_EX` but before SQLite commit, A's row is still `queued` with an early seq. Process B polls, selects A's row (it's the "next eligible" request). Step 3(e) catches this: the selected row is NOT B's request, so B does a `LOCK_NB` probe. If the lock is free (A crashed), B marks A's row `abandoned` and re-selects. This prevents the original spec's 30-second blockage window.

**Implementation detail:** The `LOCK_NB` probe in step 3(e) must attempt ALL slots, not just the first — A might have crashed on slot 0 while slot 1 is held by a different process.

### Critical: Cancel-vs-grant race (v3 §1.2)

The race: external cancel (`UPDATE cancel_requested=1`) executes concurrently with process A's `BEGIN IMMEDIATE` transaction. Inside the transaction, A selects itself, acquires the lock, and is about to mark itself `running`. The cancel UPDATE blocks on the transaction (SQLite locking). After A commits, the cancel UPDATE fires — but A is now `running`.

**Fix (4 steps):**
1. Before attempting `LOCK_EX|LOCK_NB`, check `cancel_requested` on the selected row (step 3f).
2. After acquiring the lock but BEFORE committing, re-read `cancel_requested` from the row inside the same transaction. If now set → close fd, UPDATE `state='cancelled'`, commit (reports "cancelled" not "running").
3. After commit, check once more outside the transaction (defence in depth).
4. The cancel path uses `UPDATE cancel_requested=1 WHERE id=?` (no `AND state='queued'`), so it never blocks on a state mismatch.

This gives three checkpoints: pre-lock, pre-commit (inside tx), and post-commit.

---

## 5. Fairness Policy

### Lanes

| Lane | Source | Classification |
|---|---|---|
| `interactive` | Gateway chat sessions, CLI interactive | User-facing, waiting |
| `background` | Cron, kanban, delegation, CLI `-q`, auxiliary | Non-blocking |

**Classification propagation:** Each context explicitly passes `source=` and an `is_interactive` boolean. The admission controller never infers `is_interactive` from `source`. For auxiliary tasks: inherit `is_interactive` from the calling AIAgent's session context, not hard-coded to `False`. This prevents deadlock (see §7.4).

### Scheduling rule

- **FIFO within group** by SQLite `rowid` (monotonic, race-free)
- **Group ordering:** aged (any lane, ≥`max_queue_age`) → interactive → background
- **Burst protection:** After `interactive_burst` consecutive interactive grants **while at least one background request is queued**, force the oldest background request next. Counter `consecutive_interactive` resets on any background grant.

### SQL ordering

```sql
ORDER BY
  CASE WHEN (? - enqueued_ms) > ? THEN 0 ELSE 1 END ASC,  -- age promotion
  CASE WHEN lane = 'interactive' THEN 0 ELSE 1 END ASC,   -- lane priority
  rowid ASC                                                 -- FIFO within group
```

### Age promotion (`max_queue_age`)

Any request whose queue wait exceeds `max_queue_age` (default 120s) is promoted to the front of the wait group. The SQL `CASE` expression computes this dynamically — no stale file renames, no race conditions.

---

## 6. Two-Clock Model

| Clock | Starts | On expiry |
|---|---|---|
| `admission_timeout` | Enqueue | Row `expired`, caller notified |
| `provider_attempt_timeout` | Lock grant | `ProviderRequestStalledError` (exit 74) |

**Independent.** Queue wait never consumes generation time. Lock spans all retries within `provider_attempt_timeout`.

---

## 7. Integration Seam (v3 §4.1 — revised from v1)

### The transport base class is the wrong abstraction

`agent/transports/base.py` (`ProviderTransport`) is a **message format converter** — it handles `convert_messages` → `build_kwargs` → `normalize_response`. It knows nothing about HTTP clients, connection pools, or network calls. Adding `admit()`/`release()` here pollutes the abstraction.

### Correct seam: `AIAgent` HTTP client call site

Admission hooks live at the **client call site** in `AIAgent` or in an `AdmissionClient` wrapper around the HTTP client. The actual call chain is:

```
AIAgent.run_conversation()
  → _make_api_request()           # builds client, calls transport
    → client.chat.completions.create()  # THE call
  → _handle_response()            # normalizes
```

Place admission at `_make_api_request()` before `client.chat.completions.create()` and release in the `finally` block after.

### Implementation

```python
class AdmissionClient:
    """Wraps an HTTP client with admission control."""
    
    def __init__(self, client, endpoint_hash, config):
        self._client = client
        self._endpoint_hash = endpoint_hash
        self._config = config
    
    def chat_completions_create(self, **kwargs):
        if not self._config.admission.enabled:
            return self._client.chat.completions.create(**kwargs)
        
        token = admission_controller.admit(
            endpoint_hash=self._endpoint_hash,
            lane=classify_lane(),
            source=get_session_source(),
            admission_timeout=self._config.admission.admission_timeout,
            cancel_event=get_cancel_event(),
        )
        if token is None:
            raise AdmissionTimeoutError("admission timed out or cancelled")
        
        try:
            return self._client.chat.completions.create(**kwargs)
        finally:
            admission_controller.release(token)
```

### Per-context wiring

| Context | Where admission is called |
|---|---|
| Gateway | `AIAgent._make_api_request()` — automatically covered |
| CLI | Same — automatically covered |
| Delegation | Same — automatically covered |
| Kanban worker | **Self-admission**: the child process boots, then calls `AdmissionClient` after loading config. Parent does NOT hold the lock for the child (O_CLOEXEC prevents fd inheritance) |
| Cron | Same — automatically covered |
| Auxiliary | Same — but see §7.4 for deadlock prevention |

### Kanban worker pre-flight admission (v3 §4.3)

**Decision:** The child process does its own admission after boot. The parent (dispatcher) does NOT acquire a slot for the child because:
- The boot time (loading config, module imports, session init) would waste the slot
- `O_CLOEXEC` prevents fd inheritance anyway
- The child can use the existing `AdmissionClient` wrapper

### Auxiliary task deadlock prevention (v3 §4.4)

**Problem:** An interactive session holds the slot. It triggers an auxiliary task (title generation, compression, MOA). If the auxiliary task goes through admission, it queues behind the interactive session's row → deadlock (interactive can't finish until auxiliary completes).

**Fix:** Auxiliary tasks **inherit the slot** from the parent AIAgent. The admission controller provides:

```python
class AdmissionToken:
    def create_child_token(self) -> AdmissionToken:
        """Create a dependent token for an auxiliary subtask.
        
        The child token does NOT consume a separate slot — it shares
        the parent's slot. The parent's release() waits for all child
        tokens to be released before releasing the slot.
        """
```

If inheritance is not possible (e.g., the parent already released), auxiliary tasks use `admission_timeout=0` (fail-fast) and fall back to the provider-stall recovery chain.

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
        enabled: true                    # default false
        max_slots: 1                     # N lock files created
        admission_timeout: 30.0          # seconds; 0 = fail-fast
        interactive_burst: 3
        max_queue_age: 120.0
        circuit_breaker_threshold: 5     # consecutive timeouts
        circuit_breaker_cooldown: 300.0  # seconds to block
```

When `admission.enabled: false` (default), `AdmissionClient` returns immediately — zero overhead.

---

## 9. CLI: `hermes admitslots`

```text
hermes admitslots status <endpoint>     # Slot state, queue depth, active holder
hermes admitslots queue <endpoint>      # List queued (id, lane, source, age)
hermes admitslots cancel <id>           # Cancel a specific queued request
hermes admitslots flush <endpoint>      # Cancel all queued
hermes admitslots drain <endpoint>      # Block new enqueues, let current finish
hermes admitslots reset <endpoint>      # Reap stale rows, re-create lock files
hermes admitslots circuit <endpoint>    # Show/reset circuit breaker
hermes admitslots prune                 # Clean lock files for removed endpoints
```

Tab-completion registration: `hermes completion bash` / `zsh` must include `admitslots` subcommand.

---

## 10. Observability

### Lifecycle events

| Event | Payload |
|---|---|
| `admission.enqueued` | id, lane, source, endpoint_hash, queue_deadline_ms |
| `admission.granted` | id, queue_wait_seconds, slot_index |
| `admission.completed` | id, service_time_seconds |
| `admission.cancelled` | id, cancel_reason, queue_wait_seconds |
| `admission.expired` | id, queue_wait_seconds, reason |
| `admission.abandoned` | id (stale row reaped) |
| `admission.degraded` | id, reason |
| `admission.circuit_broken` | endpoint_hash, cooldown_seconds |

### Gauges

- `hermes_admission_queue_depth{lane, endpoint_hash}`
- `hermes_admission_active_slots{endpoint_hash, slot_index}`
- `hermes_admission_oldest_queued_age_seconds{endpoint_hash}`
- `hermes_admission_consecutive_interactive{endpoint_hash}`
- `hermes_admission_circuit_broken{endpoint_hash}` (0/1)

### Histograms

- `hermes_admission_queue_wait_seconds{lane, endpoint_hash}`
- `hermes_admission_service_time_seconds{endpoint_hash}`

### Cross-check: llama.cpp `/slots` endpoint

- `GET /slots` returns per-slot `state` (idle/processing), `prompt_tokens`, `completions_tokens`
- Compare `hermes_admission_active_slots` vs number of non-idle llama.cpp slots
- Alert if difference > 0 → bypass or release timing bug

---

## 11. Rollout Plan (updated for v3 findings)

### Phase 1 — Instrument (safe, observable)

1. Implement `AdmissionController` class, DB schema (1 migration version), lock file management.
2. Wire `AdmissionClient` at `AIAgent._make_api_request()` with `admission.enabled: false`.
3. Add observe-only "would-admit" logging (no gating).
4. Expose CLI `hermes admitslots status`.
5. **Deliverable:** Queue depth visible, zero behaviour change. **Performance benchmark** as gate-check (poll loop CPU overhead ≤ 0.1%, WAL file growth ≤ 1MB/day).

### Phase 2 — Gate background callers

1. Enable admission for `lane=background` sources.
2. Interactive sessions bypass admission entirely.
3. Fairness mechanism is **not active** during this phase (no interactive grants tracked) — document this explicitly.
4. **Deliverable:** Background workers queued and serialized. No interactive impact.

### Phase 3 — Full enforcement

1. Enable admission for all lanes.
2. Fairness algorithm active. Deadlock guards active (auxiliary token inheritance).
3. Tune `interactive_burst`, `max_queue_age` based on observed latencies.
4. **Deliverable:** All provider calls gated. Queue depth, wait times, and service times visible.

### Phase 4 — Hardening

1. Chaos: `SIGKILL` holder, kill queued waiter, cancel during SELECT/grant/generation, SQLite corruption/recovery, system reboot.
2. Circuit breaker: auto-block endpoint after threshold of consecutive `admission_timeout` failures.
3. Add cross-check alert (llama.cpp deferred vs external queue).
4. Add `hermes admitslots drain` for maintenance.
5. **Deliverable:** Crash-safe, verified under fault injection.

---

## 12. Testing Strategy

### Unit tests (pytest, hermetic, Linux-only for flock tests)

| # | Test | What it verifies |
|---|---|---|
| 1 | `test_single_acquire_release` | Acquire succeeds, release frees next acquirer |
| 2 | `test_concurrent_acquire` | Two processes: one gets lock, other blocks on same slot |
| 3 | `test_multi_slot_acquire` | Two processes: different slots when max_slots>1 |
| 4 | `test_admission_timeout` | Queue wait exceeds → row expired |
| 5 | `test_cancellation_while_queued` | Cancel removes from queue |
| 6 | `test_cancel_grant_race` | Cancel arrives during grant tx → grant aborted (3 checkpoints verified) |
| 7 | **`test_next_waiter_reclaims_crash_gap`** | Process crashes after LOCK_EX before commit → next waiter reclaims within 1 poll cycle |
| 8 | `test_fairness_burst` | 4 interactive + 1 bg → background forced after interactive_burst |
| 9 | `test_promotion_by_age` | Background exceeds max_queue_age → promoted |
| 10 | `test_crash_recovery` | Crash after LOCK_EX + after commit — reclaim |
| 11 | `test_pid_reuse_detection` | Same PID, different start time → stale detected |
| 12 | `test_o_cloexec` | Lock fd does not leak to subprocess |
| 13 | `test_circuit_breaker` | N consecutive timeouts → endpoint blocked |
| 14 | `test_circuit_breaker_reset` | Successful grant resets counter |
| 15 | **`test_auxiliary_deadlock`** | Interactive session + auxiliary: child token does not block |
| 16 | `test_degraded_flock_only` | SQLite unavailable → plain flock fallback |
| 17 | **`test_sqlite_corruption`** | DB corruption during IMMEDIATE → degrade gracefully |
| 18 | `test_schema_migration` | Version bump backward-compatible |
| 19 | `test_cli_status` | `hermes admitslots status` returns valid JSON |
| 20 | `test_noop_when_disabled` | Zero overhead when disabled |
| 21 | **`test_concurrent_enqueue_seq`** | 10 concurrent INSERTs → all get distinct monotonic rowid |

### Integration tests

| Test | What it verifies |
|---|---|
| `test_gateway_admission` | Gateway thread acquires/releases via `AdmissionClient` |
| `test_kanban_worker_self_admission` | Kanban subprocess does own admission after boot |
| `test_cron_admission` | Cron job enqueues, waits, acquires |
| `test_mixed_contexts` | Gateway + cron + kanban to same endpoint |

### Chaos tests

| Scenario | Expected behaviour |
|---|---|
| `kill -9` active holder | `flock` released; next waiter reclaims within 1 poll cycle |
| `kill -9` queued waiter | Row stays queued; expires on admission_timeout |
| SQLite DB deleted | Degrade to flock-only; re-create DB on next operation |
| SQLite DB corrupted during IMMEDIATE | Catch `DatabaseError`, degrade to flock-only, log error |
| System reboot | Boot ID mismatch → stale rows abandoned on first admission |
| Rapid PID reuse (tight restart loop) | `owner_start_ms` disambiguates |
| Lock file deleted while held | Hold remains on original inode (fd ref); new lock file created → untouched |

---

## 13. Files to Create / Modify

### New files

| File | Purpose |
|---|---|
| `agent/admission_controller.py` | Core: SQLite queue, flock acquire/release, fairness, circuit breaker |
| `tests/agent/test_admission_controller.py` | Unit and integration tests |
| `hermes_cli/admitslots_cli.py` | `hermes admitslots` subcommand |

### Modified files

| File | Change |
|---|---|
| `agent/run_agent.py` (`_make_api_request`) | Wrap HTTP client with `AdmissionClient` |
| `agent/provider_request_watchdog.py` | Accept `admission_token`, call `release()` in `complete()`, `fail()`, `cancel()` |
| `hermes_cli/kanban_db.py` | No pre-flight admission (replaced by self-admission). Remove commented-out pre-flight code. |
| `hermes_cli/config.py` | Add `admission` section to `DEFAULT_CONFIG` |
| `hermes_cli/route_identity.py` | Minor: expose `normalize_route_base_url` as canonical (no changes needed; already public) |
| `cli.py` | Register `hermes admitslots` subcommand + tab-completion |
| `tools/delegate_tool.py` | Propagate `is_interactive` flag from parent to child |
| `cron/scheduler.py` | No changes needed (covers via `AIAgent` chain) |
| `gateway/run.py` | No changes needed (covers via `AIAgent` chain) |
| `agent/transports/base.py` | No changes needed (admission moved to `AIAgent` layer) |

---

## Appendix A: Lock File Lifecycle

```python
import os, fcntl
from pathlib import Path

_LOCK_DIR = Path(os.environ["HERMES_HOME"]) / "admitslots"

def _lock_dir(endpoint_hash: str) -> Path:
    return _LOCK_DIR / endpoint_hash[:16]

def _lock_path(endpoint_hash: str, slot_index: int) -> Path:
    return _lock_dir(endpoint_hash) / f"slot-{slot_index}.lock"

def _ensure_lock_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(mode=0o644, exist_ok=True)     # NEVER open(path, 'w').close()

def _try_acquire_slot(endpoint_hash: str, max_slots: int):
    """Try to acquire any free slot. Returns (fd, slot_index) or None."""
    for slot_index in range(max_slots):
        path = _lock_path(endpoint_hash, slot_index)
        _ensure_lock_file(path)
        try:
            fd = os.open(str(path), os.O_RDWR | os.O_CLOEXEC)
        except OSError:
            continue
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return (fd, slot_index)
        except BlockingIOError:
            os.close(fd)
            continue
    return None

def _release_slot(fd: int) -> None:
    """Close the lock fd. Kernel releases the flock immediately."""
    os.close(fd)  # flock(2): "The lock is released when ... the file descriptor is closed"
```

### Cleanup
Lock files are ~0-byte markers. They persist across restarts. `hermes admitslots reset` recreates them. `hermes admitslots prune` removes lock directories for endpoints no longer in config. Stale `running` SQLite rows are cleaned on next acquisition (atomically marked `abandoned`).

---

## Appendix B: PID Reuse Detection Algorithm

```python
import os, time

def _read_boot_id() -> str:
    try:
        with open("/proc/sys/kernel/random/boot_id") as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""

def _process_start_ms(pid: int) -> int | None:
    """Read /proc/PID/stat field 22 (starttime in jiffies), convert to ms."""
    try:
        stat = open(f"/proc/{pid}/stat").read()
        starttime_jiffies = int(stat.split()[21])
        clock_ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
        return int((starttime_jiffies / clock_ticks) * 1000)
    except (IndexError, ValueError, OSError, FileNotFoundError):
        return None

def _is_holder_dead(owner_pid: int | None, owner_start_ms: int | None,
                     boot_id: str | None, current_boot_id: str) -> bool:
    """Return True if the 'running' row's holder has definitively died."""
    if owner_pid is None:
        return True
    
    # 1. Boot ID mismatch → process from previous boot
    if current_boot_id != (boot_id or ""):
        return True
    
    # 2. PID doesn't exist → process died
    if not os.path.exists(f"/proc/{owner_pid}"):
        return True
    
    # 3. PID exists but start time differs → PID reused
    if owner_start_ms is not None:
        actual_start = _process_start_ms(owner_pid)
        if actual_start is not None and actual_start != owner_start_ms:
            return True
    
    # 4. Can't read /proc (unlikely but possible) → assume dead to be safe
    return False
```

---

## Appendix C: Cancellation Contract

```
While queued:
  UPDATE requests SET cancel_requested=1 WHERE id=?
  [Wait loop polls this flag and exits if set]

Between lock grant and commit (within transaction):
  SELECT cancel_requested FROM requests WHERE id=?
  IF cancel_requested = 1:
    CLOSE fd (release flock)
    UPDATE state='cancelled'
    COMMIT
    EMIT admission.cancelled
    RETURN None

After commit (defence in depth):
  SELECT cancel_requested FROM requests WHERE id=?
  IF cancel_requested = 1:
    CLOSE fd
    UPDATE state='cancelled'
    EMIT admission.cancelled
    RETURN None

During generation:
  Notify HTTP transport to abort
  ProviderRequestMonitor calls token.release()
  CLOSE fd → kernel releases flock → next waiter woken

Process death:
  Kernel releases flock automatically
  Next admission detects via _is_holder_dead → row abandoned
```

---

## Appendix D: SQLite Contention & Thread Safety

### Busy timeout
`PRAGMA busy_timeout = 250`: If another process holds `BEGIN IMMEDIATE` for longer than 250ms, SQLite returns `SQLITE_BUSY`. The poll loop catches this and retries at the next poll interval. This prevents 5-second blocking (the SQLite default is 5000ms).

### Thread safety per process
Each process uses one dedicated `sqlite3.Connection` with `check_same_thread=False`, protected by a `threading.Lock()` to serialize concurrent admission calls from multiple threads within the same process (gateway's concurrent sessions).

```python
class PerProcessAdmissionController:
    def __init__(self, db_path: str):
        self._conn_lock = threading.Lock()
        self._conn = sqlite3.connect(str(db_path), isolation_level=None)
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA busy_timeout = 250")
    
    def _execute(self, sql: str, params: tuple = ()):
        with self._conn_lock:
            with self._conn:
                return self._conn.execute(sql, params)
```

### Row retention
`DELETE FROM requests WHERE state IN ('done','cancelled','expired','abandoned') AND finished_ms < ? - 604800000` (7 days TTL). Run in `hermes admitslots reset` and periodically in `AdmissionController.__del__`.

---

## Appendix E: Windows Compatibility (deferred)

The initial implementation targets Linux only. Key differences for a future Windows port:

| Linux feature | Windows equivalent |
|---|---|
| `fcntl.flock(fd, LOCK_EX|LOCK_NB)` | `msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)` |
| `O_CLOEXEC` | `SetHandleInformation(h, HANDLE_FLAG_INHERIT, 0)` |
| `/proc/PID/stat` field 22 | `psutil.Process(pid).create_time()` |
| `/proc/sys/kernel/random/boot_id` | `os.popen('systeminfo | find "Boot Time"').read().strip()` or `ctypes.windll.kernel32.GetTickCount64()` |

Tracked as deferred — not blocking initial implementation.

---

## Appendix F: Rogue Process Bypass

`flock` is advisory — a process that doesn't go through the admission controller could hit the endpoint directly. Mitigations:

1. All provider calls route through `AIAgent._make_api_request()` → all go through `AdmissionClient`. There is no bypass path in normal operation.
2. The `--parallel 1` server-side setting limits concurrency even if bypassed.
3. Provider-stall recovery (exit 74) handles the overflow consequences.
4. No network-level enforcement (firewall) is specified — the design trusts the Hermes process to use its own admission layer.
