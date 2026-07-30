# Admission Controller Spec Review

**Reviewer:** Claude (Hermes Subagent)
**Date:** 2026-07-29
**Spec:** `docs/design/admission-controller.md` @ `13e4e0b5e`

---

## 1. Critical Race Conditions & Crash-Safety Gaps

### 1.1 The "Next Waiter Reclaims" Crash Gap (CRITICAL)

**Problem:** The spec's safety invariant claims: *"Crash after LOCK_EX but before SQLite commit → Kernel releases flock. Row still queued — next waiter ignores it as lock-free."*

This **does not work** as described. The acquire sequence (step 3) is:

```
3(c) Select next eligible request WHERE state='queued' AND endpoint_hash=X ORDER BY lane_priority, seq
3(d) Only if it is THIS request: Attempt LOCK_EX|LOCK_NB on lock file
```

If process A crashes after `LOCK_EX` but before the SQLite commit, process A's row is still `state='queued'`. Process B polls, selects the **next** eligible request, which is still process A's row (seq is earlier). Since it's NOT process B's request, step 3(d) is skipped entirely — process B can **never** reclaim the lock for process A's row. The row sits `queued` until `admission_timeout` (default 30s) expires it, **blocking all subsequent requests** behind it.

**Fix:** Add a step 3(d-ii): If the selected row is NOT "this request" AND is `state='queued'`, check whether its lock is actually held (LOCK_NB test). If the lock is free, either (a) mark that stale row `abandoned` and re-select, or (b) acquire the lock on behalf of that row (adopt the slot), then mark the row `running` and own it.

**Severity:** High. A 1ms window crash blocks all traffic for up to `admission_timeout`.

---

### 1.2 The Cancel-vs-Grant Race (CRITICAL)

**Problem:** The cross-process cancel path (`UPDATE requests SET state='cancelled' WHERE id=? AND state='queued'`) can be silently swallowed by the grant transaction:

1. Process A's row is `queued`.
2. Process A begins `BEGIN IMMEDIATE` transaction (step 3).
3. External cancel (`hermes admitslots flush`) runs `UPDATE ... WHERE id=? AND state='queued'`. This **blocks** on Process A's IMMEDIATE transaction.
4. Inside the transaction, Process A selects itself as next, acquires the flock, and marks itself `running`. Commits.
5. The blocked cancel UPDATE now executes — but the row is `running`, not `queued`, so `WHERE state='queued'` fails silently. **Cancel is lost.**

Process A then starts generation with a `running` row that was supposed to be cancelled.

**Fix:** Either:
- (a) The cancel UPDATE should target `id=?` without `AND state='queued'`, and the grantor must check a "cancel flag" table or an in-memory flag before beginning generation.
- (b) The cancel should set a separate `cancel_requested` column that the grantor checks in step 4 before starting generation.
- (c) Use `AND state IN ('queued', 'running')` for the cancel UPDATE, and the grantor checks post-commit whether the cancel won the race via `SELECT ... WHERE cancel_reason IS NOT NULL`.

**Severity:** High. A race window exists for every cancel that arrives during the 1ms grant transaction.

---

### 1.3 seq Generation is Underspecified (HIGH)

**Problem:** The `seq INTEGER UNIQUE` column has no specified generation mechanism. Concurrent INSERTs (step 1 happens outside any transaction per the diagram) will race on `SELECT COALESCE(MAX(seq),0) + 1`, producing `UNIQUE constraint failed` errors.

**Fix:** Use the auto-incrementing `ROWID` directly for ordering (it's monotonic and safe under concurrent INSERTs in SQLite WAL mode). Or use `(epoch_ms << 16) | (counter & 0xFFFF)` with a per-pid counter. The spec must specify the mechanism.

**Severity:** High. Any concurrent enqueue (6 contexts + parallel subagents) will hit this on first use.

---

### 1.4 Boot ID / Stale Row Reclamation is Not Described in the Acquire Sequence

**Problem:** The schema has `boot_id TEXT` and the chaos plan says *"System reboot → Boot ID mismatch cleans stale rows on next admission."* But the acquire sequence (step 4a) says *"Atomically mark stale 'running' rows 'abandoned'"* without explaining **what metric determines staleness**. Is it:
- `boot_id` mismatch? If so, where is `boot_id` checked?
- `owner_pid` not alive? If so, where is the `procfs` check?
- `owner_start_ms` too old?

The `owner_start_ms` field description says *"boot-time monotonic clock when PID started"* — but `time.monotonic()` on Linux returns time since **arbitrary reference point**, not since boot. `time.clock_gettime(CLOCK_BOOTTIME)` returns time since boot but is not `time.monotonic()`.

**Fix:** Define the exact reclamation algorithm:
1. Check `boot_id`: if `row.boot_id != read_current_boot_id()`, the row is from a previous boot epoch → mark `abandoned`.
2. THEN check `owner_pid`: if `/proc/{owner_pid}` doesn't exist or `psutil.Process(owner_pid).create_time()` doesn't match `owner_start_ms`, mark `abandoned`.
3. Only then attempt to claim the lock.

**Severity:** Medium. Without this spec, the "cleanup on next admission" clause is undefined and likely buggy in implementation.

---

## 2. Design Errors and Logical Flaws

### 2.1 Dynamic Priority Promotion is Not Implementable with Static ORDER BY

**Problem:** Section 5 says *"Any request whose queue wait exceeds max_queue_age (default 120s) is promoted to the front of the interactive lane regardless of original lane."* But the ORDER BY clause in step 3(c) is `ORDER BY lane_priority, seq` — a static comparator. A background request that ages past `max_queue_age` should jump the queue, but this isn't expressible in the static ORDER BY without a computed column.

**Fix:** The SELECT must use a dynamic computation:
```sql
SELECT *, 
  CASE WHEN ? - enqueued_ms >= ? THEN 0 ELSE 1 END AS lane_precedence,
  CASE WHEN ? - enqueued_ms >= ? THEN enqueued_ms ELSE seq END AS sort_key
FROM requests
WHERE state='queued' AND endpoint_hash=?
ORDER BY lane_precedence ASC, sort_key ASC
```

This must be integrated into the sequence description.

---

### 2.2 `consecutive_interactive` Semantic Mismatch

**Problem:** The fairness policy resets `consecutive_interactive` to 0 on any background grant. But the counter tracks **grants**, not **completions**. With `max_slots=1` this is equivalent, but the semantic mismatch is confusing and will break if `max_slots > 1` is ever added (see §5.1). Consider: 3 interactive grants happen, then a 4th interactive arrives while 2 of the first 3 are still generating. The counter shows 3 → background forced even though no background is queued — harmless but misleading.

**Fix:** Either rename to `consecutive_interactive_grants` to clarify semantics, or document the "grants, not completions" behavior explicitly.

---

### 2.3 `is_interactive` Propagation Is Undefined for All 6 Contexts

**Problem:** Appendix B says *"The `is_interactive` flag is propagated from the session context"* but the spec doesn't define how each of the 6 contexts sets this flag. The gateway has multiple session types (API server vs chat vs webhook), CLI has interactive vs `-q`, kanban is always background, delegation is always background, cron is always background, but **auxiliary** tasks (titles, compression, MOA) can be triggered by both interactive and background sessions — the `is_interactive` flag would come from the **triggering agent**, not the auxiliary task itself.

**Fix:** Document the propagation path for each context. For auxiliary: inherit the `is_interactive` flag from the calling AIAgent's session context. For API-server gateway sessions: the `is_interactive` flag must be set based on the API caller's identity (human vs automated).

---

### 2.4 `max_slots > 1` is Unimplementable with the Current Design

**Problem:** The config shows `max_slots: 1` but the architecture locks a single file per endpoint. For `max_slots > 1`, the design needs either:
- N parallel lock files per endpoint (complex cleanup, directory listing for "any free slot")
- A counting approach (but `flock` is binary, not a semaphore)
- `flock` on N lock files with `flock(fd, LOCK_EX | LOCK_NB)` on each until one succeeds

None of these are mentioned. If `max_slots > 1` is ever needed, the entire flock-based capacity token breaks down.

**Fix:** Either remove `max_slots` from the config (hardcode 1), or document that it's reserved for future use and the current design only supports 1.

---

## 3. SQLite-Specific Issues

### 3.1 `BEGIN IMMEDIATE` + `busy_timeout = 5000` Causes 5s Blocking

**Problem:** With `PRAGMA busy_timeout = 5000`, a `BEGIN IMMEDIATE` will block for up to 5 seconds waiting for an ongoing write transaction to finish. Combined with a 250ms poll interval, a writer holding the IMMEDIATE lock for just 2ms (step 3 is "~1ms") can cause a simultaneous `BEGIN IMMEDIATE` from another process to block for up to 5 seconds — but the spec assumes it will fail fast and retry.

**Fix:** Use `PRAGMA busy_timeout = 0` (fail immediately on locked DB) and handle `SQLITE_BUSY` in the retry loop. Or set `busy_timeout` to match the poll interval (250ms).

---

### 3.2 SQLite Connection Thread Safety Not Addressed

**Problem:** The 6 Hermes contexts use different threading models:
- Gateway: `asyncio.to_thread` → worker threads
- CLI: synchronous main thread
- Kanban: `subprocess.Popen` → independent process
- Cron: `ThreadPoolExecutor` in gateway

Each process needs a SQLite connection, but threads within the same process sharing a connection without `check_same_thread=False` (and the associated risks) is not addressed. The `AdmissionController` must be thread-safe per-process while also being safe for multi-process access.

**Fix:** Document that each process uses one dedicated `sqlite3.Connection` with `check_same_thread=False` and its own internal `threading.Lock()` to serialize access from multiple threads within the same process (e.g., gateway's concurrent sessions).

---

### 3.3 Row Retention / Table Growth is Unbounded

**Problem:** Rows are INSERTed and updated to `done`/`expired`/`cancelled`/`abandoned` but never DELETEd. Over months of operation, the `requests` table grows without bound, degrading query performance.

**Fix:** Add a retention policy: `DELETE FROM requests WHERE state IN ('done', 'cancelled', 'expired', 'abandoned') AND finished_ms < ? - TTL_IN_MS` (e.g., 7 days). Run as part of `hermes admitslots reset` or periodically.

---

## 4. Integration Pitfalls

### 4.1 The Transport Base Class Is Not the Right Abstraction

**Problem:** Section 7 adds `admit()`/`release()` to the `ProviderTransport` base class. But `ProviderTransport` is a **message format converter** (see `base.py` L1-9: "A transport owns the data path for one api_mode"). It has no concept of HTTP clients, connection lifecycle, or network calls. Adding admission hooks here pollutes a pure formatting abstraction with lifecycle management. Every transport (ChatCompletions, Bedrock, Anthropic, Codex) would inherit no-op admit/release whether or not it makes HTTP calls.

**Fix:** Place admission at the **client call site** in `AIAgent` or in a **`AdmissionClient` wrapper** around the HTTP client, not on the transport. The transport is corrected by `convert_messages` → `build_kwargs` → `normalize_response` — it doesn't run the HTTP call.

---

### 4.2 Gateway's `_run_agent` Uses `asyncio.to_thread` — Cancel Event Bridging

**Problem:** The gateway uses `asyncio.to_thread()` to run AIAgent (synchronous, blocking) in a thread pool. Thread cancellation in Python requires the thread to cooperatively check a flag — `threading.Event` works, but bridging from `asyncio.Task.cancel()` to `threading.Event.set()` is non-trivial and must be handled per-gateway-session.

**Fix:** Document the `asyncio → threading.Event` bridging pattern. The gateway's `_run_agent_turn` must install a `CancellationError` handler that calls `admission_controller.cancel(token_id)` and `cancel_event.set()`.

---

### 4.3 Kanban Worker Pre-Flight Admission

**Problem:** Section 7 (Tertiary) says *"Add a pre-flight admission check before subprocess.Popen."* But kanban workers are **child processes** that don't share the parent's admission state. The pre-flight check would admit the parent (the dispatcher) for the child's slot. But if the child crashes before starting generation, the slot is held until `admission_timeout` or process death — but the parent (dispatcher) is the one holding the lock, not the child. The child inherits `O_CLOEXEC` file descriptors (the spec says `O_CLOEXEC` prevents this!), so the lock doesn't pass to the child.

This means the parent holds the slot while the child boots up, loads config, and begins its turn — the slot is consumed by the boot process (potentially seconds of overhead). This is wasteful at best and introduces deadlock scenarios if the child needs to re-enter admission for auxiliary tasks.

**Fix:** Either (a) have the child process do its own admission after boot, or (b) have the parent pass the open lock fd to the child (carefully coordinating `O_CLOEXEC` handling) so the child "owns" the slot from the start. The spec must choose and document the trade-offs.

---

### 4.4 Auxiliary Tasks Trigger Admission Recursively

**Problem:** Auxiliary tasks (titles, compression, MOA) are launched during an interactive session's turn. If the auxiliary task also goes through admission, it will queue behind the interactive session that triggered it. If the queue is FIFO + fairness, and the interactive session holds the slot, the auxiliary task will wait until the interactive session finishes — but the interactive session can't finish until the auxiliary task completes. **Deadlock.**

**Fix:** Auxiliary tasks must either (a) bypass admission entirely (inherit the parent's slot), (b) use a separate endpoint/admission gate, or (c) use `admission_timeout=0` (fail-fast if slot not immediately free) and retry via the existing fallback chain. The spec must address this.

---

### 4.5 Two Normalization Functions May Produce Inconsistent Hashes

**Problem:** The spec says *"Existing normalization: `agent/backend_identity.py::normalize_base_url()` and `hermes_cli/route_identity.py::normalize_route_base_url()`."* But:
1. `backend_identity.py` has `_norm_base_url()` (private), not `normalize_base_url()` (public). The function referenced doesn't exist.
2. `route_identity.py` has `normalize_route_base_url()` which strips trailing slashes and lowercases the host.
3. These two functions might produce **different outputs** for the same URL, leading to different endpoint hashes from different call sites.

Any single admission DB with hashes from mismatched normalization will have separate queues for the same endpoint.

**Fix:** Mandate a **single** normalization function (e.g., `route_identity.normalize_route_base_url`), called once at config load time, and stored in the `BackendIdentity` dataclass or the config entry. The admission controller must use the pre-computed hash from the config entry, never re-normalize.

---

## 5. Fairness and Priority Gaps

### 5.1 Background Starvation with Long-Lived Completions

**Problem:** The fairness rule forces a background request after 3 consecutive interactive grants. But if each interactive generation takes 120 seconds (a long thinking/Codex model), the background request waits **up to 360 seconds** before being forced. The `max_queue_age` (default 120s) helps — the background request is promoted to the interactive lane after 120s of queue wait, so worst case is 120s + one interactive turn. But this assumes `max_queue_age < 3 × interactive_generation_time`, which may not hold.

**Fix:** Either (a) lower the default `max_queue_age` to match typical interactive generation time + buffer, or (b) document that `max_queue_age` should be tuned based on observed generation times.

---

### 5.2 `is_interactive` Flag Not Serializable Across Process Boundaries

**Problem:** Kanban workers are `subprocess.Popen` — a new Python process. The `is_interactive` flag is a Python runtime value. How does a kanban worker know it should be `background`? It's specified in Appendix B as always background for kanban, but the source code for lane classification doesn't use any persistent state — it's a `classify_lane(source, is_interactive)` Python function call. Each context must correctly pass `source='kanban'` and `is_interactive=False`.

**Fix:** The `source` field should be the authoritative determinant, not `is_interactive`. Or at minimum, document that every entry point into the admission controller must specify both `source` and `is_interactive` independently.

---

## 6. Spec-Level Inconsistencies

### 6.1 Lock File Lifetime vs. Lock File Acquisition Race

**Problem:** Section 3 says *"Lock file is created on first use (`open(path, 'w').close()` inside the lock acquisition)"* and Appendix A says the lock file is `touch()`ed. But two processes doing this concurrently could race on file creation. On Linux, `touch(mode=0o644, exist_ok=True)` is race-free for the same path, but `open(path, 'w').close()` truncates an existing file, which is a **different** behavior (and worse — it can clear the file between someone's open and flock call).

**Fix:** Standardize on `Path.touch(exist_ok=True)` as shown in Appendix A, not `open(path, 'w').close()`. The spec text and appendix contradict each other.

---

### 6.2 `scheduler_state` Table Has `consecutive_interactive` But No Other State

**Problem:** The `scheduler_state` table has a single column `consecutive_interactive`. But a full implementation would need:
- `current_queue_front_seq` — to avoid re-scanning from the beginning every poll
- `total_interactive_served` / `total_background_served` — for fairness metrics
- `last_grant_timestamp_ms` — for debugging

These aren't specified and the schema looks like it was stubbed.

---

### 6.3 Rollout Phase 2 Moots the Fairness Policy

**Problem:** Phase 2 gates background callers only. Interactive sessions bypass the queue entirely. This means:
- The `consecutive_interactive` counter is never incremented (interactive never enters the queue).
- Background requests enter the queue but are never forced to the front (counter stays at 0).
- The fairness mechanism doesn't work during Phase 2.

This is acceptable as a transitional state but should be explicitly noted in the spec so implementers don't debug a "non-working" fairness algorithm.

---

## 7. Testing and Rollout Oversights

### 7.1 No SQLite Corruption Test

**Problem:** The chaos tests include *"SQLite database deleted mid-operation"* but not *"SQLite database corrupt mid-operation"* (e.g., a torn write from a crash). SQLite WAL mode is crash-safe for normal operations, but file deletion/corruption during a write transaction can leave the DB in a state where `BEGIN IMMEDIATE` raises `DatabaseError`. The degrade-to-flock path must handle this gracefully.

### 7.2 No Performance Benchmark

**Problem:** The polling overhead estimate ("40 writes/second" for 10 processes at 250ms intervals) is a crude theoretical bound. There's no benchmark to validate that the poll loop doesn't cause measurable CPU or SQLite WAL file growth. Phase 1 should include a performance benchmark as a deliverable.

### 7.3 No Windows Support Consideration

**Problem:** The entire design is Unix-centric (`fcntl.flock`, `O_CLOEXEC`, `/proc/sys/kernel/random/boot_id`). The cron scheduler already has fallback `msvcrt.locking` for Windows compatibility. The admission controller should specify an equivalent Windows mechanism:
- `msvcrt.locking` instead of `fcntl.flock`
- Windows boot time via `os.popen('systeminfo')` or `ctypes.windll.kernel32.GetTickCount64`
- `CREATE_FILE` with `FILE_FLAG_DELETE_ON_CLOSE` equivalences

---

## 8. Answer: Simpler Alternatives Analysis

### (a) Plain flock with filesystem-based queue using atomic renames

**Verdict: Not simpler — trades SQLite complexity for filesystem correctness complexity.**

A filesystem-based queue uses `rename(2)` (atomic on the same filesystem) for ordering. Each waiter creates a timestamped `<seq>-<pid>-<uuid>.wait` file in a directory. The holder atomically renames the "next" wait file to `.active` and takes `flock` on it.

**Problems:**
- `rename(2)` is atomic only on the **same filesystem**. Temporary /tmp vs ~/.hermes across filesystems breaks atomicity.
- Directory listing is **not ACID** — `listdir` races with concurrent `rename`s from other waiters.
- Implementing priority (interactive → background → age-promotion) requires reading all `.wait` files, sorting in Python, and handling the TOCTOU race between read-and-sort vs claim.
- No built-in query mechanism for observability (`hermes admitslots status` requires iterating the directory).
- Crash recovery requires scanning all `.wait` files for orphaned PIDs (procfs check).
- Cross-platform issues: `rename` atomicity on Windows differs from POSIX.

**Compared to SQLite:** SQLite's `BEGIN IMMEDIATE` + WAL mode provides ACID semantics for exactly this use case. The filesystem-based approach re-implements ACID badly.

### (b) Just relying on llama.cpp's own internal queue

**Verdict: Insufficient. Rejected correctly by the spec.**

`llama.cpp --parallel 1` internally queues requests, but:
- Queue wait **consumes the provider attempt timeout** (the HTTP request is already in-flight while queued at the server).
- No priority lanes (interactive vs background).
- No cancellation mechanism — you can't cancel a queued request at the server without closing the HTTP connection.
- No observability — no visibility into queue depth, wait times, or caller identity.
- **No cross-process integration** with Hermes' stall recovery chain.
- A stuck generation at the server blocks the queue until the HTTP timeout kills it.

### (c) Single-process broker pattern

**Verdict: Superior semantics but larger deployment scope. Worth consideration for Phase 5.**

A Unix-domain socket broker (or an in-process admission thread in the gateway) that all 6 Hermes contexts connect to. The broker manages a priority queue in memory with `threading.Condition` for instant wakeup.

**Advantages over the hybrid design:**
- **Zero polling** — condition variable wakes the next waiter instantly when a slot frees.
- **Full control** over scheduling (fairness, promotion, cancellation).
- **No SQLite contention** — metadata is in-process memory.
- **No crash gap** — broker detects client disconnection via socket close and immediately releases the slot.
- **Atomic** cancel/grant — no race between SQLite transaction and cancel UPDATE.
- **Observability** is trivial (broker exposes a status endpoint on the same socket).

**Disadvantages acknowledged by the spec:**
- Broker is a supervised daemon (systemd unit or gateway-embedded thread).
- If the broker dies, all admission stops (but the spec's degrade-to-flock path covers this).
- IPC protocol over Unix sockets adds rollout complexity.
- The broker introduces its own crash-safety concerns (process death recovery).

**Verdict on complexity:** The polling approach trades ~50 LOC of socket protocol for the entire SQLite DB schema, connection management, transaction retry logic, migration system, and row retention. The broker is **simpler at runtime** but **harder to deploy**. For a user with only a local gateway process, the broker-as-gateway-thread is actually the simpler option.

### (d) Pure SQLite only (no flock)

**Verdict: Not safe. Rejected correctly by the spec.**

Use only SQLite row-level state (`state='running'`), no `flock`. Crash recovery relies on PID/start-time matching.

**Problem:** After `state='running'` is committed, if the process crashes, the next waiter must detect the crash via PID check. But between the commit and the next waiter's PID check, another process that doesn't check PID could observe `state='running'` and assume the slot is busy, while in reality it's free. The `flock` is the authoritative gating mechanism because the kernel guarantees release-on-death.

Without `flock`, you get **lease-based** coordination (as the spec correctly notes in §2 "Why not alternatives") which has the split-brain problem.

### (e) Recommended simplification of the hybrid design

Rather than a different architecture, I recommend these simplifications to the existing design:

1. **Eliminate the separate lock file.** Use `flock(fd, LOCK_EX)` on the **SQLite .db-wal file** as the capacity token, not a separate `.lock` file. The WAL file exists as long as the DB is open in WAL mode. When the holder crashes, the kernel releases the flock on the WAL file, and the next acquirer (which must hold the DB open too) can claim it. **Caveat:** This means the flock is held while the DB is open (not just during generation), so you need to use a **dedicated connection** for the lock, opened once at startup and kept alive for the process lifetime. This conflates DB access with slot holding and makes the "degrade to flock only" path impossible.

2. **Accept the trade-offs and simplify the queue model.** Remove age-based promotion and `max_queue_age` (adds complexity for a corner case). Keep only FIFO + `consecutive_interactive` fairness.

3. **Use `ROWID` directly instead of `seq`.** Eliminate the `seq` column entirely. The ROWID in SQLite with AUTOINCREMENT is monotonic and race-free.

4. **Remove `owner_start_ms`. Use only `boot_id` + `owner_pid`.** Check `/proc/{pid}` existence (Linux) or `psutil.pid_exists()`. `owner_start_ms` adds complexity and hasn't been proven necessary.

5. **Remove the `scheduler_state` table.** Keep `consecutive_interactive` as a column in the main `requests` table or compute it from the last N grants in the `requests` table itself (`SELECT COUNT(*) FROM requests WHERE endpoint_hash=? AND state IN ('running','done') AND lane='interactive' ORDER BY seq DESC LIMIT 3 HAVING ...`). This eliminates a table and a JOIN.

---

## 9. Miscellaneous Issues

| # | Issue | Detail |
|---|-------|--------|
| 9.1 | SHA256[:16] collision domain | 16 hex chars = 64 bits. Birthday bound is ~4B endpoints. Fine in practice but use the full hash (64 chars) for zero collisions. |
| 9.2 | Stray lock files never cleaned | Lock files for removed endpoints persist at `~/.hermes/admitslots/` forever. Add `hermes admitslots prune` or auto-clean on config reload. |
| 9.3 | `admission_timeout: 0` undefined | Is it "never wait" (fail-fast) or "wait forever"? Must be documented. |
| 9.4 | `int` vs `float` for time values | Config shows `admission_timeout: 30.0` (float), but SQLite columns are `INTEGER` (ms). The conversion from float seconds to ms is implicit and could round incorrectly. |
| 9.5 | Missing `hermes admitslots` tab-completion | New CLI subcommand needs shell completion registration. |
| 9.6 | No `lseeks`/`connection_check` | The spec should note that `fcntl.flock(fd, LOCK_EX | LOCK_NB)` on a freshly opened fd works even if the underlying file was deleted (the fd holds a reference). The spec implies this but doesn't state it explicitly. |
| 9.7 | Advisory vs mandatory locking | `flock` is advisory — a rogue process that doesn't use the admission controller could bypass the lock and hit the endpoint directly. The spec assumes all provider calls go through admission, but there's no enforcement mechanism. |
