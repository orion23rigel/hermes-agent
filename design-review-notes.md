# Independent Design Review — Simplicity Assessment & Gap Analysis

## Is there a simpler approach?

I considered four alternatives:

### (a) Filesystem queue + plain flock (no SQLite)
Queue files named `{seq:05d}-{lane}-{uuid}.req` in a directory per endpoint. Atomically create via tempfile+rename. FIFO by sorted filename order. Grant = oldest file owner tries `LOCK_EX|LOCK_NB`.

**Loses vs SQLite:**
- No crash-consistent state machine: "running" vs "abandoned" must be inferred from PID liveness checks
- No structured `consecutive_interactive` counter storage → starvation guard is harder
- Aged-background promotion requires renaming files → race-prone with concurrent listers
- Observability requires parsing filenames + stat() instead of a simple SQL query
- Stale cleanup sweep needed (orphaned `.req` files from crashed processes)

**Verdict:** SQLite's incremental cost is near-zero (it's already a stdlib dependency, already used by Hermes for sessions/kanban). The filesystem-only approach would re-invent ACID on top of the filesystem — not simpler in practice.

### (b) Rely on llama.cpp internal queue
Set `--parallel 2` and let the server queue internally. 503 retry with backoff. Existing exit-74 chain handles stalls.

**Fatal flaw:** Hermes' 6 contexts send concurrent requests. llama.cpp accepts them all on its HTTP server, queues N-1 internally, but each request's HTTP connection stays open consuming the Hermes **provider_attempt_timeout**. By the time the 5th-in-line gets processed, its timeout has expired. Exit 74 is reactive, not preventive — it should be the safety net, not the primary strategy.

### (c) Unix-domain broker daemon
A small process that owns the connection to llama.cpp. Others multiplex through it via UDS.

**Adds:** supervised daemon, IPC protocol, startup ordering, crash recovery of the broker. Not simpler.

### (d) Single `LOCK_EX` file, no queue at all
Processes race on `LOCK_EX|LOCK_NB`. Losers immediately get `BlockingIOError` and fail fast.

**Loses:** All ordering guarantees. Interactive vs background priority. Predictable latency. Bounded queue time. Observability.

---

**Conclusion:** The hybrid SQLite+flock design is the *minimal correct solution*. SQLite adds no new dependency and solves state tracking, crash recovery, fairness counters, and observability in ~40 lines of schema. Any simpler design loses at least one correctness requirement.

---

## Self-Critical Issues Found

Before Claude's review arrives, I found these gaps in my own spec:

### 1. AUTOINCREMENT needed for seq
The `seq INTEGER UNIQUE` column has a race: two concurrent INSERTs can get the same `seq` value if assigned by the application. Fix: use `INTEGER PRIMARY KEY AUTOINCREMENT` and let SQLite generate it. Or use `(SELECT COALESCE(MAX(seq), 0) + 1 FROM requests)` inside the transaction.

### 2. max_slots > 1 requires N lock files
The spec's single lock file per endpoint only supports `max_slots=1`. For `max_slots=N`, need `slot-0.lock` through `slot-{N-1}.lock`. The acquire tries `LOCK_EX|LOCK_NB` on each in sequence until one succeeds. The current spec is correct for N=1 but incomplete for the config parameter's intent.

### 3. Cancellation + grant race: missing release steps
The cancellation contract says "After grant, before HTTP: check cancel_event. If set → release + done." But release means:
  a) Close flock fd (lock released)
  b) UPDATE state='cancelled'
  c) Remove from scheduler_state tracking
  d) Emit admission.cancelled event
The spec only says "release + done" — too vague for implementation.

### 4. Boot ID → process death on same boot
The `boot_id` field tracks reboots. But a process can crash and restart on the **same boot** with a reused PID. The `owner_start_ms` (monotonic clock) catches this: if `owner_pid` matches but `owner_start_ms` is newer than when the row was created, the original holder crashed. This is Rust/stdlib pattern for PID reuse detection.

### 5. The poll loop can miss cancellations of later-queued requests
The spec says "Expire stale cancelled/expired rows" during step 3's transaction. But what if a request enqueued AFTER us gets cancelled? Its cancellation makes no difference to us — we're ahead in line. However, the scan should only consider `state='queued'` rows when selecting the next eligible — cancelled rows are filtered out.

Actually, this IS correct — the spec says `WHERE state='queued'` in the SELECT. No issue here.

### 6. No health-check poisoning guard
What if the endpoint goes down permanently (llama.cpp crashed)? All queued requests will exhaust `admission_timeout` one by one. This is correct behaviour — they should all fail. But the operator needs to notice this and either restart the endpoint or `hermes admitslots flush` the queue. The spec's drain/flush commands handle this, but there's no automatic circuit breaker after N consecutive admission_timeout failures for the same endpoint. This would be a nice-to-have for Phase 4.

### 7. Missing: what happens to the lock file fd during provider retries?
If the first HTTP attempt fails with a retryable error, should the admission lock be held across retries? Yes — releasing on first failure and re-acquiring for retry would waste queue position. The spec should clarify that the lock is held for the **entire provider_attempt_timeout**, covering all retries within that window.

### 8. Lane classification is too simplistic
`Appendix B` says gateway + interactive CLI are "interactive" and everything else is "background". But a CLI `-q` one-shot that the user is waiting on is effectively interactive. And a gateway conversation that's idle (user walked away) is not truly interactive. The spec should note this is a conservative first cut and can be refined with idle-detection later.
