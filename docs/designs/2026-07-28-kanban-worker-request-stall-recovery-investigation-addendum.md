---
proposal_id: prop:af7347ffd8d22853efd59922e4fda7a85aed7058
spec_hash: b8457a750e82148eeb1e5bbb0974ea62b22fc292353237672a25025574fb013e
repo_name: hermes-agent
feature_slug: kanban-worker-request-stall-recovery-investigation
status: approved_linked_investigation_addendum
amends_proposal_id: prop:756ea35adb0982bbb5d028b24d53be31e1e8d9fa
consumed_by_kanban_task_id: t_c2657ee3
---

# Investigation Addendum: Cumulative Stream-Recovery Deadline

**Date:** 2026-07-28  
**Project:** `hermes-agent`  
**Queued runtime specification:** `docs/designs/2026-07-28-kanban-worker-request-stall-recovery.md`  
**Implementation task:** `t_c2657ee3`  
**Purpose:** incorporate the completed source investigation without changing the queued proposal identity or creating a second runtime task

## 1. Corrected incident interpretation

The observed `preparing write_file` line does not show that `write_file` executed.

The line is emitted while Hermes is still assembling streamed tool-call arguments:

1. `agent/chat_completion_helpers.py::_call_chat_completions()` receives the tool name and calls `agent._fire_tool_gen_started(name)`.
2. `run_agent.py::AIAgent._fire_tool_gen_started()` invokes the display callback.
3. `cli.py` prints `preparing {tool_name}`.
4. Actual tool validation and dispatch occur later in `agent/conversation_loop.py` and `AIAgent._execute_tool_calls()`.

The absence of file or Git changes is consistent with the tool never reaching dispatch.

## 2. Verified truncation behavior to preserve

Current Hermes already handles explicit output-length truncation conservatively:

- `agent/conversation_loop.py::run_conversation()` recognizes `finish_reason="length"` with an incomplete tool call.
- It retries with a larger temporary output allowance while `truncated_tool_call_retries < 4`.
- It does not append or execute the incomplete tool call.
- After retry exhaustion, it returns a partial error and refuses execution.
- `agent/chat_completion_helpers.py::build_api_kwargs()` applies the temporary output allowance on the retry.

The runtime implementation must preserve this behavior. It must not repair or execute incomplete tool arguments and must not broaden retry counts or spend.

## 3. Verified liveness gap

`agent/chat_completion_helpers.py::interruptible_streaming_api_call()` has a no-progress/stale detector:

- real chunks update `last_chunk_time`;
- cloud stale timeout defaults are finite and may scale for large or reasoning contexts;
- the poll loop interrupts a stream only after no real chunks arrive for the stale interval.

It does not provide an absolute monotonic ceiling for the complete streaming/recovery episode. A provider can therefore evade the stale detector by emitting tiny argument fragments often enough to refresh `last_chunk_time`.

`httpx.Timeout` is not sufficient because its connect/read/write/pool values are operation-level timeouts, not a cumulative response deadline.

The bounded conversation retry count also does not bound elapsed time when an individual retry can remain active indefinitely.

## 4. Required deadline hierarchy

The queued runtime specification’s deadline contract is refined to require all three levels:

1. **No-progress deadline** — bounds elapsed time since the last real provider byte/chunk/event advance.
2. **Absolute attempt deadline** — bounds one provider attempt even when chunks continue arriving.
3. **Cumulative recovery-episode deadline** — bounds the complete truncation/tool-call recovery episode, including internal stream retries, reconnects, and boosted output-length attempts.

Requirements:

- Chunk arrival, wait notices, automatic activity heartbeats, reconnects, and retry transitions must not extend or reset the cumulative deadline.
- Use monotonic time.
- The episode deadline may be inherited from an existing outer request/turn deadline where that contract is already enforceable; otherwise introduce the smallest shared supervisor required.
- Per-task `max_runtime_seconds` remains a separate operator-level backstop and is not the primary provider-stream fix.
- On deadline expiry, use existing transport-specific abort/poison/owner-thread cleanup paths.
- Return a structured partial failure with no successful tool result and no mutating side effect.
- Do not restore the broader reverted session-stall feature merely to obtain this deadline.

## 5. Heartbeat interaction

`run_agent.py::AIAgent._touch_activity()` bridges activity into `tools/kanban_tools.py::heartbeat_current_worker_from_env()` approximately once per minute.

The streaming poll loop can call wait/activity hooks even without useful provider progress. Consequently:

- fresh heartbeats prove process/request liveness only;
- they must not extend provider request or recovery deadlines;
- they must not count as productive progress in blocker-monitor policy;
- changing global heartbeat freshness semantics is not the primary runtime fix.

## 6. Mandatory regression tests

### 6.1 Infinite trickle stream

Add the primary low-level regression beside the stream watchdog tests, preferably in:

- `tests/run_agent/test_stream_stale_circuit_breaker.py`; or
- the adjacent watchdog section of `tests/run_agent/test_streaming.py`.

Create a fake stream that yields tiny chunks faster than the no-progress timeout but exceeds a small cumulative deadline.

Assert:

- the call terminates within a bounded test duration;
- no-progress timeout does not fire first;
- the stream/request close or abort path is attempted;
- no tool result is produced;
- automatic activity/heartbeat callbacks do not extend the cumulative deadline.

### 6.2 Truncated-tool recovery episode

Extend the existing truncation tests in:

- `tests/run_agent/test_run_agent.py`;
- `tests/run_agent/test_partial_stream_finish_reason.py`; and, where transport-specific, 
- `tests/run_agent/test_anthropic_truncation_continuation.py`.

Sequence:

1. explicit `finish_reason="length"` with incomplete mutating-tool arguments;
2. boosted recovery request begins;
3. retry emits advancing trickle chunks indefinitely;
4. cumulative deadline expires.

Assert:

- incomplete tool arguments never dispatch;
- retry/output-cap behavior before expiry remains unchanged;
- the recovery episode returns structured partial failure;
- cleanup wins exactly once;
- no capacity-holding worker thread remains.

### 6.3 Kanban contract

Extend Kanban integration coverage to prove:

- automatic heartbeats remain fresh during the simulated stream;
- those heartbeats do not extend the runtime deadline;
- the worker exits through existing retry/block semantics rather than remaining `running`;
- task-level `max_runtime_seconds` remains an independent final backstop.

## 7. Current-main gate

Run these regressions against the latest fetched `origin/main` before implementation.

- If all contracts already pass, record the exact implementing commit and avoid duplicate runtime code.
- If the trickle or cumulative-recovery test fails, implement the smallest shared deadline correction.
- Preserve existing stream-stale, truncation classification, abort-race, prompt-cache, role-alternation, and provider-specific tests.

## 8. Acceptance additions

The queued runtime task is complete only when:

1. an active trickle stream cannot run indefinitely;
2. the cumulative recovery episode is bounded independently of chunk activity and automatic heartbeats;
3. existing incomplete-tool safety and output-length retry behavior remain intact;
4. timeout cleanup leaves no capacity-holding thread, socket/client reuse hazard, or claimed worker;
5. current `origin/main` is tested before code is written;
6. no second runtime task, monitor, watchdog, or cron job is introduced.
