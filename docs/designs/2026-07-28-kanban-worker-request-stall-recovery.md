---
proposal_id: prop:756ea35adb0982bbb5d028b24d53be31e1e8d9fa
spec_hash: e50f9e709914aad916532811d43f274069c4e2e6c7b9e63652676b384d644541
repo_name: hermes-agent
feature_slug: kanban-worker-request-stall-recovery
status: queued
kanban_task_id: t_c2657ee3
origin_interface: discord
hermes_thread_id: 1531747310298730677
builds_thread_id: 1531747318821421226
depends_on_kanban_task_id: t_84eaea9b
blocks_kanban_task_id: t_a75bfbea
---

# Design: Bounded Provider-Request Recovery for Kanban Workers

**Date:** 2026-07-28  
**Project:** `hermes-agent`  
**Project path:** `/home/orion/.hermes/hermes-agent`  
**Implementation workspace:** isolated worktree based on current `origin/main`  
**Project type:** Python runtime; test-driven bug fix  
**Integration policy:** upstream-quality branch and PR; do not modify the dirty installed checkout directly  
**Plan approval gate:** no; Orion approved this linked runtime work in Discord on 2026-07-28

## 1. Goal

Ensure that a Kanban worker cannot remain alive indefinitely in a provider request after a truncated response or retry. Every provider request must either make observable request progress, complete, fail within a bounded deadline, or terminate cleanly when interrupted. Incomplete tool-call arguments must never execute.

This specification fixes the runtime failure at its source. Productive-progress detection, policy, and conservative reclaim remain in the linked `hermes-orchestration` blocker-monitor addendum; Hermes Agent must expose and enforce the underlying request lifecycle correctly rather than duplicating that monitor.

## 2. Verified incident and refined root cause

Task `t_a75bfbea` ran under the `specifier` profile and produced this sequence:

1. The worker completed multiple file and search tools while preparing an implementation plan.
2. Its log recorded `Response truncated (finish_reason='length') - model hit max output tokens` while preparing `write_file`.
3. The current conversation loop recognized the truncated tool call and began a retry rather than executing incomplete arguments.
4. The subsequent provider API call never produced another model response, tool result, timeout, or structured failure.
5. The worker process stayed alive and emitted automatic Kanban heartbeats approximately every minute.
6. No plan file or Git change was created.
7. More than one hour later, manual reclaim interrupted the worker; its final log line was `Interrupted during API call.`

Therefore, the direct evidence does **not** show that Hermes executed a truncated `write_file`. It shows that existing truncation recovery entered another provider request and that request remained interruptible but unbounded from the board’s point of view.

The failure spans two contracts:

- **Agent/provider request contract:** a request or stream can remain in flight without completing or returning a structured timeout.
- **Kanban worker contract:** automatic heartbeat reports process liveness while providing no evidence that the active provider call is advancing.

The linked blocker-monitor addendum covers the second contract. This design covers the first.

## 3. Current-main prerequisite finding

The installed checkout at `/home/orion/.hermes/hermes-agent` is 55 commits behind its fetched `origin/main` and has unrelated local modifications in `hermes_cli/tools_config.py` and `package-lock.json`. It must not be updated or edited in place.

Current `origin/main` already contains:

- `finish_reason='length'` normalization across chat-completions, Bedrock, and Anthropic modes;
- bounded truncated-tool-call retries;
- refusal to execute incomplete tool arguments after retry exhaustion;
- stream-stale detection infrastructure;
- request-client interrupt/abort and socket cleanup improvements;
- tests for partial streams, interrupted tool calls, and abort races.

Implementation must first reproduce the incident against the current `origin/main` worktree. If current main already satisfies every acceptance criterion, do not duplicate the fix. Record `implemented_on_main`, add only any missing regression/contract test that proves the incident, and route a separately reviewed local upgrade/deployment. If reproduction still fails, implement the smallest root-cause correction on current main.

## 4. Goals

1. Bound provider request stalls across every request path used by Kanban workers.
2. Preserve existing safe truncated-tool-call retry behavior.
3. Ensure a timeout or interrupt actually unwinds the request and worker turn.
4. Return a structured machine-readable outcome that Kanban can route deterministically.
5. Make automatic heartbeat provenance distinct from provider/tool progress.
6. Reuse existing provider timeout configuration and transport abstractions.
7. Preserve prompt caching, message alternation, session integrity, and existing fallback semantics.
8. Add a deterministic regression test matching the observed sequence.

## 5. Non-goals and protected boundaries

1. Do not add another monitor, daemon, cron job, or model tool.
2. Do not put non-secret timeout configuration in `.env` or introduce a new user-facing `HERMES_*` variable.
3. Do not execute or repair incomplete mutating tool-call arguments speculatively.
4. Do not change provider/model assignments or fallback order in this runtime fix.
5. Do not silently increase retry counts or provider spend.
6. Do not mutate prior conversation history in a way that breaks prompt caching or role alternation.
7. Do not edit the dirty installed checkout or overwrite its unrelated local changes.
8. Do not automatically deploy code from an unreviewed branch.
9. Do not let the worker monitor reach into provider internals; request lifecycle belongs in Hermes Agent.
10. Do not claim the existing truncation handler is absent; test and preserve it.

## 6. Required request lifecycle

Every model request used by a Kanban worker has these states:

```text
prepared
  -> connecting
  -> response_started
  -> response_advancing
  -> completed
  -> failed
  -> timed_out
  -> interrupted
```

Terminal states are `completed`, `failed`, `timed_out`, and `interrupted`. A request must enter exactly one terminal state, release owned resources, and notify the conversation loop.

The runtime tracks two distinct deadlines:

1. **Absolute request deadline:** bounds total wall-clock duration for one provider attempt.
2. **No-progress/stale deadline:** bounds time since the last provider response event or byte/chunk advance after the request starts.

The effective values use existing configuration resolution:

1. model-specific `providers.<provider>.models.<model>.request_timeout_seconds` or equivalent existing model override;
2. provider-level `providers.<provider>.request_timeout_seconds`;
3. existing Hermes default appropriate to the transport.

Stale-stream configuration follows the existing provider/model stale-timeout resolver. Explicit user config wins. Reasoning-model floors may increase stale patience where already supported, but they may not create an infinite absolute deadline for Kanban workers.

All request modes used by workers must honor the same contract:

- OpenAI-compatible streaming chat completions;
- non-streaming chat completions;
- native Gemini transport when selected;
- Anthropic messages;
- Bedrock converse/streaming;
- Codex responses;
- fallback-provider requests;
- retries after a truncated or interrupted response.

## 7. Progress and heartbeat provenance

The runtime must not equate its periodic Kanban liveness heartbeat with provider progress.

Expose bounded activity categories through the existing callback/event bridge where available:

```text
request.started
request.first_response
request.progress
request.retry
request.completed
request.failed
request.timed_out
request.interrupted
tool.started
tool.completed
tool.failed
```

Requirements:

1. Events contain task ID, run ID, provider/model identifiers, request-attempt ordinal, monotonic elapsed duration, and a reason code.
2. Events contain no prompt text, response text, credentials, headers, complete tool arguments, or secret-bearing URLs.
3. `request.progress` is rate-limited and emitted only when transport-level evidence advances; it is not a timer tick.
4. Automatic Kanban heartbeat remains a separate `automatic_liveness` source.
5. The blocker monitor may consume these categories, but Hermes Agent does not implement monitor policy.
6. Absence of the optional event sink must not change request behavior.

Prefer extending an existing activity/progress callback or lifecycle hook over adding a new core model tool or speculative plugin hook.

## 8. Truncated and incomplete tool-call handling

Preserve the current invariant:

> A response with `finish_reason='length'` and incomplete or suspect tool-call arguments is never dispatched as a real tool action.

Required behavior:

1. Normalize the partial response using the active transport.
2. Classify text truncation separately from tool-call truncation.
3. For tool-call truncation, retry only within the existing bounded recovery policy.
4. Each retry receives the normal request deadline and stale-progress deadline; retrying must not reset an outer task/run deadline.
5. Do not append an incomplete tool call to durable conversation state as an executable call.
6. After retry exhaustion, close any interrupted tool sequence with a synthetic non-executable error result using existing message-repair conventions.
7. Return a structured partial failure and release request resources.
8. A mutating tool such as `write_file`, `patch`, `terminal`, or `browser_click` must have zero side effects from the incomplete response.

The observed failure occurred during the retry request, so testing only the first truncated response is insufficient.

## 9. Timeout, cancellation, and cleanup

### 9.1 Timeout

When either request deadline expires:

1. classify the result as `provider_request_stalled` with subtype `absolute_timeout` or `no_progress_timeout`;
2. request cancellation through the transport/client’s supported path;
3. close or poison any request client that cannot be safely reused;
4. unwind the worker request thread within a bounded cleanup grace period;
5. emit one terminal lifecycle event;
6. return control to the conversation loop without waiting for the provider indefinitely.

### 9.2 External interrupt

When Kanban reclaim or user interruption occurs:

1. mark the request `interrupted`;
2. abort the exact in-flight client/request;
3. verify the worker thread or task unwinds;
4. prevent the aborted client/socket pool from being reused;
5. close resources on their owning thread where required by the existing client-lifecycle design;
6. emit one terminal event and no later progress events for that request.

### 9.3 Uncooperative transports

If a transport cannot cancel cleanly, the request supervisor must still bound the worker-visible wait. Resource cleanup may complete asynchronously only when the existing architecture proves this is safe and tests show no leaked non-daemon thread, socket, or process can hold Kanban capacity.

Do not introduce unsafe cross-thread client closing. Reuse the current abort/poison/owner-thread cleanup patterns and their race tests.

## 10. Structured outcome contract

A stalled request returns a bounded internal result equivalent to:

```yaml
completed: false
partial: true
error_code: provider_request_stalled
error_subtype: no_progress_timeout | absolute_timeout | interrupted
provider: <non-secret provider id>
model: <model id>
request_attempt: <integer>
elapsed_seconds: <bounded number>
retryable: true | false
side_effecting_tool_dispatched: false
```

The user-facing text may be concise, but the worker/dispatcher must not rely on parsing prose.

For a Kanban worker whose task remains `running`, the worker wrapper must map this result through existing task failure/retry semantics. It must not exit successfully while leaving the task claimed and heartbeat-active. Depending on existing board policy, the deterministic outcome is one of:

- bounded retry/fallback when already authorized;
- task returned for retry with failure count incremented; or
- task blocked with a sanitized reason.

The implementation plan must select the existing canonical path and test it end to end rather than inventing parallel task-state logic.

## 11. Provider parity audit

Before changing code, build a table for every active request mode showing:

- timeout configuration source;
- absolute timeout enforcement site;
- no-progress timeout enforcement site;
- cancellation method;
- resource owner/cleanup site;
- structured outcome mapping;
- existing tests.

Any mode used by Kanban that silently drops `request_timeout_seconds`, lacks stale detection, or cannot unwind on interrupt is in scope. Modes not used by Kanban may be covered when they share the same broken abstraction, but unrelated provider refactors are out of scope.

The audit must explicitly include the `gemini` provider path used by the observed `specifier` profile.

## 12. Configuration and deployment

1. Reuse `config.yaml` provider/model timeout keys already supported by Hermes.
2. If a missing user-facing config key is genuinely required, add it to the canonical config schema/defaults and setup/config documentation; do not require `.env`.
3. The linked model-routing implementation may set worker-appropriate timeout values through its sanitized, allowlisted, atomic deployment policy.
4. This runtime task must not make ad hoc edits to `/home/orion/.hermes/config.yaml` or profile configuration.
5. Code deployment occurs only after review and verification against the current local checkout state.
6. Preserve and separately reconcile the installed checkout’s existing modifications in `hermes_cli/tools_config.py` and `package-lock.json`; never overwrite them through reset/pull.
7. Restart owning Hermes gateway/worker processes only through a reviewed deployment step after code changes are installed.

## 13. Tests

### 13.1 Primary regression

Create a deterministic fake transport sequence:

1. first response ends with `finish_reason='length'` and a partial `write_file` tool call;
2. Hermes classifies it as truncated and begins its bounded retry;
3. the retry connects but emits no response progress indefinitely;
4. a fake clock crosses the configured stale or absolute deadline.

Assert:

- `write_file` is never dispatched;
- the retry is cancelled/aborted;
- the request returns `provider_request_stalled` within the bound;
- the worker request thread exits;
- exactly one terminal lifecycle event is emitted;
- no automatic heartbeat can extend the request deadline;
- session messages remain valid and role-alternating;
- request resources are not reused after abort;
- the Kanban task is routed through its existing retry/block path rather than left running.

### 13.2 Current-main verification

Run the primary regression unmodified against current `origin/main` first. If it passes, record the exact commit providing the behavior and do not add duplicate implementation. Retain a regression test only if current coverage does not prove the full Kanban-worker contract.

### 13.3 Provider modes

Use contract tests for streaming and non-streaming OpenAI-compatible paths, native Gemini, Anthropic, Bedrock, Codex responses, and fallback retries. Tests may use fake transports but must exercise real normalization, timeout resolution, cancellation, and result-mapping code.

### 13.4 Cancellation races

Cover:

- timeout and external interrupt racing;
- response completion at the timeout boundary;
- client abort finding zero sockets;
- owner-thread cleanup after cross-thread abort;
- stale event arriving after terminal state;
- retry/fallback activation after a timed-out attempt;
- process shutdown during request cleanup.

Exactly one terminal outcome wins.

### 13.5 Invariants

Assert prompt-cache input remains byte-stable except for existing legal continuation/repair behavior; message alternation is preserved; incomplete mutating tools have no side effects; configured provider/model timeout precedence is unchanged for working modes; and non-Kanban interactive sessions retain their documented interrupt behavior.

## 14. Observability and privacy

Log and lifecycle records may include:

- task/run/request identifiers;
- provider/model IDs;
- timing and attempt number;
- timeout subtype;
- cancellation and cleanup outcome;
- whether any tool was dispatched.

They must not include prompt/response contents, credentials, authorization headers, complete tool arguments, or secret query strings. Error text from providers must pass existing redaction and truncation controls.

## 15. Rollout

1. Reproduce on current `origin/main` with a fake transport and fake clock.
2. If already fixed, verify the implementing commits and stop duplicate development.
3. Otherwise implement the smallest shared request-lifecycle correction using TDD.
4. Run focused request/stream/cancellation tests.
5. Run Kanban worker integration tests.
6. Run the full relevant Hermes Agent suite using repository instructions.
7. Open an upstream-quality PR from a user fork; do not push directly to NousResearch main.
8. After review, stage a local upgrade in isolation and verify the installed checkout’s unrelated modifications are preserved.
9. Restart the owning gateway/worker processes and run a synthetic Kanban canary.
10. Enable the blocker monitor’s warning/reclaim stages separately according to its addendum rollout.

## 16. Rollback

Runtime rollback restores the previously installed Hermes Agent revision and restarts owning processes. Configuration changes, if any, are reverted through their own atomic backups. The blocker monitor may remain in observe/warn mode during runtime rollback but must not assume new lifecycle events exist.

No rollback deletes Kanban tasks, worktrees, logs, proposal records, or user configuration.

## 17. Acceptance criteria

1. The observed truncated-`write_file` then stalled-retry sequence is reproduced by an automated test.
2. Incomplete tool arguments never execute.
3. Every Kanban-used provider request path has a bounded absolute and no-progress outcome or a documented shared implementation proving it.
4. A stalled retry returns a structured `provider_request_stalled` result and the worker request unwinds.
5. Automatic Kanban heartbeat cannot extend a provider request deadline.
6. Timeout/interrupt cleanup does not leak a capacity-holding process, thread, or reusable poisoned client.
7. Kanban routes the failed run through existing retry/block semantics rather than leaving it `running`.
8. Provider/model timeout precedence remains backward-compatible and is configured through `config.yaml`.
9. Current-main behavior is tested before implementation; already-implemented behavior is not duplicated.
10. The dirty installed checkout is not modified or reset during development.
11. Focused and integration tests pass, with prompt-cache and message-alternation invariants preserved.
12. The runtime work is completed before blocker-doctor task `t_a75bfbea` resumes implementation.
