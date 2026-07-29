# Kanban Systemic Reliability Investigation and Design

Date: 2026-07-29

## Scope and safety

This investigation used the live board, logs, SQLite database, profile
configuration, Docker state, and process/cgroup state read-only. No live task,
database, profile, cron, container, or service was changed. Evidence below is
aggregated and omits task bodies, credentials, private messages, and secret
configuration values.

The implementation is split by ownership:

- `hermes-agent` owns task/DAG invariants, dispatcher classification,
  workspace portability, worker sandbox lifecycle, supersession, provenance,
  diagnostics, and CLI primitives.
- `hermes-orchestration` owns role policies, wave/integration discipline,
  deployment of those policies, and real-contract tests for local automation.

No new model tool is required. The narrow waist stays unchanged.

## Evidence

### Live board and runtime

- The default board has no `default_workdir`.
- Forty-five tasks have `workspace_kind=worktree` and a null path: 19 archived,
  3 blocked, and 23 todo. The three currently blocked rows each recorded the
  same deterministic workspace failure three times.
- All 28 `spawn_failed` events and 14 associated `gave_up` errors sampled have
  the same missing-worktree/default-workdir root cause.
- Thirty-two events report a worker exiting rc=0 without
  `kanban_complete`/`kanban_block`.
- Nine nonterminal tasks are descendants of archived parents. Ordinary archive
  currently satisfies a dependency and immediately promotes children.
- A persistent engineer container is at `pids.current=256` with
  `pids.max=256`; `pids.events` records 1,259 limit hits. Its process table
  contains stale npm/vitest trees from earlier attempts. A fresh comparison
  container was healthy at 2/256.
- A representative linked worktree's `.git` file points into the owning
  repository's absolute Git common directory. The engineer container can see
  the worktree but not that common directory, and `git rev-parse` deterministically
  fails there.
- Worker logs show the auxiliary compression model detected as 45,824 tokens
  and rejected below Hermes' 64K minimum, even though the profile's configured
  custom-model catalog records 131,584. The failure happened after repeated
  reads and left no terminal Kanban transition.
- Legacy profile `.env` files still contain non-secret `TERMINAL_*` settings
  that differ from `config.yaml`. Current gateway/CLI source makes the terminal
  config section authoritative, so this is a latent configuration smell rather
  than the demonstrated workspace/backend cause. Startup readback should still
  detect contradictions without printing values.

### Exact source flow

- `create_task()` validates only the workspace enum. A worktree with no
  explicit path and no board default is inserted as runnable.
- `decompose_triage_task()` clears inherited worktree paths for sibling
  isolation, but does not verify that the board has a usable Git anchor before
  inserting and promoting the graph.
- Workspace resolution is deferred until after `claim_task()`.
  `_dispatch_once_locked()` classifies every resolution exception as an
  ordinary spawn failure, so deterministic defects consume the transient retry
  ladder.
- Linked worktree materialization correctly isolates branches on the host, but
  the Docker cwd mount includes only the checkout. Absolute Git admin/common
  metadata is not mounted.
- `_resolve_container_task_id()` collapses normal terminal users to `default`.
  Docker reuse therefore shares one `(default, profile)` container across
  separate Kanban workers. Persist-mode cleanup intentionally preserves that
  container and its background processes; the orphan reaper only removes old
  exited containers.
- `archive_task()` intentionally treats archived parents like done parents.
  This behavior is useful for ordinary manual archive and must remain; it
  cannot also represent cancellation/supersession.
- Completion metadata is free-form and downstream dependency edges gate only
  status. There is no enforced workspace/branch/commit readback and a child
  branch is normally created from the repository's current `HEAD`, not a
  completed parent's recorded commit.

### Orchestration and CLI contract

- Current specifier/orchestrator/reviewer/integrator policies check prose and
  plan readability, not an executable workspace/commit/artifact manifest.
  Reviewer cards commonly use scratch space plus an instruction to “discover”
  the engineer worktree.
- Several completed prerequisites produced commits on independent branches.
  Later cards start from `main`; dependency edges delay dispatch but do not
  compose Git history.
- Policy deployment copies shared reference files, not the actual
  `<profile>/SOUL.md` targets. Source, shared copy, and live profile hashes have
  drifted.
- The installed parser rejects the monitor wrapper's invented contract:
  `cron list` has no `--json`, the mutation verb is `edit` rather than
  `update`, and `cron create` takes schedule positionally. Tests passed because
  their fake implemented the same invented grammar.

## Failure taxonomy and root causes

| Class | Root cause | Required invariant |
|---|---|---|
| Invalid runnable task/DAG | Workspace feasibility is checked after insert and claim | Validate the complete workspace spec before any task or graph becomes runnable |
| Remote worktree cannot commit | Checkout mount omits absolute Git common/admin metadata | Mount checkout and owning Git metadata read-write at their original absolute paths and verify Git before work |
| PID/process exhaustion | All Kanban workers for a profile reuse one persistent `default` container | Kanban task-scoped disposable sandbox; health/headroom preflight; exact-sandbox quarantine only |
| Repeated deterministic retries | Workspace/config errors have no typed classification | Permanent preflight/spawn errors block once; only transient errors retry |
| Clean protocol exit | Critical runtime feasibility is lazy and failure does not imply a terminal task call | Eager Kanban startup preflight plus explicit terminal-state verification |
| Duplicate DAG promotion | Archive doubles as dependency success and cancellation | Separate atomic supersede-subtree transition with replacement linkage |
| Unreachable prerequisite work | Status edges carry no enforced Git/artifact provenance | Record and validate workspace, branch, base/result SHA, clean state, plan hash; start linear dependents from validated parent SHA |
| Eager fan-out | Decomposer inserts the whole graph before foundation feasibility is known | Atomic graph preflight, explicit dependency edges, bounded wave policy |
| Self-confirming CLI tests | Wrapper fake is the contract oracle | Run wrapper/parser integration against the real entry point in a temp `HERMES_HOME` |
| Policy deployment drift | Deployment target differs from runtime profile target | Deploy/check actual role SOUL files atomically |

## Rejected symptom-level workarounds

- Retrying the same missing-workspace error: deterministic and only increases
  board noise.
- Setting today's live task paths by hand: repairs cards, not creation paths.
- Copying a linked worktree's `.git` file into scratch: preserves a stale
  absolute pointer without the owning repository metadata.
- Mounting only the checkout at `/workspace`: the Git pointer and backlink
  still refer to host-absolute paths.
- Globally disabling persistent Docker reuse: breaks the intentional normal
  chat contract. Isolation must be Kanban-scoped.
- Host-wide `pkill`, Docker prune, or indiscriminate container removal: can
  kill unrelated work.
- Treating archive as cancellation: ordinary archive is intentionally
  dependency-successful and races descendant promotion.
- Encoding dependencies in body prose: the dispatcher cannot enforce prose.
- Automatically merging arbitrary parallel parent commits: conflicts and
  semantic integration require an explicit integration wave.
- Silently disabling compression or lowering Hermes' context minimum: hides a
  broken runtime and can corrupt long task execution.
- Adding more mocks for the cron wrapper: a fake grammar cannot validate the
  real parser.

## Chosen design

### 1. Central workspace and worker preflight

Add one read-only workspace validator used by direct creation, dashboard/tool
creation, project-derived workspaces, decomposition, diagnostics, and dispatch.

- `worktree` must resolve through an explicit absolute path or a valid absolute
  board default to an existing Git repository/common directory.
- `dir` and legacy explicit scratch paths must be absolute.
- Decomposition preflights every effective child workspace before its single
  write transaction. Any invalid node rejects the graph with zero inserts.
- Dispatch revalidates legacy rows. A typed permanent preflight exception
  closes the run and blocks once with repair guidance; transient filesystem or
  process failures retain the configured retry ladder.
- Kanban startup validates the effective profile tool surface, compression
  context floor, terminal backend/mount plan, Git branch/write/commit
  capability, and container PID headroom before the first model turn.

### 2. Portable, hygienic Kanban execution

For a worktree worker, derive the checkout, Git dir, and Git common directory
on the host. Pass a machine-readable mount plan to the terminal backend. Docker
bind-mounts the checkout and Git metadata read-write at the same absolute paths
in addition to the normal `/workspace` convenience mount.

Kanban terminal environments use the Kanban task id as their isolation key,
disable cross-process container reuse, and are force-removed at worker
finalization. A preflight rejects/quarantines only an exact Hermes-labeled
task/profile sandbox when PID capacity is exhausted or the mount/Git readback
does not match. Normal interactive containers keep their existing persistence.

### 3. Atomic supersession

Add nullable `superseded_by` and `superseded_at` columns and a database
transaction that:

1. verifies the replacement exists and is outside the old subtree;
2. computes the old root plus all descendants;
3. archives every nonterminal member, closes active runs, clears claims, and
   marks it superseded;
4. records the replacement id on the old root and audit events on every member.

Superseded archive rows do not satisfy dependency promotion. Ordinary archive
continues to satisfy dependencies. Expose this as a CLI command with a
read-only `--dry-run`; do not add a model tool.

### 4. Provenance-aware handoff and promotion

Worktree completion derives and validates, rather than trusting model prose:

- absolute workspace and Git common directory;
- expected/current branch;
- base SHA and result `HEAD` SHA;
- clean working tree;
- optional plan SHA/hash supplied by policy.

The trusted fields are stored in run metadata and surfaced to children.
Completion fails closed on an uncommitted or mismatched worktree. A
single-parent worktree child is created from the validated parent result SHA.
Multi-parent integration remains an explicit card/wave; all parent SHAs remain
reachable and are listed for deliberate cherry-pick/merge.

### 5. Diagnostics and compatibility

Add diagnostics for invalid legacy workspace rows, superseded descendants,
missing/mismatched handoff provenance, worker preflight failure, PID exhaustion,
and stale behavior-setting keys in `.env` (key names only). Suggested actions
are dry-run or explicit CLI commands. No automatic repair touches protected
jobs, artifacts, or live tasks.

Existing boards migrate additively. Invalid legacy rows remain visible but
cannot enter another retry loop; operators set a board default/recreate a
valid replacement and atomically supersede the old subtree. Ordinary archive,
scratch tasks, external/manual worker lanes, and normal Docker persistence
retain their contracts.

### 6. Orchestration policy

Update the role contracts to use a staged manifest:

- planning/specification records plan path and hash;
- foundation wave is small and explicitly dependency-linked;
- each implementation handoff records workspace, common dir, branch, base SHA,
  result SHA, tests, and clean state;
- integration/review consumes exact SHAs in an explicit reachable workspace;
- later waves are created/promoted only after manifest readback;
- remediation uses the core supersede command rather than parallel duplicate
  DAGs.

Deploy/check actual profile SOUL targets. Replace cron grammar mocks as the
contract oracle with a temp-home invocation of the real parser/entry point.

## Compatibility, migration, and rollback

- Schema changes are additive and nullable. Old binaries ignore the new
  columns; new binaries migrate them idempotently.
- No existing task is auto-superseded or auto-rewritten.
- New invalid worktree tasks are rejected. Existing invalid tasks are
  diagnosed and fail once if dispatch reaches them.
- Operators migrate by configuring a valid board default or creating a valid
  replacement, verifying the dry-run subtree, then executing supersede.
- Rollback is code rollback plus leaving additive columns in place. Do not
  downgrade by deleting columns. Before rollback, stop using supersede because
  an old dispatcher would again treat those archived rows as dependency
  success.
- Kanban-scoped disposable containers can be rolled back independently by
  reverting the worker env/isolation change; normal chat persistence is never
  changed.

## RED-GREEN-REFACTOR test matrix

| Layer | Red regression | Green contract |
|---|---|---|
| Creation | Worktree/no path/no default inserts ready row | Raises before insert; task count unchanged |
| Decomposition E2E | Invalid root fans out runnable descendants | Entire graph rejected; root remains triage; zero children |
| Legacy dispatch | Same invalid row consumes retry ladder | One permanent failure/run, blocked immediately |
| Worktree mount | Container sees checkout but `git rev-parse` fails | Exact absolute checkout/common-dir readback; edit, test, commit succeed |
| Sandbox reuse | Same-profile tasks reuse `default` container | Unique task labels/mounts; exact sandbox removed; unrelated container untouched |
| PID health | Reused container at limit attempts another fork | Preflight quarantines/recreates exact task sandbox before work |
| Protocol/config | Bad 45,824 compression route fails after reads/rc=0 | Eager preflight fails before first model/tool call, no transient retry |
| Supersede | Archiving old parent promotes descendants | Transaction freezes subtree and records replacement; no later promotion |
| Provenance | Dirty/mismatched branch completes with prose metadata | Completion rejects; trusted clean branch/SHA metadata round-trips |
| Dependency SHA | Linear child starts from unrelated `main` HEAD | Child branch base equals validated parent result SHA |
| Policy wave | Body-only prerequisite starts downstream work | Manifest/edge/readback gate prevents promotion |
| Cron wrapper | Fake accepts invented grammar | Real entry point in temp home validates create/edit/pause/list idempotently |
| Deployment | Shared copy passes while actual SOUL is stale | Dry-run no writes; apply/check hashes actual targets; protected fixtures unchanged |
| Disposable worker E2E | Worker may leave task running/processes | Worker edits, tests, commits, calls terminal state, exits, and leaks no process/container |

Focused suites cover Kanban DB/CLI/decomposition/diagnostics/worktree/dispatcher,
terminal Docker isolation, and orchestration policy/wrapper behavior. Broader
verification runs the relevant Hermes CLI, tools, gateway Kanban, terminal, and
orchestration suites with all homes and board paths redirected to temporary
directories.
