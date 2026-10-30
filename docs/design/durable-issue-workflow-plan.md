# Durable Issue Workflow Implementation Plan

This plan sequences the intended workflow in
[durable-issue-workflow.md](durable-issue-workflow.md). The canonical document
remains the behavior contract; this file tracks implementation decisions,
acceptance evidence, and blockers.

## Phase 1: Prove The Manual Durable Path

**Status: Implemented locally; live manual runtime acceptance remains unverified.**

### Local Verification

- `python -m pytest orchestrator/supervisor/ -q`: 116 passed.
- `python -m pytest orchestrator/gateway/ -q`: 451 passed.
- `ruff check orchestrator/`: passed.
- The gateway suite includes a manual `POST /api/v1/tasks/spawn` integration
  through the resident graph and real SQLite checkpoint saver. Its worker,
  judge, and PR boundaries are mocked; this does not substitute for the live
  manual runtime acceptance run below.

### Evidence-backed decisions

- Use the existing manual issue intake in `POST /api/v1/tasks`; Phase 1 does
  not consume GitHub Project events. The gateway writes the queued task to
  SQLite and pins its repository credentials and agent configuration.
- Keep the task row and operational events authoritative for task identity,
  lifecycle/status, business results, and execution history. A LangGraph
  checkpoint may contain resumable execution state only.
- Use the database `task_id` as the proposed graph `thread_id`. Initial issue
  IDs are stable and unique in the task table; manual retries get a distinct
  task ID. Add a regression test for this mapping when graph invocation is
  implemented.
- Invoke LangGraph from the resident claimed-task path. Do not wrap the current
  procedural workflow in a no-op graph and do not replace its verified
  candidate, judge, scoped worker, or publication behavior with the legacy
  mock-only `TaskState` graph.
- Preserve the current human gate: publication opens or reuses an open PR and
  never merges it. The task branch and existing-open-PR lookup make replay
  idempotent; a GitHub duplicate-create response is reconciled by rereading the
  matching open PR.

### Resolved Decisions

- **Backend/package/path:** use `langgraph-checkpoint-sqlite>=3.1,<4.0` and
  `AsyncSqliteSaver`. Keep checkpoint files separate from the gateway/supervisor
  task DB to avoid sharing SQLite connections and journal configuration. The
  default path is `/data/tasks/checkpoints-v<schema>-attempt-<n>.sqlite3`, beside
  `tasks.sqlite3` on the existing shared persistent volume; operators may set
  `TASK_CHECKPOINT_DB_DIR` to another directory on durable storage.
- **Retention:** no automatic checkpoint pruning. Retain every task/attempt/
  schema file for the lifetime of the task record; deletion requires an explicit
  operator retention action. New attempts and schema versions get new files,
  leaving prior checkpoints available for diagnosis.
- **Identity/versioning:** `tasks.task_id` is the LangGraph `thread_id`.
  Checkpoint state schema version 1 stores only task ID, attempt, schema
  version, and routing flags. A schema change increments the version and uses a
  new file; it does not reinterpret or overwrite old checkpoint data.
- **Recovery precedence:** the task row and operational events always win.
  Before DB writes or side effects, graph nodes reread and fence on running
  status, `claim_owner`, and attempt. Plans, worker jobs/results, judge outcomes,
  PR URL, and terminal result are read from or committed to the task DB; a
  checkpoint only supplies a resumable graph cursor/routing state. A new task
  attempt uses a new checkpoint file; lease recovery within the same attempt
  resumes the same task/thread/file.
- **Failure ordering/side effects:** task/job/plan/event writes commit before
  graph checkpoint progression. A deterministic Kubernetes job name and saved
  job record prevent redispatch after replay. Judge acceptance is recorded
  before publication. Publication writes a `started` event before GitHub calls,
  then commits `pr_url` with its completed event; replay reuses the task branch
  and open PR. If a duplicate PR create races, HTTP 422 triggers a reread and
  reuse of the matching open PR. The gateway accepts terminal writes only from
  the current claim and attempt and requires the DB-recorded PR URL.
- **Graph boundary:** the compiled durable graph invokes actual plan, dispatch,
  candidate wait, judge, publication, and completion operations. It does not
  call the legacy mock-only `TaskState` workflow. Candidate and other business
  results are not copied into checkpoint state.

### Tasks

1. [x] Decide and document checkpoint storage, durable path, retention, identity,
  schema/version policy, and recovery precedence.
2. [x] Invoke the real staged graph from the resident claimed-task path with
  claim fencing, DB authority, and replay reconciliation.
3. [x] Add focused manual-enqueue, task/thread identity, lease recovery,
  validated-candidate, and DB/checkpoint ordering tests.
4. [x] Test PR replay and a duplicate-create race; verify one PR and no merge.
5. [ ] Run one manually enqueued issue through the live resident runtime and
  capture DB events/status, checkpoint recovery, validation, resulting PR, and
  the still-required human merge. Not performed: no designated issue/repository
  and live GitHub App/cluster run authorization were supplied, so creating a
  real PR would not be a safe local validation step.

### Acceptance criteria

- A local integration test manually enqueues through the supported task API and
  executes the compiled LangGraph workflow; no Project webhook or reconciliation
  is involved. A live manual runtime run remains unverified.
- Tests show interruption/lease recovery resumes the same attempt and task thread
  without losing DB-authoritative state or repeating judge/publication effects.
- Replay and simulated duplicate-create race tests show one PR URL is reused;
  publication and recovery contain no merge path.
- Tests cover the selected SQLite storage/path/version/retention contract and
  DB/checkpoint failure ordering. Live end-to-end issue/PR evidence is still
  outstanding for the blocker above.

## Phase 2: Complete Execution Semantics

**Status: Not started; gated on Phase 1 being stable.** Phase 1 stability means
its manual durable path and recovery acceptance criteria pass. This section is
an implementation plan, not evidence that these behaviors exist. Keep Project
intake and status projection work in Phases 3 and 4.

### Decisions Required Before Implementation

Resolve and record these decisions before building the Phase 2 execution path.
CodeFactory owns application and workflow decisions; involve infra-codefactory
only for environment, package, or rollout changes that the selected design
actually requires.

- **DAG schema and state:** Define the persisted plan, dependency edges, step
  lifecycle states, and plan-version behavior. Define stable step work-item IDs
  across retries and recovery, including what happens when a plan is revised.
  Keep the database authoritative for business and lifecycle state, coordinated
  with resumable graph/checkpoint state per the Phase 1 contract.
- **Workspace and artifact handoff:** Define how each step gets an isolated
  workspace, its declared inputs, and the accepted artifacts it returns. Specify
  how artifacts are identified, persisted, and made available to dependent
  steps, and how isolation is enforced between sibling steps and retries.
- **Allowed-agent policy:** Define the authoritative policy and the point at
  which planned agent assignments are checked. Define behavior for missing,
  invalid, or disallowed assignments; issue labels remain context and cannot
  authorize an agent.
- **Acceptance and review boundaries:** Define step acceptance criteria and
  distinguish executable validation (for example, configured checks) from the
  judge's assessment against those criteria. Define what evidence each produces,
  what constitutes a judge rejection, and the separate integration/final-review
  criteria across accepted step outputs.
- **Integration and final-review contract:** Define how accepted artifacts are
  combined, how integration conflicts are reported or resolved, and what final
  review must pass before publication. Keep the publication unit to exactly one
  PR per sub-issue, not one PR per step or agent.
- **Idempotency, retries, and recovery:** Define durable dispatch, attempt,
  validation, judgment, acceptance, unlock, integration, and publication state;
  idempotency keys and side-effect boundaries; and recovery precedence after
  partial writes or crashes. Count judge rejections against one planned step
  work item within one sub-issue. The fixed limit is eight rejections for that
  item; after the eighth, surface exhaustion and stop automatic retries without
  automatic replanning.
- **Human completion gate:** Preserve the existing intended contract: CodeFactory
  may publish the PR but never merge it or mark the sub-issue `Done`. A human
  reviews and merges the PR, then marks the sub-issue `Done`.

### Implementation Tasks

All workflow implementation tasks below belong to CodeFactory. Any necessary
runtime dependency, environment, or deployment changes belong to
infra-codefactory and are limited to enabling the chosen CodeFactory design.

1. Persist a versioned plan DAG with stable step work-item IDs, dependencies,
   assignments, criteria, and durable step states. Validate the plan and reject
   invalid dependencies or disallowed agent assignments before dispatch.
2. Select and dispatch only steps whose dependencies are durably accepted. Make
   dispatch idempotent so recovery does not start duplicate work for the same
   step attempt.
3. Execute each dispatched step in its isolated workspace using only its
   declared inputs. Capture outputs as identified artifacts and hand them to
   dependents only through the defined artifact contract.
4. Run step validation and judge assessment as distinct outcomes. Persist
   attempts and judge rejections per stable step work-item ID; allow at most
   eight judge rejections, then surface exhausted work for resolution without
   automatic replanning.
5. On acceptance, persist the accepted result and artifacts before marking
   dependents ready. Make acceptance and unlock replay-safe so recovery neither
   repeats accepted work nor unlocks a dependent prematurely.
6. Once all required steps are accepted, integrate their artifacts, surface
   conflicts according to the agreed contract, and run final review across the
   combined result. Do not publish when integration or final review fails.
7. Publish exactly one PR for the sub-issue after successful integration and
   final review. Make publication replay-safe; leave merge and the subsequent
   human `Done` transition outside CodeFactory control.

### Focused Acceptance Tests

- **Dependency order:** blocked steps do not dispatch; accepting prerequisites
  makes only their eligible dependents ready.
- **Allowed agents:** allowed assignments dispatch, while missing or disallowed
  assignments are rejected before worker execution; labels cannot authorize
  assignments.
- **Workspace isolation:** sibling steps and separate attempts cannot read or
  mutate one another's workspace state except through declared artifacts.
- **Retry accounting and exhaustion:** judge rejections accrue to the correct
  step work item, persist across recovery, and stop at eight; exhaustion is
  surfaced and does not trigger automatic replanning or another automatic
  attempt.
- **Resume and replay:** replay after interruption does not duplicate accepted
  work, acceptance, unlocks, integration effects, or publication effects.
- **Integration conflicts:** conflicts follow the chosen contract and prevent
  publication until resolved; final review runs against the combined result.
- **PR uniqueness:** one sub-issue produces exactly one PR across retries,
  crashes, and replay, regardless of its number of DAG steps.
- **Human gate:** CodeFactory does not merge the PR or mark the sub-issue
  `Done`; completion remains gated on human merge and the human status change.
- **Phase 1 regression:** the Phase 1 manual durable-path and recovery tests
  continue to pass.

**Exit criteria:** all prerequisite decisions are recorded; the focused tests
above pass, including Phase 1 recovery tests; and a manual end-to-end run
demonstrates DAG execution through one PR while preserving the human merge and
`Done` gates. Do not proceed to Phase 3 intake until Phase 2 is stable.

## Phase 3: Add Project Intake

Only after Phase 2 is stable, configure the human `Ready` status and add
authenticated webhook intake plus reconciliation. Project state is an intake
signal only; after enqueue, the CodeFactory DB is authoritative for execution.

### Decisions Required Before Implementation

- **Project configuration and identity:** Define the `Ready` option and the
  Project, item, repository, and issue identifiers used to recognize an
  eligible sub-issue and map it to one durable CodeFactory task. Record who
  applies the Project configuration.
- **Webhook authentication and event contract:** Select the supported GitHub
  integration and verification mechanism, required credentials/scopes, accepted
  event types, and the stable delivery/event identity used for deduplication.
  Define acknowledgment and retry behavior when authentication, processing, or
  enqueue fails.
- **Reconciliation and disagreement policy:** Define its source query,
  cadence/trigger, and behavior when a webhook is delayed or missing. Specify
  how `Ready` removal, an already-enqueued issue, and disagreement between
  Project state and DB state are handled. Once enqueued, reconciliation must
  not use Project state to rewind or replace DB lifecycle state.
- **Replay and transaction boundary:** Define the durable deduplication record
  and idempotency key for enqueue, including how event recording and task
  creation recover from partial failure. Project events and reconciliation
  must converge on the same task identity without duplicate work.

### Implementation Tasks

CodeFactory owns Project intake, event handling, reconciliation, and durable
enqueue behavior. `infra-codefactory` is involved only if the selected design
requires runtime secrets, environment, package, or rollout changes.

1. Configure the human `Ready` status and document its eligible-item meaning
   and Project identity mapping.
2. Implement authenticated webhook receipt and validation. Persist/deduplicate
   deliveries and idempotently enqueue eligible sub-issues using the agreed
   stable identity; do not treat event order as lifecycle authority.
3. Implement reconciliation using the agreed query and schedule. Route
   discovered eligible issues through the same deduplication and enqueue path
   as webhooks so the two intake mechanisms converge.
4. Make processing replay-safe across duplicate deliveries, retries, crashes,
   and partial writes. Reconcile GitHub observations against the DB without
   changing an existing task's authoritative execution state.
5. Add operational outcomes for rejected/unauthenticated events and
   reconciliation or enqueue failures so they can be retried or investigated
   without silently losing eligible work.

### Focused Tests

- Valid authenticated deliveries are accepted; invalid or missing
  authentication cannot enqueue work.
- Duplicate deliveries, webhook/reconciliation races, and retry after partial
  failure produce one task and one enqueue effect for an issue.
- Delayed and out-of-order events, including `Ready` removal after enqueue,
  cannot duplicate work or rewind DB lifecycle; disagreement follows the
  recorded policy.
- Replaying a delivery or reconciliation pass after task creation or recovery
  is safe, and an enqueue failure remains recoverable.

**Exit criteria:** Phase 2 is stable; Project identity, authentication,
deduplication, reconciliation, and disagreement decisions are recorded; and
focused tests prove authenticated intake and reconciliation converge
idempotently without overriding DB execution state.

## Phase 4: Add Visibility Contracts

Begin after Phase 3's intake identity and DB-authority prerequisites are agreed;
implement the projection after Phase 3 is stable. Project `In Progress` is a
human-visible projection of active execution, never an execution-state input.

### Decisions Required Before Implementation

- **Projection owner and update mechanism:** Name the component responsible for
  updating Project status and select its authenticated update mechanism. Define
  how it observes authoritative DB state and handles permission, rate-limit,
  and transient API failures. Keep application behavior in CodeFactory;
  involve `infra-codefactory` only for required runtime configuration or
  rollout.
- **Trigger, timing, and stale repair:** Define which DB lifecycle transitions
  enter and leave active execution, when projection updates occur, and how
  delayed or failed updates are retried. Specify a repair/reconciliation
  trigger and how stale Project status is detected and corrected from DB state.
- **Parent/sub-issue identity and rollup:** Define the stable identities and
  association source for a parent and its sub-issues, then specify the parent
  status/progress calculation for mixed, incomplete, failed, or empty child
  sets. Define how child changes trigger or refresh the rollup.
- **Human completion gate:** Keep `Done` human-controlled and only applicable
  after PR merge. Define how documentation and tests distinguish this human
  transition from CodeFactory's projection updates; PR creation or DB execution
  completion must not mark a sub-issue `Done`.

### Implementation Tasks

1. Implement the selected CodeFactory-owned projector from authoritative DB
   lifecycle state to Project `In Progress`, using the agreed trigger and
   idempotent update mechanism. Project status must never write or override DB
   execution state.
2. Implement retry and stale-state repair from DB to Project, including
   observable handling of failed updates. Repair must converge the projection
   to DB state and must not infer execution state from Project status.
3. Implement parent progress rollup using the agreed parent/sub-issue identity
   and child-state contract; make refresh safe to repeat and recover.
4. Preserve the human-only `Done` transition after merge. Do not have
   publication, task completion, projection repair, or parent rollup mark a
   sub-issue `Done`.

### Focused Tests

- DB lifecycle transitions produce the agreed `In Progress` projection, and
  Project status changes never mutate or override DB execution state.
- Failed or delayed projection updates are retried; stale Project state is
  repaired from DB state, including safe repeated repair.
- Parent rollup matches the defined identity and mixed-child contract and is
  consistent after child updates, retries, and repair.
- PR creation and CodeFactory execution completion do not set `Done`; the
  human completion gate remains after PR merge.

**Exit criteria:** ownership, trigger/timing, repair, identity, rollup, and
human-gate decisions are recorded; focused tests prove projection consistency,
stale repair, rollup, and human-controlled completion without changing DB
authority.
