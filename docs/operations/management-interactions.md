# Management agent interactions

Expand a task in **Management → Tasks** to see **Agent interactions** above
the existing execution, failure and configuration details. It refreshes with
the task every ten seconds. The view is read-only and requires the same
management authentication as task details.

## What the view actually knows

- **Supervisor:** whether a durable plan exists, not its contents.
- **Supervisor → worker:** the latest stored Kubernetes job name, not pod
  health or confirmation of a running coder.
- **Coder / worker → supervisor:** a retained candidate/candidate-failed
  callback and its sandbox validation status. Individual model calls are not
  tracked. The terminal supervisor result replaces the worker result, so a
  completed task can correctly show no retained worker result.
- **Judge → supervisor:** a persisted numeric review score in `[0, 1]`, when
  present in the terminal result. No acceptance threshold or pass/fail decision
  is inferred from this number.
- **Feedback loop:** latest saved feedback source, attempt number and
  allowlisted review topics (tests, correctness, security, scope,
  patch-integrity). This is not a transcript or confirmation that the next
  worker consumed it.
- **Outcome:** task state and the existing Human review pull-request link.
  Completion is not human approval or merge.

The gateway `/api/v1/tasks/{id}` now includes `coordination.plan_recorded`
and `coordination.latest_feedback`, projected from existing durable `plan` and
`retry_feedback` columns. The management adapter revalidates that projection
and adds `coordination.judge_score` from the existing result. No database
migration, new runtime events or deployment configuration is needed.
Only fixed stage/topic vocabularies, a bounded integer attempt, a boolean
and a bounded numeric score are added to the management response. Raw plans,
prompts, model reviews, exception payloads, patches, credentials and endpoint
configuration are not added.

This is a **snapshot, not an ordered event history**. Requeue clears a candidate
result but can retain a previous job and feedback. Attempts can span those
records; no timestamps, durations, in-flight judge status, worker identity or
per-attempt handoff history are fabricated. Missing/malformed telemetry is
shown as not recorded. API errors use the existing retry state rather than
displaying a false empty interaction history.
Manual retries can inherit feedback from their `retry_of` task; the saved
feedback attempt number can therefore refer to the original task.

## Model and prompt revisions

Management → Models can draft and validate coder/judge model IDs plus bounded
worker, planner and judge guidance. Apply requires the loaded config hash and
the same authenticated, same-origin Management request checks as other writes.
The gateway validates a fixed role/field allowlist, records the operator and
timestamp, and atomically advances the active revision only when the expected
hash still matches. Prompt guidance cannot change catalog tools, roles, file
limits, credentials, task authority or response contracts; agent definitions
remain read-only. Endpoints, context settings, compute arguments and inference
pod rollouts are not managed here.

The gateway stores immutable config revisions in the durable task SQLite
database on the existing task-state volume. Each newly accepted task pins the
active version and hash in its task row; the supervisor reads that exact
revision for planning, worker dispatch and judging. Running tasks keep their
original snapshot. No Helm-owned ConfigMap is written and applying a revision
does not restart inference pods.

Helm provides the initial config seed only. Once the database has been seeded,
subsequent chart values do not replace the active UI-managed model or prompt
revision. Use Management for those live changes and include the task-state
database in the existing backup/retention plan so task snapshots remain
available. ConfigMap reads remain only for tasks pinned before this revision
store was introduced.

## Local checks

From the repository root, using the service test environment:

```sh
python3 -m pytest orchestrator/management/ -q
python3 -m pytest orchestrator/gateway/ -q
ruff check orchestrator/
```

From `orchestrator/management/ui` with Node 24 and dependencies installed using
`npm ci`:

```sh
npm test
npm run lint
npm run build
npm run test:e2e
```

Playwright checks the interaction view at desktop and mobile widths using
explicit test fixtures (not captured production activity).
