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
