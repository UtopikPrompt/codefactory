# Orchestrator Supervisor Image

Orchestrator image for the autonomous LangGraph supervisor loop.

## Files
- `Dockerfile` — supervisor image build definition
- `requirements.txt` — Python dependencies
- `supervisor.py` — resident supervisor loop entry point
- `task_runtime.py` — durable task claiming, catalog validation, and worker-job dispatch
- `candidate_workflow.py` — candidate verification, judge/planner calls, PR publication, and gateway completion
- `task_graph.py` — LangGraph state and planning/evaluation transitions
- `supervisor_runtime.py` — resident polling loop and workflow coordination

## Build
```bash
docker build -f orchestrator/supervisor/Dockerfile \
	-t ghcr.io/utopikprompt/codefactory-supervisor:$TAG \
	orchestrator/supervisor
```

## Runtime behavior
- Container entrypoint is `python supervisor.py`.
- Process is resident (always-on) and serves:
	- `GET /healthz` on port `9000`
	- `GET /metrics` on port `9000`
- The supervisor remains idle between tasks and exposes Prometheus metrics for
	external scraping.

## Candidate filtering and retry feedback
- The coder is asked to return only changed files. Every proposed edit is checked
  for shape, duplicate paths, path/policy safety and assigned secrets before any
  no-op is filtered or file is written. The existing `original.strip() ==
  replacement.strip()` no-op policy is retained: boundary-whitespace-only edits
  are omitted, not applied. Empty proposals and entirely unchanged batches fail
  explicitly. Mixed batches retain genuine edits and still require sandbox
  validation, supervisor patch reconstruction and judge acceptance.
- No-op diagnostics contain filenames and metadata only; indices and counts refer
  to the original proposal, while candidate edits/changed_files/patch contain only
  retained changes.
- Both SQLite owners migrate the nullable `tasks.retry_feedback` column. Requeue
  atomically saves the last failure feedback when clearing the worker result.
  Feedback survives claims, lease recovery, database reopen and manual retry
  lineage, separately from the original user payload, cached plan and config pin.
  Dispatch passes it alongside the reused plan to the next coder generation.
- Feedback is deliberately lossy and bounded (under 512 bytes): stage, attempt,
  an allowlisted failure code, and, for judge rejection, closed-vocabulary review
  topics (tests, correctness, security, scope, patch-integrity). Unknown exceptions
  become `task-execution-failed`; no raw review, traceback, prompt, source or
  credential is copied into feedback. The attempt distinguishes repeated failures
  without changing retry limits. Feedback and plans are untrusted generation data,
  never privileged instructions. This does not guarantee a model will correct an
  error or choose different edits.
- Candidates scoring from the acceptance threshold up to the configured
  `judge-review-margin` receive a second critical review from the existing judge;
  the lower score governs acceptance. The default margin is `0.1`, and `0` disables
  this extra inference pass. Stronger approvals keep the single-review path.
