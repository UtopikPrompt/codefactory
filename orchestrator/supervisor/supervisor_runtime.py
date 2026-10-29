"""Resident durable task polling and supervisor workflow coordination."""

import asyncio
import json
import time
import uuid

from task_runtime import failure_feedback, record_task_event, requeue_failed_task, review_topics


class SupervisorLoop:
    def __init__(self, supervisor):
        if {"PR_GITHUB_TOKEN", "GITHUB_TOKEN", "REPO_GITHUB_PAT", "CODEFACTORY_AUTH_TOKEN",
            "GITHUB_AUTH_MODE", "CODEFACTORY_AUTH_MODE"}.intersection(supervisor.os.environ):
            raise ValueError("repository-authentication-configuration-invalid")
        self.supervisor = supervisor
        self.graph = supervisor.create_supervisor_graph()
        self.logger = supervisor.logger

    def _record_task_metrics(self, state, start_time):
        supervisor = self.supervisor
        duration = max(0.0, asyncio.get_running_loop().time() - start_time)
        supervisor.DURATION.observe(duration)
        status = state.get("status", "unknown")
        supervisor.TASKS.labels(status=status).inc()
        supervisor.ITERATIONS.inc(state.get("iteration", 1))
        supervisor.ACTIVE_TASKS.set(0)
        if state.get("best_output"):
            supervisor.BEST_SCORE.set(state["best_output"].get("score", 0.0))
        supervisor.SUPERVISOR_STATE.set(self._state_to_value(status))
        supervisor.CURRENT_ITERATION.set(state.get("iteration", 1))
        supervisor.logger.info(
            f"[{state['task_id']}] Metrics: status={status} duration={duration:.2f}s "
            f"best_score={state['best_output']['score'] if state.get('best_output') else 'n/a'}")

    @staticmethod
    def _state_to_value(status):
        mapping = {"idle": 0.0, "planning": 1.0, "executing": 2.0, "evaluating": 3.0,
                   "complete": 4.0, "failed": -1.0, "error": -2.0}
        return mapping.get(status, -9.0)

    async def run_task(self, task):
        supervisor = self.supervisor
        start_time = asyncio.get_running_loop().time()
        supervisor.ACTIVE_TASKS.set(1)
        supervisor.SUPERVISOR_STATE.set(self._state_to_value("planning"))
        result = await self.graph.ainvoke(task)
        self._record_task_metrics(result, start_time)
        return result

    async def process_next_durable_task(self):
        supervisor = self.supervisor
        owner = supervisor.os.environ.get("SUPERVISOR_INSTANCE_ID", str(uuid.uuid4()))
        db = supervisor.task_database()
        task = supervisor.claim_queued_task(db, owner)
        if task is None:
            db.close()
            return False
        task_id = task["task_id"]
        record_task_event(db, task_id, task["attempts"] or 1, "supervisor", "claim", "started")
        runtime = None
        failure_stage = "supervisor"
        try:
            if json.loads(task["payload"]).get("repository_auth") != "github-app":
                raise RuntimeError("task-authentication-unsupported")
            catalog, runtime = supervisor._load_pinned_catalog(task)
            if task["attempts"] >= supervisor.MAX_TOTAL_ATTEMPTS and not task["result"]:
                record_task_event(db, task_id, task["attempts"], "supervisor", "task", "failed",
                                  reason="max-total-attempts-exhausted")
                supervisor._complete_via_gateway(task_id, "failed", runtime, error="max-total-attempts-exhausted")
                return True
            if not task["plan"]:
                plan = await supervisor._plan_task(task, catalog, runtime)
                db.execute("UPDATE tasks SET plan=? WHERE task_id=? AND claim_owner=?", (plan, task_id, owner))
                record_task_event(db, task_id, task["attempts"], "supervisor", "plan", "recorded")
                task = db.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            else:
                record_task_event(db, task_id, task["attempts"], "supervisor", "plan", "reused")
            if task["result"]:
                candidate = json.loads(task["result"])
            else:
                job_name = supervisor._dispatch_task(task, runtime)
                db.execute("UPDATE tasks SET job=? WHERE task_id=? AND claim_owner=?",
                           (json.dumps({"name": job_name}), task_id, owner))
                record_task_event(db, task_id, task["attempts"], "supervisor", "dispatch", "dispatched")
                deadline = time.monotonic() + int(supervisor.os.environ.get("TASK_RESULT_TIMEOUT_SECONDS", "1800"))
                candidate = None
                while time.monotonic() < deadline:
                    current = db.execute("SELECT result, claim_owner FROM tasks WHERE task_id=?", (task_id,)).fetchone()
                    if current is None or current["claim_owner"] != owner:
                        raise RuntimeError("task-claim-lost")
                    if current["result"]:
                        candidate = json.loads(current["result"])
                        break
                    await asyncio.sleep(float(supervisor.os.environ.get("TASK_POLL_SECONDS", "5")))
                if candidate is None:
                    raise RuntimeError("worker-result-timeout")
            if candidate.get("status") != "candidate":
                failure_stage = "worker"
                raise RuntimeError(candidate.get("error", "worker-candidate-failed"))
            candidate_attempt = candidate.pop("attempt", task["attempts"])
            if type(candidate_attempt) is not int or candidate_attempt != task["attempts"]:
                failure_stage = "worker"
                raise RuntimeError("task-attempt-mismatch")
            candidate = supervisor._verified_candidate_patch(task, candidate)
            score, review = await supervisor._judge_candidate(task, candidate, runtime)
            threshold = float(runtime["threshold"])
            if not 0 <= threshold <= 1:
                raise RuntimeError("acceptance-threshold-invalid")
            accepted = score >= threshold
            record_task_event(db, task_id, task["attempts"], "judge", "review",
                              "accepted" if accepted else "rejected", reason=None if accepted else "judge-rejected",
                              score=score, review_topics=review_topics(review))
            if score < threshold:
                feedback = failure_feedback("judge", "judge-rejected", task["attempts"], review)
                if task["attempts"] >= supervisor.MAX_TOTAL_ATTEMPTS:
                    record_task_event(db, task_id, task["attempts"], "supervisor", "task", "failed",
                                      reason="judge-rejected", review_topics=review_topics(review))
                    db.execute("UPDATE tasks SET retry_feedback=? WHERE task_id=? AND claim_owner=?",
                               (feedback, task_id, owner))
                    supervisor._complete_via_gateway(task_id, "failed", runtime,
                                                     error="judge-rejected-after-eight-attempts", score=score, review=review)
                else:
                    record_task_event(db, task_id, task["attempts"], "supervisor", "task", "retrying",
                                      reason="judge-rejected", review_topics=review_topics(review))
                    requeue_failed_task(db, task_id, owner, feedback)
                return True
            if json.loads(task["payload"]).get("repository_auth") != "github-app":
                raise RuntimeError("task-authentication-unsupported")
            current = db.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            pr_url = supervisor._publish_candidate(current, candidate, runtime)
            record_task_event(db, task_id, task["attempts"], "supervisor", "publish", "completed")
            supervisor._complete_via_gateway(task_id, "completed", runtime, pr_url=pr_url, score=score, review=review,
                                             config_version=task["config_version"], config_hash=task["config_hash"])
            record_task_event(db, task_id, task["attempts"], "supervisor", "task", "completed")
        except Exception as error:
            supervisor.logger.exception("durable task failed task=%s", task_id)
            terminal_errors = {"pr-policy-required", "pr-base-branch-not-allowed", "pr-reviewer-policy-invalid",
                               "pr-publication-credential-required", "task-authentication-unsupported", "judge-credential-required",
                               "planner-credential-required", "planner-output-invalid",
                               "candidate-publish-data-missing", "candidate-edit-policy-violation",
                               "task-config-pin-invalid", "task-catalog-hash-mismatch",
                               "task-catalog-schema-invalid", "task-catalog-version-unsupported", "task-catalog-role-set-invalid",
                               "task-catalog-role-invalid", "task-catalog-capability-boundary-invalid"}
            reason = str(error).split(":", 1)[0]
            feedback = failure_feedback(failure_stage, reason, task["attempts"])
            safe_feedback = json.loads(feedback)
            terminal = task["attempts"] >= supervisor.MAX_TOTAL_ATTEMPTS or reason in terminal_errors
            record_task_event(db, task_id, task["attempts"], failure_stage, "task",
                              "failed" if terminal else "retrying", reason=safe_feedback["reason"],
                              review_topics=safe_feedback.get("review_topics", []))
            if terminal:
                db.execute("UPDATE tasks SET retry_feedback=? WHERE task_id=? AND claim_owner=?",
                           (feedback, task_id, owner))
                try:
                    supervisor._complete_via_gateway(task_id, "failed", runtime, error=str(error)[:1000])
                except Exception:
                    supervisor.logger.exception("could not persist terminal failure task=%s", task_id)
            else:
                requeue_failed_task(db, task_id, owner, feedback)
        finally:
            db.close()
        return True

    async def run_forever(self):
        supervisor = self.supervisor
        interval_seconds = int(supervisor.os.getenv("SUPERVISOR_IDLE_SLEEP_SECONDS", "15"))
        supervisor.SUPERVISOR_STATE.set(self._state_to_value("idle"))
        supervisor.logger.info("Supervisor durable task polling started.")
        while True:
            try:
                did_work = await self.process_next_durable_task()
            except Exception:
                supervisor.logger.exception("durable task poll failed")
                supervisor.SUPERVISOR_STATE.set(self._state_to_value("idle"))
                did_work = False
            if not did_work:
                await asyncio.sleep(interval_seconds)