"""Resident durable task polling and supervisor workflow coordination."""

import asyncio
import json
import threading
import time
import uuid
import weakref

from durable_task_graph import (
    WORKFLOW_SCHEMA_VERSION,
    DurableTaskFailure,
    DurableTaskWorkflow,
    build_durable_task_graph,
)
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from task_runtime import (
    failure_feedback,
    record_task_event,
    requeue_failed_task,
)

_checkpoint_setup_locks = weakref.WeakKeyDictionary()
_checkpoint_setup_locks_guard = threading.Lock()


def _checkpoint_setup_lock():
    loop = asyncio.get_running_loop()
    with _checkpoint_setup_locks_guard:
        lock = _checkpoint_setup_locks.get(loop)
        if lock is None:
            lock = asyncio.Lock()
            _checkpoint_setup_locks[loop] = lock
        return lock


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
        if state.get("best_output"):
            supervisor.BEST_SCORE.set(state["best_output"].get("score", 0.0))
        supervisor.SUPERVISOR_STATE.set(self._state_to_value(status))
        supervisor.CURRENT_ITERATION.set(state.get("iteration", 1))
        supervisor.logger.info(
            f"[{state['task_id']}] Metrics: status={status} duration={duration:.2f}s "
            f"best_score={state['best_output']['score'] if state.get('best_output') else 'n/a'}")

    async def _handle_task_failure(self, db, task, owner, stage, error):
        supervisor = self.supervisor
        task_id = task["task_id"]
        current = db.execute("SELECT status,claim_owner,attempts FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if (current is None or current["status"] != "running" or current["claim_owner"] != owner
                or current["attempts"] != task["attempts"]):
            return
        reason = str(error).split(":", 1)[0]
        feedback = failure_feedback(stage, reason, task["attempts"])
        safe_feedback = json.loads(feedback)
        terminal_errors = {
            "pr-policy-required", "pr-base-branch-not-allowed", "pr-reviewer-policy-invalid",
            "pr-publication-credential-required", "task-authentication-unsupported", "judge-credential-required",
            "planner-credential-required", "planner-output-invalid", "candidate-publish-data-missing",
            "candidate-edit-policy-violation", "task-config-pin-invalid", "task-catalog-hash-mismatch",
            "task-catalog-schema-invalid", "task-catalog-version-unsupported", "task-catalog-role-set-invalid",
            "task-catalog-role-invalid", "task-catalog-capability-boundary-invalid",
        }
        terminal = task["attempts"] >= supervisor.MAX_TOTAL_ATTEMPTS or reason in terminal_errors
        actor = stage if stage in ("worker", "judge") else "supervisor"
        record_task_event(db, task_id, task["attempts"], actor, "task",
                          "failed" if terminal else "retrying", reason=safe_feedback["reason"],
                          claim_owner=owner, claim_attempt=task["attempts"])
        if terminal:
            db.execute("UPDATE tasks SET retry_feedback=? WHERE task_id=? AND claim_owner=? AND attempts=?",
                       (feedback, task_id, owner, task["attempts"]))
            try:
                supervisor._complete_via_gateway(task_id, "failed", None, claim_owner=owner,
                                                 attempt=task["attempts"], error=str(error)[:1000])
            except Exception:
                supervisor.logger.exception("could not persist terminal failure task=%s", task_id)
        else:
            accepted = db.execute("""SELECT 1 FROM task_events WHERE task_id=? AND attempt=?
                AND actor='judge' AND action='review' AND outcome='accepted' LIMIT 1""",
                (task_id, task["attempts"])).fetchone()
            if stage == "publish" or (current["status"] == "running" and accepted):
                db.execute("UPDATE tasks SET claim_owner=NULL,claimed_at=? WHERE task_id=? AND claim_owner=?",
                           (time.time(), task_id, owner))
            else:
                requeue_failed_task(db, task_id, owner, feedback)

    @staticmethod
    def _state_to_value(status):
        mapping = {"idle": 0.0, "planning": 1.0, "executing": 2.0, "evaluating": 3.0,
                   "complete": 4.0, "failed": -1.0, "error": -2.0}
        return mapping.get(status, -9.0)

    async def run_task(self, task):
        supervisor = self.supervisor
        start_time = asyncio.get_running_loop().time()
        supervisor.ACTIVE_TASKS.inc()
        supervisor.SUPERVISOR_STATE.set(self._state_to_value("planning"))
        try:
            result = await self.graph.ainvoke(task)
            self._record_task_metrics(result, start_time)
            return result
        finally:
            supervisor.ACTIVE_TASKS.dec()

    async def process_next_durable_task(self):
        supervisor = self.supervisor
        instance_id = supervisor.os.environ.get("SUPERVISOR_INSTANCE_ID", "supervisor")
        owner = f"{instance_id}:{uuid.uuid4()}"
        max_active_tasks = int(supervisor.os.getenv("SUPERVISOR_WORKER_PARALLELISM", "1"))
        if not 1 <= max_active_tasks <= 2:
            raise ValueError("worker-parallelism-invalid")
        db = supervisor.task_database()
        task = supervisor.claim_queued_task(db, owner, max_active_tasks=max_active_tasks)
        if task is None:
            db.close()
            return False
        task_id = task["task_id"]
        started_at = time.monotonic()
        supervisor.ACTIVE_TASKS.inc()
        try:
            record_task_event(db, task_id, task["attempts"] or 1, "supervisor", "claim", "started",
                              claim_owner=owner, claim_attempt=task["attempts"])
            config = {"configurable": {
                "thread_id": task_id,
                "claim_owner": owner,
            }}
            checkpoint_path = supervisor.checkpoint_database_path(
                db, task["attempts"], WORKFLOW_SCHEMA_VERSION)
            async with AsyncSqliteSaver.from_conn_string(checkpoint_path) as checkpointer:
                async with _checkpoint_setup_lock():
                    await checkpointer.aget_tuple(config)
                graph = build_durable_task_graph(DurableTaskWorkflow(supervisor, db), checkpointer)
                await graph.ainvoke({"task_id": task_id, "attempt": task["attempts"],
                                    "schema_version": WORKFLOW_SCHEMA_VERSION}, config)
        except DurableTaskFailure as stage_error:
            await self._handle_task_failure(db, task, owner, stage_error.stage, stage_error.error)
        except Exception:
            supervisor.logger.exception("durable task failed task=%s", task_id)
            current = db.execute("SELECT status,claim_owner FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if current and current["status"] == "running" and current["claim_owner"] == owner:
                record_task_event(db, task_id, task["attempts"], "supervisor", "task", "retrying",
                                  reason="checkpoint-store-unavailable", claim_owner=owner,
                                  claim_attempt=task["attempts"])
                db.execute("UPDATE tasks SET claim_owner=NULL,claimed_at=? WHERE task_id=? AND claim_owner=?",
                           (time.time(), task_id, owner))
        finally:
            try:
                duration = max(0.0, time.monotonic() - started_at)
                task_role = task["role"] if task["role"] in ("coder", "doc") else "coder"
                supervisor.AGENT_TASK_ATTEMPT_DURATION.labels(role=task_role).observe(duration)
                current = db.execute("SELECT status FROM tasks WHERE task_id=?", (task_id,)).fetchone()
                if current and current["status"] in ("completed", "failed"):
                    outcome = "success" if current["status"] == "completed" else "failure"
                    supervisor.AGENT_TASKS.labels(role=task_role, outcome=outcome).inc()
            finally:
                supervisor.ACTIVE_TASKS.dec()
                db.close()
        return True

    async def run_forever(self):
        supervisor = self.supervisor
        interval_seconds = int(supervisor.os.getenv("SUPERVISOR_IDLE_SLEEP_SECONDS", "15"))
        parallelism = int(supervisor.os.getenv("SUPERVISOR_WORKER_PARALLELISM", "1"))
        if not 1 <= parallelism <= 2:
            raise ValueError("worker-parallelism-invalid")
        supervisor.SUPERVISOR_STATE.set(self._state_to_value("idle"))
        supervisor.logger.info("Supervisor durable task polling started.")
        active_tasks = set()
        try:
            while True:
                while len(active_tasks) < parallelism:
                    task = asyncio.create_task(self.process_next_durable_task())
                    active_tasks.add(task)
                    await asyncio.sleep(0)
                    if task.done():
                        active_tasks.remove(task)
                        try:
                            did_work = task.result()
                        except Exception:
                            supervisor.logger.exception("durable task poll failed")
                            did_work = False
                        if not did_work:
                            break
                if active_tasks:
                    completed, _ = await asyncio.wait(active_tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in completed:
                        active_tasks.remove(task)
                        try:
                            task.result()
                        except Exception:
                            supervisor.logger.exception("durable task poll failed")
                else:
                    await asyncio.sleep(interval_seconds)
        finally:
            for task in active_tasks:
                task.cancel()
            if active_tasks:
                await asyncio.gather(*active_tasks, return_exceptions=True)