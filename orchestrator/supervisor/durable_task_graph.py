"""LangGraph boundary for the DB-authoritative durable task workflow."""

import asyncio
import json
import os
import time
from typing import TypedDict

from langgraph.graph import END, START, StateGraph
from task_runtime import (
    failure_feedback,
    record_task_event,
    requeue_failed_task,
    review_topics,
    safe_judge_review,
)

WORKFLOW_SCHEMA_VERSION = 1


class DurableTaskState(TypedDict, total=False):
    task_id: str
    attempt: int
    schema_version: int
    accepted: bool
    terminal: bool


class DurableTaskFailure(RuntimeError):
    def __init__(self, stage, error):
        super().__init__(str(error))
        self.stage = stage
        self.error = error


def _guarded(stage, operation):
    async def run(state, config):
        try:
            return await operation(state, config)
        except DurableTaskFailure:
            raise
        except Exception as error:
            raise DurableTaskFailure(stage, error) from error

    return run


class DurableTaskWorkflow:
    """Run actual task operations while keeping checkpoint state non-authoritative."""

    def __init__(self, supervisor, db):
        self.supervisor = supervisor
        self.db = db

    @staticmethod
    def _owner(config):
        return config["configurable"]["claim_owner"]

    def _task(self, state, config):
        owner = self._owner(config)
        task = self.db.execute("SELECT * FROM tasks WHERE task_id=?", (state["task_id"],)).fetchone()
        if (task is None or task["status"] != "running" or task["claim_owner"] != owner
                or task["attempts"] != state["attempt"]):
            raise RuntimeError("task-claim-lost")
        return task

    def _runtime(self, task):
        _, runtime = self.supervisor._load_pinned_catalog(task, self.db)
        return runtime

    def _event(self, state, config, actor, action, outcome, **values):
        record_task_event(self.db, state["task_id"], state["attempt"], actor, action, outcome,
                          claim_owner=self._owner(config), claim_attempt=state["attempt"], **values)

    def _has_event(self, state, actor, action, outcome):
        return self.db.execute("""SELECT 1 FROM task_events WHERE task_id=? AND attempt=?
            AND actor=? AND action=? AND outcome=? LIMIT 1""",
            (state["task_id"], state["attempt"], actor, action, outcome)).fetchone() is not None

    async def plan(self, state, config):
        supervisor = self.supervisor
        task = self._task(state, config)
        if json.loads(task["payload"]).get("repository_auth") != "github-app":
            raise RuntimeError("task-authentication-unsupported")
        catalog, runtime = supervisor._load_pinned_catalog(task, self.db)
        if task["attempts"] >= supervisor.MAX_TOTAL_ATTEMPTS and not task["result"]:
            self._event(state, config, "supervisor", "task", "failed", reason="max-total-attempts-exhausted")
            supervisor._complete_via_gateway(task["task_id"], "failed", runtime,
                                             claim_owner=self._owner(config), attempt=state["attempt"],
                                             error="max-total-attempts-exhausted")
            return {"terminal": True}
        if not task["plan"]:
            plan = await supervisor._plan_task(task, catalog, runtime)
            updated = self.db.execute("""UPDATE tasks SET plan=? WHERE task_id=? AND status='running'
                AND claim_owner=? AND attempts=?""",
                (plan, task["task_id"], self._owner(config), state["attempt"]))
            if updated.rowcount != 1:
                raise RuntimeError("task-claim-lost")
            self._event(state, config, "supervisor", "plan", "recorded")
        else:
            self._event(state, config, "supervisor", "plan", "reused")
        return state

    async def dispatch(self, state, config):
        supervisor = self.supervisor
        task = self._task(state, config)
        if task["result"]:
            return state
        if task["job"]:
            return state
        runtime = self._runtime(task)
        endpoints = runtime["worker_inference_endpoints"]
        endpoint_index = supervisor._allocate_worker_endpoint(
            self.db, task["task_id"], self._owner(config), len(endpoints))
        runtime = dict(runtime)
        runtime["coder_url"] = endpoints[endpoint_index]["url"]
        job_name = supervisor._dispatch_task(task, runtime)
        updated = self.db.execute("""UPDATE tasks SET job=? WHERE task_id=? AND status='running'
            AND claim_owner=? AND attempts=?""",
            (json.dumps({"name": job_name}), task["task_id"], self._owner(config), state["attempt"]))
        if updated.rowcount != 1:
            raise RuntimeError("task-claim-lost")
        self._event(state, config, "supervisor", "dispatch", "dispatched")
        return state

    async def candidate(self, state, config):
        deadline = time.monotonic() + int(os.environ.get("TASK_RESULT_TIMEOUT_SECONDS", "1800"))
        while time.monotonic() < deadline:
            task = self._task(state, config)
            if task["result"]:
                candidate = json.loads(task["result"])
                if candidate.get("status") != "candidate":
                    raise RuntimeError(candidate.get("error", "worker-candidate-failed"))
                candidate_attempt = candidate.get("attempt", task["attempts"])
                if type(candidate_attempt) is not int or candidate_attempt != state["attempt"]:
                    raise RuntimeError("task-attempt-mismatch")
                return state
            updated = self.db.execute("""UPDATE tasks SET claimed_at=? WHERE task_id=? AND status='running'
                AND claim_owner=? AND attempts=?""",
                (time.time(), state["task_id"], self._owner(config), state["attempt"]))
            if updated.rowcount != 1:
                raise RuntimeError("task-claim-lost")
            await asyncio.sleep(float(os.environ.get("TASK_POLL_SECONDS", "5")))
        raise RuntimeError("worker-result-timeout")

    def _retry_rejected(self, state, config, task, score, review):
        supervisor = self.supervisor
        feedback = failure_feedback("judge", "judge-rejected", state["attempt"], review)
        if state["attempt"] >= supervisor.MAX_TOTAL_ATTEMPTS:
            if not self._has_event(state, "supervisor", "task", "failed"):
                self._event(state, config, "supervisor", "task", "failed", reason="judge-rejected",
                            score=score, review_topics=review_topics(review))
            runtime = self._runtime(task)
            supervisor._complete_via_gateway(task["task_id"], "failed", runtime,
                                             claim_owner=self._owner(config), attempt=state["attempt"],
                                             error="judge-rejected-after-eight-attempts", score=score,
                                             review=safe_judge_review(review, (task["payload"],)))
        else:
            if not self._has_event(state, "supervisor", "task", "retrying"):
                self._event(state, config, "supervisor", "task", "retrying", reason="judge-rejected",
                            score=score, review_topics=review_topics(review))
            requeue_failed_task(self.db, task["task_id"], self._owner(config), feedback)
        return {"accepted": False}

    async def judge(self, state, config):
        task = self._task(state, config)
        previous = self.db.execute("""SELECT score,outcome FROM task_events WHERE task_id=? AND attempt=?
            AND actor='judge' AND action='review' ORDER BY event_id DESC LIMIT 1""",
            (state["task_id"], state["attempt"])).fetchone()
        if previous and previous["outcome"] == "accepted":
            return {"accepted": True}
        if previous and previous["outcome"] == "rejected":
            details = self.db.execute("""SELECT failure_details FROM task_events WHERE task_id=? AND attempt=?
                AND actor='judge' AND action='review' ORDER BY event_id DESC LIMIT 1""",
                (state["task_id"], state["attempt"])).fetchone()
            review = ""
            if details and details["failure_details"]:
                review = json.loads(details["failure_details"]).get("summary", "")
            return self._retry_rejected(state, config, task, previous["score"] or 0, review)

        candidate = json.loads(task["result"] or "{}")
        if candidate.get("status") != "candidate":
            raise RuntimeError(candidate.get("error", "worker-candidate-failed"))
        candidate_attempt = candidate.pop("attempt", task["attempts"])
        if type(candidate_attempt) is not int or candidate_attempt != state["attempt"]:
            raise RuntimeError("task-attempt-mismatch")
        candidate = self.supervisor._verified_candidate_patch(task, candidate)
        runtime = self._runtime(task)
        score, review = await self.supervisor._judge_candidate(task, candidate, runtime)
        threshold = float(runtime["threshold"])
        if not 0 <= threshold <= 1:
            raise RuntimeError("acceptance-threshold-invalid")
        accepted = score >= threshold
        summary = safe_judge_review(review, (
            task["payload"], candidate.get("patch", ""),
            *(edit.get("content", "") for edit in candidate.get("edits", []) if isinstance(edit, dict)),
        ))
        self._event(state, config, "judge", "review", "accepted" if accepted else "rejected",
                    reason=None if accepted else "judge-rejected", score=score,
                    review_topics=review_topics(review),
                    failure_details={"stage": "judge", "code": "review-rejected", "summary": summary}
                    if summary and not accepted else None,
                    task_updates={"judge_review": summary or ""} if accepted else None)
        if not accepted:
            return self._retry_rejected(state, config, task, score, review)
        return {"accepted": True}

    async def publish(self, state, config):
        task = self._task(state, config)
        if not self._has_event(state, "judge", "review", "accepted"):
            raise RuntimeError("candidate-not-judge-accepted")
        if task["pr_url"]:
            return state
        if not self._has_event(state, "supervisor", "publish", "started"):
            self._event(state, config, "supervisor", "publish", "started")
        candidate = json.loads(task["result"] or "{}")
        candidate_attempt = candidate.pop("attempt", task["attempts"])
        if type(candidate_attempt) is not int or candidate_attempt != state["attempt"]:
            raise RuntimeError("task-attempt-mismatch")
        runtime = self._runtime(task)
        pr_url = self.supervisor._publish_candidate(task, candidate, runtime)
        self._event(state, config, "supervisor", "publish", "completed",
                    task_updates={"pr_url": pr_url})
        return state

    async def complete(self, state, config):
        task = self._task(state, config)
        if not task["pr_url"]:
            raise RuntimeError("pr-publication-unverified")
        review_event = self.db.execute("""SELECT score FROM task_events WHERE task_id=? AND attempt=?
            AND actor='judge' AND action='review' AND outcome='accepted' ORDER BY event_id DESC LIMIT 1""",
            (state["task_id"], state["attempt"])).fetchone()
        runtime = self._runtime(task)
        self.supervisor._complete_via_gateway(
            task["task_id"], "completed", runtime, claim_owner=self._owner(config),
            attempt=state["attempt"], pr_url=task["pr_url"], score=review_event["score"],
            review=task["judge_review"], config_version=task["config_version"],
            config_hash=task["config_hash"],
        )
        return state


def build_durable_task_graph(workflow, checkpointer):
    """Compile the real claimed-task stages with a persistent checkpointer."""
    graph = StateGraph(DurableTaskState)
    graph.add_node("plan", _guarded("planner", workflow.plan))
    graph.add_node("dispatch", _guarded("worker", workflow.dispatch))
    graph.add_node("candidate", _guarded("worker", workflow.candidate))
    graph.add_node("judge", _guarded("judge", workflow.judge))
    graph.add_node("publish", _guarded("publish", workflow.publish))
    graph.add_node("complete", _guarded("supervisor", workflow.complete))
    graph.add_edge(START, "plan")
    graph.add_conditional_edges(
        "plan",
        lambda state: END if state.get("terminal") else "dispatch",
        {"dispatch": "dispatch", END: END},
    )
    graph.add_edge("dispatch", "candidate")
    graph.add_edge("candidate", "judge")
    graph.add_conditional_edges(
        "judge",
        lambda state: "publish" if state.get("accepted") else END,
        {"publish": "publish", END: END},
    )
    graph.add_edge("publish", "complete")
    graph.add_edge("complete", END)
    return graph.compile(checkpointer=checkpointer)