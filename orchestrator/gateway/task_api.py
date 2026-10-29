"""Task intake, callback, retry, and status operations for the gateway."""

import json
import re
import time

EVENT_REASONS = {
    "no-op-edit", "empty-or-invalid-edits", "invalid-generation-output", "invalid-generated-edit",
    "edit-outside-policy", "unsafe-edit-path", "protected-edit-path", "secret-in-model-context",
    "edits-not-applied", "empty-patch", "validation-failed", "validation-output-too-large",
    "coder-http-error", "worker-result-timeout", "worker-candidate-failed", "candidate-patch-edits-mismatch",
    "candidate-edit-set-mismatch", "candidate-validation-not-passed", "task-execution-failed",
    "max-total-attempts-exhausted", "planner-credential-required", "planner-output-invalid",
    "judge-credential-required", "judge-score-invalid", "task-authentication-unsupported",
    "task-catalog-hash-mismatch", "task-catalog-schema-invalid", "task-catalog-version-unsupported",
    "task-catalog-role-set-invalid", "task-catalog-role-invalid", "task-catalog-capability-boundary-invalid",
    "pr-policy-required", "pr-base-branch-not-allowed", "pr-reviewer-policy-invalid",
    "pr-publication-credential-required", "candidate-publish-data-missing", "base-branch-changed",
    "existing-task-branch-conflict", "existing-pr-policy-conflict", "pr-creation-unverified", "pr-identity-invalid",
    "judge-rejected",
}
EVENT_TOPICS = ("tests", "correctness", "security", "scope", "patch-integrity")
FAILURE_DETAIL_CODES = {
    "validation": {"nonzero-exit"},
    "execution": {"timeout", "os-error", "json-error", "http-error", "unexpected"},
    "judge": {"review-rejected"},
}


def safe_failure_details(details):
    if not isinstance(details, dict):
        return None
    stage, code = details.get("stage"), details.get("code")
    if not isinstance(stage, str) or not isinstance(code, str) or stage not in FAILURE_DETAIL_CODES or code not in FAILURE_DETAIL_CODES[stage]:
        return None
    if stage == "judge":
        summary = details.get("summary")
        if (not isinstance(summary, str) or not 1 <= len(summary) <= 500
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 ,.;:!?()'-]*", summary)
                or re.search(r"https?://|bearer|api[-_ ]?key|token|password|secret|credential|private",
                             summary, re.IGNORECASE)
                or re.search(r"[A-Z0-9]{32,}", summary)):
            return None
        return {"stage": "judge", "code": "review-rejected", "summary": summary}
    safe = {"stage": stage, "code": code}
    for key, maximum in (("exit_status", 255), ("duration_ms", 600000), ("output_bytes", 65536)):
        value = details.get(key)
        minimum = -maximum if key == "exit_status" else 0
        if type(value) is int and minimum <= value <= maximum:
            safe[key] = value
    return safe


def append_event(db, task_id, attempt, actor, action, outcome, *, reason=None, validation_status=None, score=None,
                 review_topics=(), failure_details=None):
    safe_reason = reason if reason in EVENT_REASONS else None
    safe_validation = validation_status if validation_status in ("passed", "failed", "not-run") else None
    safe_score = score if type(score) in (int, float) and 0 <= score <= 1 else None
    topics = [topic for topic in EVENT_TOPICS if topic in review_topics]
    safe_details = safe_failure_details(failure_details)
    if (safe_details and safe_details.get("stage") == "judge"
            and (actor != "judge" or action != "review" or outcome != "rejected" or reason != "judge-rejected")):
        safe_details = None
    db.execute("""INSERT INTO task_events(task_id,created_at,attempt,actor,action,outcome,reason,
        validation_status,score,review_topics,failure_details) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        (task_id, time.time(), max(1, min(8, int(attempt))), actor, action, outcome, safe_reason,
         safe_validation, safe_score, json.dumps(topics), json.dumps(safe_details, sort_keys=True) if safe_details else None))
    db.execute("""DELETE FROM task_events WHERE task_id=? AND event_id NOT IN
        (SELECT event_id FROM task_events WHERE task_id=? ORDER BY event_id DESC LIMIT 128)""", (task_id, task_id))


def operational_events(db, task_id):
    events = []
    for row in db.execute("SELECT * FROM task_events WHERE task_id=? ORDER BY event_id", (task_id,)):
        try:
            topics = json.loads(row["review_topics"] or "[]")
        except (ValueError, TypeError):
            topics = []
        try:
            failure_details = safe_failure_details(json.loads(row["failure_details"] or "null"))
        except (ValueError, TypeError):
            failure_details = None
        safe_details = failure_details
        if (safe_details and safe_details.get("stage") == "judge"
            and (row["actor"] != "judge" or row["action"] != "review" or row["outcome"] != "rejected"
                 or row["reason"] != "judge-rejected")):
            safe_details = None
        events.append({
            "timestamp": row["created_at"], "attempt": row["attempt"] if type(row["attempt"]) is int and 1 <= row["attempt"] <= 8 else None,
            "actor": row["actor"] if row["actor"] in ("supervisor", "worker", "judge") else "supervisor",
            "action": row["action"] if row["action"] in ("claim", "plan", "dispatch", "candidate", "review", "publish", "task") else "task",
            "outcome": row["outcome"] if row["outcome"] in ("started", "recorded", "reused", "dispatched", "returned", "failed", "accepted", "rejected", "retrying", "completed") else "recorded",
            "reason": row["reason"] if row["reason"] in EVENT_REASONS else None,
            "validation_status": row["validation_status"] if row["validation_status"] in ("passed", "failed", "not-run") else None,
            "score": row["score"] if type(row["score"]) in (int, float) and 0 <= row["score"] <= 1 else None,
            "review_topics": [topic for topic in EVENT_TOPICS if isinstance(topics, list) and topic in topics],
            "failure_details": safe_details,
        })
    return events


def coordination_summary(task):
    """Expose bounded facts, never plan text or free-form retry/model feedback."""
    feedback = None
    try:
        stored = json.loads(task["retry_feedback"] or "null")
    except (ValueError, TypeError):
        stored = None
    if (isinstance(stored, dict) and stored.get("stage") in ("supervisor", "worker", "judge")
            and type(stored.get("attempt")) is int and 1 <= stored["attempt"] <= 8):
        topics = stored.get("review_topics")
        feedback = {
            "stage": stored["stage"], "attempt": stored["attempt"],
            "review_topics": [topic for topic in ("tests", "correctness", "security", "scope", "patch-integrity")
                              if isinstance(topics, list) and topic in topics],
        }
    return {"plan_recorded": bool(task["plan"]), "latest_feedback": feedback}


class TaskAPI:
    def __init__(self, handler, gateway):
        self.handler = handler
        self.gateway = gateway

    def list_tasks(self):
        gateway, handler = self.gateway, self.handler
        query = gateway.parse_qs(gateway.urlsplit(handler.path).query, keep_blank_values=True)
        try:
            if set(query) - {"repo", "status", "limit", "offset"} or any(len(values) != 1 for values in query.values()):
                raise ValueError("invalid-query")
            limit = int(query.get("limit", ["50"])[0])
            offset = int(query.get("offset", ["0"])[0])
            if not 1 <= limit <= 100 or not 0 <= offset <= 1000000:
                raise ValueError("invalid-page")
            clauses, parameters = [], []
            if "repo" in query:
                repo = query["repo"][0]
                gateway._identity({"repo": repo})
                clauses.append("lower(repo)=lower(?)")
                parameters.append(repo)
            if "status" in query:
                status = query["status"][0]
                if status not in ("queued", "running", "completed", "failed"):
                    raise ValueError("invalid-status")
                clauses.append("status=?")
                parameters.append(status)
        except (ValueError, TypeError):
            handler._json(400, {"error": "invalid-task-query"})
            return
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        db = gateway._task_db()
        try:
            total = db.execute("SELECT COUNT(*) FROM tasks" + where, parameters).fetchone()[0]
            rows = db.execute(
                "SELECT task_id, repo, issue AS issue_number, status, lifecycle, credential_cleanup, "
                "retry_of, attempts, config_version, config_hash FROM tasks" + where +
                " ORDER BY rowid DESC LIMIT ? OFFSET ?", [*parameters, limit, offset]).fetchall()
            handler._json(200, {"tasks": [dict(row) for row in rows], "total": total,
                                "limit": limit, "offset": offset})
        finally:
            db.close()

    def lookup(self, db):
        gateway = self.gateway
        parsed = gateway.urlsplit(self.handler.path)
        task_id = parsed.path.removeprefix("/api/v1/tasks/").removesuffix("/sync").removesuffix("/retry")
        query = gateway.parse_qs(parsed.query)
        if not gateway.re.fullmatch(r"[A-Za-z0-9_.-]{1,63}", task_id):
            raise ValueError("invalid-task-id")
        repo = query.get("repo", [None])[0]
        task = db.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if task is None and task_id.isdigit() and repo:
            task = db.execute("SELECT * FROM tasks WHERE lower(repo)=lower(?) AND issue=?", (repo, int(task_id))).fetchone()
        if task and repo and repo.lower() != (task["repo"] or "").lower():
            raise PermissionError("repository-mismatch")
        return task

    def get_task(self):
        gateway, handler = self.gateway, self.handler
        db = gateway._task_db()
        try:
            task = self.lookup(db)
            if task is None:
                handler._json(404, {"error": "task-not-found"})
                return
            handler._json(200, {
                "task_id": task["task_id"], "repo": task["repo"], "issue_number": task["issue"],
                "status": task["status"], "result": gateway.json.loads(task["result"]) if task["result"] else None,
                "lifecycle": task["lifecycle"], "credential_cleanup": task["credential_cleanup"],
                "retry_of": task["retry_of"], "attempts": task["attempts"],
                "config_version": task["config_version"], "config_hash": task["config_hash"],
                "retry_task_id": next((row[0] for row in db.execute(
                    "SELECT task_id FROM tasks WHERE retry_of=?", (task["task_id"],))), None),
                "job": gateway.json.loads(task["job"]) if task["job"] else None,
                "coordination": coordination_summary(task),
                "events": operational_events(db, task["task_id"]),
            })
        except PermissionError:
            handler._json(403, {"error": "repository-mismatch"})
        except ValueError:
            handler._json(400, {"error": "invalid-task-id"})
        finally:
            db.close()

    def retry(self, payload):
        gateway, handler = self.gateway, self.handler
        gateway._reject_credentials(payload)
        if set(payload) != {"retry_key"} or not isinstance(payload["retry_key"], str) or not gateway.re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", payload["retry_key"]
        ):
            raise ValueError("invalid-retry-request")
        db = gateway._task_db()
        try:
            task = self.lookup(db)
            if task is None:
                handler._json(404, {"error": "task-not-found"})
                return
            original = gateway.json.loads(task["payload"])
        finally:
            db.close()
        if handler.actions_repo and handler.actions_repo.lower() != (task["repo"] or "").lower():
            raise PermissionError("repository-mismatch")
        if original.get("repository_auth") != "github-app":
            handler._json(403, {"error": "task-authentication-unsupported"})
            return
        original.pop("task_id", None)
        original.pop("repository_auth")
        self.spawn(original, retry_of=task["task_id"], retry_key=payload["retry_key"])

    def spawn(self, payload, retry_of=None, retry_key=None):
        gateway, handler = self.gateway, self.handler
        gateway._reject_credentials(payload, handler.headers.get("Authorization", "").removeprefix("Bearer "))
        if "repository_auth" in payload:
            raise ValueError("repository-authentication-assigned-by-gateway")
        repo, issue = gateway._identity(payload)
        if "worker_id" in payload:
            raise ValueError("gateway-assignment-required")
        if gateway._is_external_workflow_payload(payload) and issue is None:
            raise ValueError("issue-required")
        if "task_id" in payload:
            gateway._validate_assigned_task_id(payload["task_id"])
            if issue is not None and str(payload["task_id"]) != str(issue):
                raise ValueError("issue-mismatch")
        if not handler._transport_allowed():
            raise gateway.CredentialError("transport-not-allowed")
        policy = gateway._github_app().policy(repo)
        if handler.actions_repo and handler.actions_repo.lower() != policy["repository"].lower():
            raise PermissionError("repository-mismatch")
        if not handler.actions_repo:
            user_token = handler.headers.get("X-GitHub-User-Token", "")
            gateway._reject_credentials(payload, user_token)
            gateway._github_app().authorize_user(repo, user_token)
        token = gateway._github_app().mint(repo)["token"]
        gateway._reject_credentials(payload, token)
        gateway._validate_repository(repo, issue, token)
        task_id = "issue-" + gateway.hashlib.sha256(f"{repo.lower()}#{issue}".encode()).hexdigest()[:32] if issue else gateway.uuid.uuid4().hex
        if retry_of:
            task_id = "issue-" + gateway.hashlib.sha256(f"retry:{retry_of}:{retry_key}".encode()).hexdigest()[:32]
        assigned = gateway.copy.deepcopy(payload)
        assigned["task_id"] = task_id
        assigned["repository_auth"] = "github-app"
        if repo:
            assigned["repo"] = repo
        db = gateway._task_db()
        try:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if old:
                db.commit()
                code = 409 if old["status"] == "failed" else 202
                handler._json(code, {"task_id": task_id, "repo": repo, "status": old["status"], "replayed": True})
                return
            if retry_of:
                parent = db.execute("SELECT * FROM tasks WHERE task_id=?", (retry_of,)).fetchone()
                child = db.execute("SELECT task_id FROM tasks WHERE retry_of=?", (retry_of,)).fetchone()
                if child:
                    db.commit()
                    handler._json(409, {"error": "retry-already-created", "task_id": child["task_id"]})
                    return
                if not parent or parent["status"] != "failed" or not parent["result"]:
                    db.commit()
                    handler._json(409, {"error": "retry-requires-failed-task"})
                    return
                settled = {"synced", "credential-missing", "credential-expired", "credential-denied",
                           "credential-binding-mismatch", "credential-invalid"}
                if parent["lifecycle"] not in settled or parent["credential_cleanup"] != "deleted":
                    db.commit()
                    handler._json(409, {"error": "retry-requires-lifecycle-sync"})
                    return
                active = db.execute("SELECT task_id FROM tasks WHERE lower(repo)=lower(?) AND issue=? AND status IN ('queued','running')",
                                    (repo, issue)).fetchone()
                if active:
                    db.commit()
                    handler._json(409, {"error": "issue-task-active", "task_id": active["task_id"]})
                    return
            callback = gateway.secrets.token_urlsafe(32)
            credential_ref = "task-credential-" + task_id
            config_version = gateway.os.environ.get("AGENT_CONFIG_VERSION", "1")
            config_hash = gateway.os.environ.get("AGENT_CONFIG_HASH") or gateway.hashlib.sha256(config_version.encode()).hexdigest()
            db.execute("INSERT INTO tasks(task_id,repo,issue,status,payload,credential_ref,callback_hash,retry_of,retry_key,config_version,config_hash,retry_feedback) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                       (task_id, repo, issue, "queued", gateway.json.dumps(assigned), credential_ref, gateway.hashlib.sha256(callback.encode()).hexdigest(),
                        retry_of, retry_key, config_version, config_hash, parent["retry_feedback"] if retry_of else None))
            db.commit()
            try:
                credential_uid = gateway._create_task_secret(task_id, repo, callback)
                db.execute("UPDATE tasks SET credential_uid=? WHERE task_id=?", (credential_uid, task_id))
            except gateway.KubernetesError:
                failure = gateway.json.dumps({"task_id": task_id, "repo": repo, "issue_number": issue,
                                              "status": "failed", "error": "credential-create-failed"})
                db.execute("UPDATE tasks SET status='failed', result=? WHERE task_id=?", (failure, task_id))
                gateway._retry_lifecycle(task_id)
                handler._json(503, {"task_id": task_id, "error": "credential-create-failed"})
                return
            gateway.ACCEPTED_TASKS.labels(kind="retry" if retry_of else "initial").inc()
            handler._json(202, {"task_id": task_id, "repo": repo, "status": "queued"})
        finally:
            db.close()

    def credentials(self, payload, supervisor=False):
        gateway, handler = self.gateway, self.handler
        if payload:
            raise ValueError("credential-request-invalid")
        if supervisor and not handler._supervisor_authorized():
            handler._json(401, {"error": "supervisor-unauthorized"})
            return
        prefix = "/api/v1/supervisor/tasks/" if supervisor else "/api/v1/tasks/"
        task_id = gateway.urlsplit(handler.path).path.removeprefix(prefix).removesuffix("/credentials")
        if not gateway.re.fullmatch(r"[A-Za-z0-9_.-]{1,63}", task_id):
            raise ValueError("invalid-task-id")
        db = gateway._task_db()
        try:
            db.execute("BEGIN IMMEDIATE")
            task = db.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            callback = handler.headers.get("X-Task-Token", "")
            if not supervisor and (not task or not callback or not task["callback_hash"] or not gateway.hmac.compare_digest(
                    gateway.hashlib.sha256(callback.encode()).hexdigest(), task["callback_hash"])):
                raise PermissionError("task-callback-denied")
            if (not task or task["status"] not in ("queued", "running")
                    or gateway.json.loads(task["payload"]).get("repository_auth") != "github-app"
                    or not task["credential_uid"] or task["credential_cleanup"] != "retained"):
                raise PermissionError("task-credential-unavailable")
            credential = gateway._github_app().mint(task["repo"], write=supervisor)
            handler._json(200, credential)
        finally:
            db.rollback()
            db.close()

    def resync(self, payload):
        gateway, handler = self.gateway, self.handler
        if payload:
            raise ValueError("sync-body-must-be-empty")
        db = gateway._task_db()
        try:
            task = self.lookup(db)
            if task is None:
                handler._json(404, {"error": "task-not-found"})
                return
            if task["status"] not in ("completed", "failed") or not task["result"]:
                handler._json(409, {"error": "task-not-terminal"})
                return
        finally:
            db.close()
        lifecycle = gateway._retry_lifecycle(task["task_id"])
        handler._json(200, {"task_id": task["task_id"], "status": task["status"], "lifecycle": lifecycle})

    def result(self, payload):
        gateway, handler = self.gateway, self.handler
        gateway._reject_credentials(payload, handler.headers.get("X-Task-Token", ""))
        repo, issue = gateway._identity(payload)
        db = gateway._task_db()
        try:
            db.execute("BEGIN IMMEDIATE")
            task = self.lookup(db)
            if task is None:
                db.commit()
                handler._json(404, {"error": "task-not-found"})
                return
            callback = handler.headers.get("X-Task-Token", "")
            if not callback or not task["callback_hash"] or not gateway.hmac.compare_digest(gateway.hashlib.sha256(callback.encode()).hexdigest(), task["callback_hash"]):
                raise PermissionError("task-callback-denied")
            legacy_id = gateway.urlsplit(handler.path).path.rsplit("/", 1)[1].isdigit() and repo is not None
            if str(payload.get("task_id")) != (str(task["issue"]) if legacy_id else task["task_id"]):
                raise ValueError("task-id-mismatch")
            if (repo or "").lower() != (task["repo"] or "").lower() or issue != task["issue"]:
                raise ValueError("result-identity-mismatch")
            if payload.get("status") not in ("candidate", "candidate_failed"):
                raise ValueError("candidate-status-required")
            active_attempt = max(1, task["attempts"] or 0)
            reported_attempt = payload.get("attempt")
            if reported_attempt is None and active_attempt == 1:
                reported_attempt = 1
            if type(reported_attempt) is not int or reported_attempt != active_attempt:
                db.commit()
                handler._json(409, {"error": "task-attempt-mismatch"})
                return
            payload = {**payload, "attempt": reported_attempt}
            if "failure_details" in payload:
                failure_details = safe_failure_details(payload.get("failure_details"))
                if failure_details is None:
                    payload.pop("failure_details")
                else:
                    payload["failure_details"] = failure_details
            if task["status"] in ("completed", "failed"):
                db.commit()
                handler._json(409, {"error": "task-already-terminal"})
                return
            if payload["status"] == "candidate" and (payload.get("error") or not payload.get("patch")):
                raise ValueError("empty-candidate")
            serialized = gateway.json.dumps(payload, sort_keys=True)
            if task["result"] and gateway.json.loads(task["result"]) != payload:
                db.commit()
                handler._json(409, {"error": "terminal-result-conflict"})
                return
            db.execute("UPDATE tasks SET result=? WHERE task_id=?", (serialized, task["task_id"]))
            if not task["result"]:
                try:
                    validation = payload.get("validation")
                    validation = validation if isinstance(validation, dict) else {}
                    reason = str(payload.get("error", "")).split(":", 1)[0]
                    append_event(db, task["task_id"], reported_attempt, "worker", "candidate",
                                 "failed" if payload["status"] == "candidate_failed" else "returned",
                                 reason=reason, validation_status=validation.get("status"),
                                 failure_details=payload.get("failure_details"))
                except (TypeError, ValueError):
                    append_event(db, task["task_id"], task["attempts"] or 1, "worker", "candidate", "returned")
            db.commit()
            handler._json(200, {"task_id": task["task_id"], "status": payload["status"]})
        finally:
            db.close()

    def supervisor_result(self, payload):
        gateway, handler = self.gateway, self.handler
        if not handler._supervisor_authorized():
            handler._json(401, {"error": "supervisor-unauthorized"})
            return
        task_id = gateway.urlsplit(handler.path).path.removeprefix("/api/v1/supervisor/tasks/")
        if not gateway.re.fullmatch(r"[A-Za-z0-9_.-]{1,63}", task_id):
            raise ValueError("invalid-task-id")
        if payload.get("task_id") != task_id or payload.get("status") not in ("completed", "failed"):
            raise ValueError("invalid-supervisor-result")
        if payload["status"] == "completed" and not gateway.re.fullmatch(
                r"https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/pull/[1-9][0-9]*",
                payload.get("pr_url", ""), gateway.re.IGNORECASE):
            raise ValueError("human-review-pr-required")
        gateway._reject_credentials(payload)
        db = gateway._task_db()
        try:
            db.execute("BEGIN IMMEDIATE")
            task = db.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if task is None:
                db.commit()
                handler._json(404, {"error": "task-not-found"})
                return
            result = gateway.json.dumps(payload, sort_keys=True)
            if task["status"] in ("completed", "failed") and task["result"] != result:
                db.commit()
                handler._json(409, {"error": "terminal-result-conflict"})
                return
            db.execute("UPDATE tasks SET status=?, result=?, claim_owner=NULL, claimed_at=NULL WHERE task_id=?",
                       (payload["status"], result, task_id))
            db.commit()
            lifecycle = gateway._retry_lifecycle(task_id)
            handler._json(200, {"task_id": task_id, "status": payload["status"], "lifecycle": lifecycle})
        finally:
            db.close()