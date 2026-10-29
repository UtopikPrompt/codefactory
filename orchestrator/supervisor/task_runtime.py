"""Durable task claiming and pinned worker-job construction."""

import copy
import hashlib
import json
import os
import re
import sqlite3
import time
import urllib.parse

import yaml
from jsonschema import Draft202012Validator

MAX_TOTAL_ATTEMPTS = 8
TASK_DB_PATH = os.environ.get("TASK_DB_PATH", "/data/tasks/tasks.sqlite3")


def task_database(path: str | None = None):
    path = path or TASK_DB_PATH
    db = sqlite3.connect(path, timeout=30, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("PRAGMA busy_timeout=30000")
    db.execute("""CREATE TABLE IF NOT EXISTS tasks (
        task_id TEXT PRIMARY KEY, repo TEXT, issue INTEGER, status TEXT NOT NULL,
        payload TEXT NOT NULL, result TEXT, job TEXT, lifecycle TEXT NOT NULL DEFAULT 'pending',
        sync_started REAL NOT NULL DEFAULT 0, credential_ref TEXT, callback_hash TEXT,
        retry_of TEXT, retry_key TEXT, credential_cleanup TEXT NOT NULL DEFAULT 'retained',
        attempts INTEGER NOT NULL DEFAULT 0, claim_owner TEXT, claimed_at REAL,
        config_version TEXT, config_hash TEXT, plan TEXT, retry_feedback TEXT)""")
    columns = {row[1] for row in db.execute("PRAGMA table_info(tasks)")}
    for name, declaration in (("attempts", "INTEGER NOT NULL DEFAULT 0"), ("claim_owner", "TEXT"),
                              ("claimed_at", "REAL"), ("config_version", "TEXT"),
                              ("config_hash", "TEXT"), ("plan", "TEXT"), ("retry_feedback", "TEXT")):
        if name not in columns:
            db.execute(f"ALTER TABLE tasks ADD COLUMN {name} {declaration}")
    db.execute("""CREATE TABLE IF NOT EXISTS task_events (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL, created_at REAL NOT NULL,
        attempt INTEGER NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL, outcome TEXT NOT NULL,
        reason TEXT, validation_status TEXT, score REAL, review_topics TEXT NOT NULL DEFAULT '[]',
        failure_details TEXT)""")
    event_columns = {row[1] for row in db.execute("PRAGMA table_info(task_events)")}
    if "failure_details" not in event_columns:
        db.execute("ALTER TABLE task_events ADD COLUMN failure_details TEXT")
    db.execute("CREATE INDEX IF NOT EXISTS task_events_by_task ON task_events(task_id, event_id)")
    return db


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


def record_task_event(db, task_id, attempt, actor, action, outcome, *, reason=None, validation_status=None,
                      score=None, review_topics=()):
    if actor not in ("supervisor", "worker", "judge") or action not in (
            "claim", "plan", "dispatch", "candidate", "review", "publish", "task") or outcome not in (
            "started", "recorded", "reused", "dispatched", "returned", "failed", "accepted", "rejected", "retrying", "completed"):
        raise ValueError("invalid-operational-event")
    safe_reason = reason if reason in EVENT_REASONS else None
    safe_validation = validation_status if validation_status in ("passed", "failed", "not-run") else None
    safe_score = score if type(score) in (int, float) and 0 <= score <= 1 else None
    topics = [topic for topic in EVENT_TOPICS if topic in review_topics]
    db.execute("BEGIN IMMEDIATE")
    try:
        db.execute("""INSERT INTO task_events(task_id,created_at,attempt,actor,action,outcome,reason,
            validation_status,score,review_topics) VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (task_id, time.time(), max(1, min(8, int(attempt))), actor, action, outcome, safe_reason,
             safe_validation, safe_score, json.dumps(topics)))
        db.execute("""DELETE FROM task_events WHERE task_id=? AND event_id NOT IN
            (SELECT event_id FROM task_events WHERE task_id=? ORDER BY event_id DESC LIMIT 128)""", (task_id, task_id))
        db.commit()
    except Exception:
        db.rollback()
        raise


def review_topics(review):
    text = str(review)[:4000].lower()
    return [topic for topic, pattern in (
        ("tests", r"\b(test\w*|coverage|validation)\b"),
        ("correctness", r"\b(bug\w*|correct\w*|logic|behavior|requirement\w*)\b"),
        ("security", r"\b(secur\w*|secret\w*|credential\w*|unsafe)\b"),
        ("scope", r"\b(scope|unrelated|unnecessary)\b"),
        ("patch-integrity", r"\b(patch|diff|unchanged|no-op)\b"),
    ) if re.search(pattern, text)]


def failure_feedback(stage, reason, attempt, review=""):
    """Keep only closed-vocabulary diagnostics, never raw exceptions or model text."""
    allowed_reasons = {
        "no-op-edit", "empty-or-invalid-edits", "invalid-generation-output",
        "invalid-generated-edit", "edit-outside-policy", "unsafe-edit-path",
        "protected-edit-path", "secret-in-model-context", "edits-not-applied",
        "empty-patch", "validation-failed", "validation-output-too-large",
        "coder-http-error", "worker-result-timeout", "worker-candidate-failed",
        "candidate-patch-edits-mismatch", "candidate-edit-set-mismatch",
        "candidate-validation-not-passed", "task-execution-failed",
    }
    code = str(reason).split(":", 1)[0]
    feedback = {"stage": stage, "attempt": min(MAX_TOTAL_ATTEMPTS, max(1, int(attempt))),
                "reason": code if code in allowed_reasons else "task-execution-failed"}
    if stage == "judge":
        feedback["reason"] = "judge-rejected"
        # Extract topics, not excerpts: redaction alone cannot safely remove arbitrary
        # source, prompts, credentials, or instructions from a free-form review.
        feedback["review_topics"] = review_topics(review)
    return json.dumps(feedback, sort_keys=True)


def requeue_failed_task(db, task_id, owner, feedback):
    # Clearing a result and retaining its safe feedback must survive as one write.
    db.execute("""UPDATE tasks SET status='queued', result=NULL, retry_feedback=?,
        claim_owner=NULL, claimed_at=NULL WHERE task_id=? AND claim_owner=?""",
        (feedback, task_id, owner))


def claim_queued_task(db, owner: str, now: float | None = None, lease_seconds: int = 900,
                      max_attempts: int = MAX_TOTAL_ATTEMPTS):
    now = time.time() if now is None else now
    db.execute("BEGIN IMMEDIATE")
    row = db.execute("""SELECT * FROM tasks WHERE status='queued'
        OR (status='running' AND (claimed_at IS NULL OR claimed_at < ?))
        ORDER BY rowid LIMIT 1""", (now - lease_seconds,)).fetchone()
    if row is None:
        db.commit()
        return None
    attempts = row["attempts"] or 0
    if attempts >= max_attempts and row["status"] == "queued":
        db.execute("UPDATE tasks SET status='running', claim_owner=?, claimed_at=? WHERE task_id=?",
                   (owner, now, row["task_id"]))
        claimed = db.execute("SELECT * FROM tasks WHERE task_id=?", (row["task_id"],)).fetchone()
        db.commit()
        return claimed
    next_attempt = attempts + (1 if row["status"] == "queued" else 0)
    db.execute("UPDATE tasks SET status='running', attempts=?, claim_owner=?, claimed_at=? WHERE task_id=?",
               (next_attempt, owner, now, row["task_id"]))
    claimed = db.execute("SELECT * FROM tasks WHERE task_id=?", (row["task_id"],)).fetchone()
    db.commit()
    return claimed


def release_task_claim(db, task_id: str, owner: str, requeue: bool = True):
    status = "queued" if requeue else "running"
    db.execute("UPDATE tasks SET status=?, claim_owner=NULL, claimed_at=NULL WHERE task_id=? AND claim_owner=?",
               (status, task_id, owner))


def load_pinned_catalog(task, k8s_request):
    namespace = os.environ.get("POD_NAMESPACE", "orchestrator")
    prefix = os.environ.get("AGENT_CATALOG_NAME_PREFIX", "orchestrator-agent-catalog-")
    name = prefix + (task["config_hash"] or "")[:12]
    config = k8s_request("GET", f"/api/v1/namespaces/{namespace}/configmaps/{name}")
    content = config.get("data", {}).get("agent-roles.yaml", "")
    schema_content = config.get("data", {}).get("agent-roles.schema.json", "")
    runtime_content = config.get("data", {}).get("runtime-config.json", "")
    if (not content or not schema_content
                or not runtime_content
                or hashlib.sha256((content + schema_content + runtime_content).encode()).hexdigest() != task["config_hash"]):
        raise RuntimeError("task-catalog-hash-mismatch")
    catalog = yaml.safe_load(content)
    schema = json.loads(schema_content)
    validator = Draft202012Validator(schema)
    if any(validator.iter_errors(catalog)):
        raise RuntimeError("task-catalog-schema-invalid")
    if not isinstance(catalog, dict) or catalog.get("spec_version") != 1 or catalog.get("config_version") != task["config_version"]:
        raise RuntimeError("task-catalog-version-unsupported")
    roles = catalog.get("roles")
    if not isinstance(roles, dict) or set(roles) != {"coder", "judge"}:
        raise RuntimeError("task-catalog-role-set-invalid")
    allowed_tools = {"repository_read", "candidate_edit", "sandbox_validation", "candidate_review"}
    for role_id, role in roles.items():
        if (not isinstance(role, dict) or role.get("id") != role_id
                or role.get("endpoint_alias") != role_id
                or not set(role.get("tools", [])) <= allowed_tools):
            raise RuntimeError("task-catalog-role-invalid")
    if "candidate_review" in roles["coder"].get("tools", []) or "candidate_edit" in roles["judge"].get("tools", []):
        raise RuntimeError("task-catalog-capability-boundary-invalid")
    runtime = json.loads(runtime_content)
    required = {"acceptor_url", "coder_url", "coder_model", "coder_native_download", "judge_url", "judge_model",
                "threshold", "max_total_attempts", "allowed_base_branches", "reviewers",
                "repo_policies", "worker_parallelism"}
    if (not isinstance(runtime, dict) or not required <= runtime.keys()
            or runtime.get("max_total_attempts") != MAX_TOTAL_ATTEMPTS
            or type(runtime.get("threshold")) not in (float, int)
            or not 0 <= runtime["threshold"] <= 1
            or runtime.get("worker_parallelism") != 1):
        raise RuntimeError("task-runtime-config-invalid")
    review_margin = runtime.get("judge_review_margin", 0.1)
    if type(review_margin) not in (float, int) or not 0 <= review_margin <= 1:
        raise RuntimeError("task-runtime-config-invalid")
    runtime["judge_review_margin"] = review_margin
    for endpoint in (runtime["acceptor_url"], runtime["coder_url"], runtime["judge_url"]):
        parsed = urllib.parse.urlsplit(endpoint)
        if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise RuntimeError("task-runtime-endpoint-invalid")
    return catalog, runtime


def dispatch_task(task, runtime, k8s_request):
    namespace = os.environ.get("POD_NAMESPACE", "orchestrator")
    template_name = os.environ.get("WORKER_JOB_TEMPLATE_CONFIGMAP", "orchestrator-worker")
    configmap = k8s_request("GET", f"/api/v1/namespaces/{namespace}/configmaps/{template_name}")
    template_spec = json.loads(configmap["data"]["job-spec.json"])
    name = f"{template_name}-{task['task_id'][-24:]}-{task['attempts']}"
    job = {"apiVersion": "batch/v1", "kind": "Job", "spec": copy.deepcopy(template_spec)}
    job["metadata"] = {"name": name, "namespace": namespace,
                       "labels": {"app": "orchestrator", "component": "worker",
                                  "task-id": task["task_id"], "config-hash": (task["config_hash"] or "unknown")[:63]}}
    job.pop("status", None)
    job_spec = job.setdefault("spec", {})
    job_spec.pop("suspend", None)
    job_spec.pop("selector", None)
    job_spec.pop("manualSelector", None)
    pod_labels = job_spec["template"].get("metadata", {}).get("labels", {})
    for label in ("controller-uid", "job-name", "batch.kubernetes.io/controller-uid", "batch.kubernetes.io/job-name"):
        pod_labels.pop(label, None)
    job_spec.update({"parallelism": 1, "completions": 1, "backoffLimit": 0,
                     "ttlSecondsAfterFinished": int(os.environ.get("TASK_JOB_TTL_SECONDS_AFTER_FINISHED", "86400"))})
    pod_spec = job_spec["template"]["spec"]
    pod_spec["restartPolicy"] = "Never"
    pod_spec["automountServiceAccountToken"] = False
    catalog_prefix = os.environ.get("AGENT_CATALOG_NAME_PREFIX", "orchestrator-agent-catalog-")
    catalog_name = catalog_prefix + (task["config_hash"] or "")[:12]
    if not task["config_hash"] or not re.fullmatch(r"[a-z0-9-]+", catalog_name):
        raise RuntimeError("task-config-pin-invalid")
    k8s_request("GET", f"/api/v1/namespaces/{namespace}/configmaps/{catalog_name}")
    for volume in pod_spec.get("volumes", []):
        if volume.get("name") == "agent-catalog":
            volume["configMap"]["name"] = catalog_name
    container = next(item for item in pod_spec["containers"] if item["name"] == "coder")
    if any(entry.get("name") in {"CODEFACTORY_AUTH_TOKEN", "REPO_GITHUB_PAT", "GITHUB_TOKEN", "PR_GITHUB_TOKEN",
                                 "GITHUB_AUTH_MODE", "CODEFACTORY_AUTH_MODE"} for entry in container.get("env", [])):
        raise RuntimeError("task-authentication-unsupported")
    assigned_payload = json.loads(task["payload"])
    if assigned_payload.get("repository_auth") != "github-app":
        raise RuntimeError("task-authentication-unsupported")
    assigned_payload["supervisor_plan"] = task["plan"] or ""
    assigned_payload["retry_feedback"] = json.loads(task["retry_feedback"]) if task["retry_feedback"] else None
    env = [entry for entry in container.get("env", []) if entry.get("name") not in {
        "ASSIGNED_TASK_ID", "ASSIGNED_TASK_PAYLOAD", "TASK_CALLBACK_TOKEN", "TASK_REPOSITORY",
        "TASK_ATTEMPT",
        "TASK_CREDENTIAL_EXPIRES_AT", "TASK_REPOSITORY_AUTH",
        "AGENT_CONFIG_VERSION", "AGENT_CONFIG_HASH", "JUDGE_URL", "JUDGE_MODEL",
        "JUDGE_NATIVE_DOWNLOAD", "JUDGE_ACCEPT_THRESHOLD", "OPENAI_API_KEY", "CODER_URL",
        "CODER_MODEL", "CODER_NATIVE_DOWNLOAD", "ACCEPTOR_URL", "ALLOWED_REPOS", "ALLOWED_REPO",
        "TASK_REPO_POLICIES", "WORKER_ROLE"}]
    env.extend([
        {"name": "ASSIGNED_TASK_ID", "value": task["task_id"]},
        {"name": "TASK_ATTEMPT", "value": str(task["attempts"])},
        {"name": "ASSIGNED_TASK_PAYLOAD", "value": json.dumps(assigned_payload, sort_keys=True)},
        {"name": "AGENT_CONFIG_VERSION", "value": task["config_version"] or "1"},
        {"name": "AGENT_CONFIG_HASH", "value": task["config_hash"]},
        {"name": "WORKER_ROLE", "value": "coder"},
        {"name": "TASK_REPOSITORY", "value": task["repo"] or ""},
        {"name": "TASK_CREDENTIAL_EXPIRES_AT", "valueFrom": {"secretKeyRef": {
            "name": task["credential_ref"], "key": "expires-at"}}},
        {"name": "TASK_REPOSITORY_AUTH", "valueFrom": {"secretKeyRef": {
            "name": task["credential_ref"], "key": "repository-auth"}}},
        {"name": "TASK_CALLBACK_TOKEN", "valueFrom": {"secretKeyRef": {
            "name": task["credential_ref"], "key": "callback-token"}}},
        {"name": "CODER_URL", "value": runtime["coder_url"]},
        {"name": "CODER_MODEL", "value": runtime["coder_model"]},
        {"name": "CODER_NATIVE_DOWNLOAD", "value": str(runtime.get("coder_native_download", False)).lower()},
        {"name": "ACCEPTOR_URL", "value": runtime["acceptor_url"]},
        {"name": "TASK_REPO_POLICIES", "value": json.dumps(runtime["repo_policies"], sort_keys=True)},
    ])
    container["env"] = env
    created = k8s_request("POST", f"/apis/batch/v1/namespaces/{namespace}/jobs", job)
    return created.get("metadata", {}).get("name", name) if created else name