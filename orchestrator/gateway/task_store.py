"""Durable task database setup and credential cleanup lifecycle."""

import os
import sqlite3
import time


def connect_task_database(path, init_lock):
    path = path or os.environ.get("TASK_DB_PATH", "/data/tasks/tasks.sqlite3")
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with init_lock:
        db = sqlite3.connect(path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("""CREATE TABLE IF NOT EXISTS tasks (
            task_id TEXT PRIMARY KEY, repo TEXT, issue INTEGER, status TEXT NOT NULL,
            payload TEXT NOT NULL, result TEXT, job TEXT, lifecycle TEXT NOT NULL DEFAULT 'pending',
            sync_started REAL NOT NULL DEFAULT 0)""")
        columns = {row[1] for row in db.execute("PRAGMA table_info(tasks)")}
        for name in ("credential_ref", "credential_uid", "callback_hash", "retry_of", "retry_key"):
            if name not in columns:
                db.execute(f"ALTER TABLE tasks ADD COLUMN {name} TEXT")
        if "credential_cleanup" not in columns:
            db.execute("ALTER TABLE tasks ADD COLUMN credential_cleanup TEXT NOT NULL DEFAULT 'retained'")
        for name, declaration in (("attempts", "INTEGER NOT NULL DEFAULT 0"),
                                  ("claim_owner", "TEXT"), ("claimed_at", "REAL"),
                                  ("config_version", "TEXT"), ("config_hash", "TEXT"), ("plan", "TEXT"),
                                  ("retry_feedback", "TEXT")):
            if name not in columns:
                db.execute(f"ALTER TABLE tasks ADD COLUMN {name} {declaration}")
        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS task_retry_parent ON tasks(retry_of) WHERE retry_of IS NOT NULL")
        db.execute("""CREATE TABLE IF NOT EXISTS task_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL, created_at REAL NOT NULL,
            attempt INTEGER NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL, outcome TEXT NOT NULL,
            reason TEXT, validation_status TEXT, score REAL, review_topics TEXT NOT NULL DEFAULT '[]',
            failure_details TEXT)""")
        db.execute("""CREATE TABLE IF NOT EXISTS agent_config_versions (
            config_hash TEXT PRIMARY KEY, config_version TEXT NOT NULL UNIQUE, revision INTEGER NOT NULL UNIQUE,
            catalog TEXT NOT NULL, catalog_schema TEXT NOT NULL, runtime_config TEXT NOT NULL,
            created_at REAL NOT NULL, created_by TEXT NOT NULL)""")
        db.execute("""CREATE TABLE IF NOT EXISTS agent_config_state (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1), active_config_hash TEXT NOT NULL)""")
        event_columns = {row[1] for row in db.execute("PRAGMA table_info(task_events)")}
        if "failure_details" not in event_columns:
            db.execute("ALTER TABLE task_events ADD COLUMN failure_details TEXT")
        db.execute("CREATE INDEX IF NOT EXISTS task_events_by_task ON task_events(task_id, event_id)")
    return db


def retry_lifecycle(task_id, task_db, delete_task_secret, kubernetes_error, logger):
    db = task_db()
    try:
        db.execute("BEGIN IMMEDIATE")
        task = db.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if task["lifecycle"] == "synced" or (task["lifecycle"] == "syncing" and time.time() - task["sync_started"] < 1800):
            db.commit()
            return task["lifecycle"]
        db.execute("UPDATE tasks SET lifecycle='syncing', sync_started=? WHERE task_id=?", (time.time(), task_id))
        db.commit()
        try:
            db.execute("UPDATE tasks SET lifecycle='cleanup-pending' WHERE task_id=?", (task_id,))
            db.execute("UPDATE tasks SET credential_cleanup='pending' WHERE task_id=?", (task_id,))
            delete_task_secret(task)
            db.execute("UPDATE tasks SET credential_cleanup='deleted' WHERE task_id=?", (task_id,))
            status = "synced"
        except kubernetes_error as error:
            logger.warning("credential-cleanup-pending task=%s", task_id)
            status = "credential-binding-mismatch" if error.status == 409 else "cleanup-pending"
        db.execute("UPDATE tasks SET lifecycle=? WHERE task_id=?", (status, task_id))
        return status
    finally:
        db.close()