"""Versioned model and prompt configuration stored with durable task state."""

import hashlib
import json
import re
import time

import yaml

MODEL_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}")
CONFIG_HASH = re.compile(r"[a-f0-9]{64}")
PROMPT_ROLES = {"worker", "planner", "judge"}
PROMPT_LIMIT = 2000
PROMPT_BLOCKLIST = re.compile(
    r"ignore\s+(?:all\s+)?(?:previous|prior)|override|bypass|circumvent|credential|secret|token|"
    r"tool|authority|contract|permission|privilege|elevat|administrator|superuser|\broot\b|"
    r"command|shell|execute|deploy|publish|push|commit|workflow|repository|editable\s+files|"
    r"system\s+prompt|output\s+(?:format|contract)|json\s+contract|capabilit|grant\s+access",
    re.IGNORECASE,
)


class ConfigConflict(ValueError):
    pass


class ConfigUnavailable(RuntimeError):
    pass


def _active(db):
    return db.execute("""SELECT versions.* FROM agent_config_state AS state
        JOIN agent_config_versions AS versions ON versions.config_hash=state.active_config_hash
        WHERE state.singleton=1""").fetchone()


def initialize(db, environ):
    required = ("AGENT_CONFIG_HASH", "AGENT_CONFIG_VERSION", "AGENT_CATALOG_YAML",
                "AGENT_CATALOG_SCHEMA_JSON", "AGENT_RUNTIME_CONFIG_JSON")
    if not any(name in environ for name in required):
        return
    if not all(environ.get(name) for name in required):
        raise ConfigUnavailable("agent-config-bootstrap-invalid")
    catalog = environ["AGENT_CATALOG_YAML"]
    schema = environ["AGENT_CATALOG_SCHEMA_JSON"]
    runtime = environ["AGENT_RUNTIME_CONFIG_JSON"]
    if hashlib.sha256((catalog + schema + runtime).encode()).hexdigest() != environ["AGENT_CONFIG_HASH"]:
        raise ConfigUnavailable("agent-config-bootstrap-hash-invalid")
    try:
        catalog_doc = yaml.safe_load(catalog)
        runtime_doc = json.loads(runtime)
    except (ValueError, yaml.YAMLError):
        raise ConfigUnavailable("agent-config-bootstrap-invalid") from None
    if (not isinstance(catalog_doc, dict) or not isinstance(runtime_doc, dict)
            or catalog_doc.get("config_version") != environ["AGENT_CONFIG_VERSION"]
            or runtime_doc.get("version") != environ["AGENT_CONFIG_VERSION"]
            or runtime_doc.get("catalog") != catalog):
        raise ConfigUnavailable("agent-config-bootstrap-invalid")
    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute("""INSERT OR IGNORE INTO agent_config_versions
            (config_hash,config_version,revision,catalog,catalog_schema,runtime_config,created_at,created_by)
            VALUES(?,?,?,?,?,?,?,?)""",
                   (environ["AGENT_CONFIG_HASH"], environ["AGENT_CONFIG_VERSION"], 1, catalog, schema,
                    runtime, time.time(), "helm"))
        db.execute("INSERT OR IGNORE INTO agent_config_state(singleton,active_config_hash) VALUES(1,?)",
                   (environ["AGENT_CONFIG_HASH"],))
        _add_catalog_roles(db, catalog_doc, schema)
        db.commit()
    except Exception:
        db.rollback()
        raise


def _add_catalog_roles(db, bootstrap_catalog, catalog_schema):
    current = _active(db)
    if current is None:
        return
    active_catalog = yaml.safe_load(current["catalog"])
    active_roles = active_catalog.get("roles") if isinstance(active_catalog, dict) else None
    bootstrap_roles = bootstrap_catalog.get("roles") if isinstance(bootstrap_catalog, dict) else None
    if not isinstance(active_roles, dict) or not isinstance(bootstrap_roles, dict):
        return
    if set(active_roles) != {"coder", "judge"} or set(bootstrap_roles) != {"coder", "doc", "judge"}:
        return

    active_catalog["roles"] = {**active_roles, "doc": bootstrap_roles["doc"]}
    root = db.execute("SELECT config_version FROM agent_config_versions WHERE revision=1").fetchone()
    latest = db.execute("SELECT MAX(revision) FROM agent_config_versions").fetchone()[0]
    if root is None or latest is None:
        raise ConfigUnavailable("agent-config-root-unavailable")
    revision = latest + 1
    version = f"{root['config_version']}-r{revision}"
    active_catalog["config_version"] = version
    catalog = yaml.safe_dump(active_catalog, sort_keys=False, allow_unicode=False)
    runtime = json.loads(current["runtime_config"])
    runtime["version"], runtime["catalog"] = version, catalog
    runtime_json = json.dumps(runtime, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256((catalog + catalog_schema + runtime_json).encode()).hexdigest()
    db.execute("""INSERT INTO agent_config_versions
        (config_hash,config_version,revision,catalog,catalog_schema,runtime_config,created_at,created_by)
        VALUES(?,?,?,?,?,?,?,?)""",
               (digest, version, revision, catalog, catalog_schema, runtime_json,
                time.time(), "helm-catalog-upgrade"))
    db.execute("UPDATE agent_config_state SET active_config_hash=? WHERE singleton=1", (digest,))


def public_config(row):
    runtime = json.loads(row["runtime_config"])
    catalog = yaml.safe_load(row["catalog"])
    return {
        "version": row["config_version"],
        "hash": row["config_hash"],
        "models": [{"role": role, "model": runtime[role + "_model"],
                    "native_download": runtime[role + "_native_download"], "availability": "unknown"}
                   for role in ("coder", "judge")],
        "prompts": runtime["prompts"],
        "agents": [{"id": role["id"], "role": name, "endpoint_alias": role["endpoint_alias"],
                    "max_files": role["max_files"], "tools": role["tools"], "availability": "unknown"}
                   for name, role in catalog["roles"].items() if name in ("coder", "doc", "judge")],
    }


def validate_draft(row, payload):
    if (not isinstance(payload, dict) or set(payload) != {"expected_hash", "models", "prompts"}
            or not isinstance(payload["expected_hash"], str)
            or not CONFIG_HASH.fullmatch(payload["expected_hash"])):
        raise ValueError("invalid-config-draft")
    if payload["expected_hash"] != row["config_hash"]:
        raise ConfigConflict("config-version-conflict")
    models, prompts = payload["models"], payload["prompts"]
    if not isinstance(models, dict) or set(models) != {"coder", "judge"}:
        raise ValueError("invalid-config-models")
    if any(not isinstance(model, str) or not MODEL_ID.fullmatch(model) for model in models.values()):
        raise ValueError("invalid-config-model")
    if not isinstance(prompts, dict) or set(prompts) != PROMPT_ROLES:
        raise ValueError("invalid-config-prompts")
    for prompt in prompts.values():
        if (not isinstance(prompt, str) or len(prompt) > PROMPT_LIMIT
                or any(ord(char) < 32 and char not in "\n\t" for char in prompt)
                or PROMPT_BLOCKLIST.search(prompt)):
            raise ValueError("invalid-config-prompt")
    return models, prompts


def apply(db, payload, actor):
    try:
        db.execute("BEGIN IMMEDIATE")
        current = _active(db)
        if current is None:
            raise ConfigUnavailable("agent-config-unavailable")
        models, prompts = validate_draft(current, payload)
        runtime = json.loads(current["runtime_config"])
        catalog_doc = yaml.safe_load(current["catalog"])
        revision = current["revision"] + 1
        root = db.execute("SELECT config_version FROM agent_config_versions WHERE revision=1").fetchone()
        if root is None:
            raise ConfigUnavailable("agent-config-root-unavailable")
        version = f"{root['config_version']}-r{revision}"
        catalog_doc["config_version"] = version
        catalog = yaml.safe_dump(catalog_doc, sort_keys=False, allow_unicode=False)
        runtime["version"] = version
        runtime["catalog"] = catalog
        runtime["coder_model"], runtime["judge_model"] = models["coder"], models["judge"]
        runtime["prompts"] = prompts
        runtime_json = json.dumps(runtime, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256((catalog + current["catalog_schema"] + runtime_json).encode()).hexdigest()
        db.execute("""INSERT INTO agent_config_versions
            (config_hash,config_version,revision,catalog,catalog_schema,runtime_config,created_at,created_by)
            VALUES(?,?,?,?,?,?,?,?)""",
                   (digest, version, revision, catalog, current["catalog_schema"], runtime_json,
                    time.time(), actor))
        db.execute("UPDATE agent_config_state SET active_config_hash=? WHERE singleton=1", (digest,))
        updated = _active(db)
        db.commit()
        return public_config(updated)
    except Exception:
        db.rollback()
        raise


def active(db):
    row = _active(db)
    if row is None:
        raise ConfigUnavailable("agent-config-unavailable")
    return public_config(row)


def validate_active_draft(db, payload):
    row = _active(db)
    if row is None:
        raise ConfigUnavailable("agent-config-unavailable")
    models, prompts = validate_draft(row, payload)
    return {"valid": True, "models": models, "prompts": prompts}


def active_row(db):
    return _active(db)