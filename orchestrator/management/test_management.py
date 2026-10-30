import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.parse import parse_qs, urlsplit

import httpx
import jwt
import pytest
import yaml
from app import create_app as actual_create_app
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

APP_VALUES = yaml.safe_load((Path(__file__).resolve().parents[2] / "helm/orchestrator/example.values.yaml").read_text())["githubAuth"]
SECRET_REFS = {
    "githubAppKey": {"name": "test-app-key", "key": "private-key.pem"},
    "managementServiceToken": {"name": "test-management-token", "key": "token"},
    "supervisorServiceToken": {"name": "test-supervisor-token", "key": "token"},
    "openaiApiKey": {"name": "", "key": "api-key"},
    "coderApiKey": {"name": "", "key": "api-key"},
    "managementAdminPassword": {"name": "test-admin-password", "key": "password"},
    "managementGithubClientSecret": {"name": "", "key": "client-secret"},
}
HELM_TEST_VALUES = {
    "secretRefs": SECRET_REFS,
    "imagePullSecrets": [{"name": "test-pull"}],
    "githubAuth": APP_VALUES,
}


def create_app(settings=None, transport=None, signing_keys=None):
    def oauth(request):
        assert request.url == "https://github.com/login/oauth/access_token"
        return httpx.Response(200, json={"access_token": "ghu_user-test-only", "token_type": "bearer", "expires_in": 3600})
    return actual_create_app(settings, transport, signing_keys, github_transport=httpx.MockTransport(oauth))


def connect_github(client):
    client.base_url = "https://factory.example"
    response = client.post("/management/api/github/connect", headers={"Origin": "https://factory.example", "X-Management-Request": "1"})
    assert response.status_code == 200
    query = parse_qs(urlsplit(response.json()["authorization_url"]).query)
    assert query["code_challenge_method"] == ["S256"]
    assert query["redirect_uri"] == ["https://factory.example/auth"]
    response = client.get("/auth", params={"state": query["state"][0], "code": "test-code"}, follow_redirects=False)
    assert response.status_code == 303
    assert "ghu_" not in response.text and "ghu_" not in response.headers.get("set-cookie", "")
    assert client.get("/management/api/github/session").json()["connected"]


def test_module_import_does_not_construct_app_with_ci_environment():
    environment = {**os.environ, "GITHUB_TOKEN": "ci-test-only"}
    subprocess.run(
        [sys.executable, "-c", "import app"],
        cwd=Path(__file__).parent, env=environment,
        capture_output=True, text=True, check=True,
    )
    result = subprocess.run(
        [sys.executable, "-c", "import app; app.create_app()"],
        cwd=Path(__file__).parent,
        env=environment,
        capture_output=True, text=True, check=False,
    )
    assert result.returncode != 0
    assert "repository-authentication-configuration-invalid" in result.stderr


@pytest.fixture
def boundary(tmp_path):
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    keys = Mock()
    keys.get_signing_key_from_jwt.return_value = SimpleNamespace(key=private.public_key())
    runtime = tmp_path / "runtime.json"
    runtime.write_text(json.dumps({"version": "1", "catalog": "roles:\n  coder:\n    id: coder\n    endpoint_alias: coder\n    max_files: 8\n    tools: [repository_read]\n",
                                   "coder_model": "configured-coder", "judge_model": "configured-judge",
                                   "coder_native_download": True, "judge_native_download": False,
                                   "reviewers": "private", "coder_url": "https://secret@example.com"}))
    settings = {"MANAGEMENT_AUTH_MODE": "oidc",
                "MANAGEMENT_OIDC_ISSUER": "https://identity.example", "MANAGEMENT_OIDC_AUDIENCE": "management",
                "MANAGEMENT_OIDC_JWKS_URL": "https://identity.example/keys", "MANAGEMENT_ALLOWED_SUBJECTS": "operator",
                "MANAGEMENT_PUBLIC_ORIGIN": "https://factory.example",
                "MANAGEMENT_GITHUB_CLIENT_ID": "test-client", "MANAGEMENT_GITHUB_CLIENT_SECRET": "test-client-secret",
                "GITHUB_REPOSITORY_POLICIES": json.dumps([{"repository": "org/repo"}]),
                "MANAGEMENT_RUNTIME_CONFIG_FILE": str(runtime), "MANAGEMENT_GATEWAY_URL": "https://gateway.example",
                "MANAGEMENT_SERVICE_TOKEN": "management-service-test-only-at-least-32-characters"}
    upstream = Mock(return_value=httpx.Response(202, json={"task_id": "task-1", "status": "queued"}))
    client = TestClient(create_app(settings, httpx.MockTransport(upstream), keys))

    def token(**changes):
        return jwt.encode({"sub": "operator", "iss": "https://identity.example", "aud": "management",
                           "iat": int(time.time()), "exp": int(time.time()) + 120, **changes}, private, algorithm="RS256")

    client.headers.update({"Authorization": "Bearer " + token(), "Origin": "https://factory.example",
                           "X-Management-Request": "1"})
    connect_github(client)
    return client, upstream, token, settings


def test_admin_login_without_oidc(boundary, tmp_path):
    _, upstream, _, settings = boundary
    settings.update({"MANAGEMENT_AUTH_MODE": "admin", "MANAGEMENT_ADMIN_PASSWORD": "admin-test-password-only"})
    for key in list(settings):
        if key.startswith("MANAGEMENT_OIDC_") or key == "MANAGEMENT_ALLOWED_SUBJECTS":
            del settings[key]
    (tmp_path / "index.html").write_text("<html>Management</html>")
    settings["MANAGEMENT_STATIC_DIR"] = str(tmp_path)
    upstream.return_value = httpx.Response(200, json={"version": "1", "hash": "a" * 64,
        "models": [], "prompts": {"worker": "", "planner": "", "judge": ""}, "agents": []})
    client = TestClient(create_app(settings, httpx.MockTransport(upstream)))
    for path in ("/", "/management/api/session", "/management/api/tasks"):
        response = client.get(path)
        assert response.status_code == 401
        assert response.headers["www-authenticate"].startswith("Basic ")
        assert client.get(path, auth=("admin", "wrong-password")).status_code == 401
        assert client.get(path, auth=("other", "admin-test-password-only")).status_code == 401
    client.auth = ("admin", "admin-test-password-only")
    assert client.get("/").status_code == 200
    assert client.get("/management/api/session").json() == {"subject": "admin"}
    assert "admin-test-password-only" not in client.get("/management/api/session").text
    assert client.post("/management/api/tasks", json={}).status_code == 403
    connect_github(client)
    upstream.return_value = httpx.Response(202, json={"task_id": "task-1", "status": "queued"})
    response = client.post("/management/api/tasks", json={"repo": "org/repo", "issue_number": 7},
                           headers={"Origin": settings["MANAGEMENT_PUBLIC_ORIGIN"], "X-Management-Request": "1"})
    assert response.status_code == 202
    assert upstream.call_args.args[0].headers["Authorization"] == "token management-service-test-only-at-least-32-characters"
    for password in ("", "short!", "abcdefgh", "abcdefgh ", "abcdefg<", "abcdef! "):
        settings["MANAGEMENT_ADMIN_PASSWORD"] = password
        assert TestClient(create_app(settings)).get("/management/api/session").status_code == 503
    settings["MANAGEMENT_AUTH_MODE"] = "unknown"
    assert TestClient(create_app(settings)).get("/management/api/session").status_code == 503


def test_app_submission_uses_policy_not_repository_pat_mapping(boundary):
    _, upstream, _, settings = boundary
    settings.update({"MANAGEMENT_SERVICE_TOKEN": "management-service-only",
                     "GITHUB_REPOSITORY_POLICIES": json.dumps([{"repository": "org/repo"}])})
    settings.update({"MANAGEMENT_AUTH_MODE": "admin", "MANAGEMENT_ADMIN_PASSWORD": "admin-test-password-only"})
    client = TestClient(create_app(settings, httpx.MockTransport(upstream)))
    client.auth = ("admin", "admin-test-password-only")
    connect_github(client)
    assert client.get("/management/api/repositories").json() == {"repositories": ["org/repo"]}
    response = client.post("/management/api/tasks", json={"repo": "org/repo", "issue_number": 7},
                           headers={"Origin": settings["MANAGEMENT_PUBLIC_ORIGIN"], "X-Management-Request": "1"})
    assert response.status_code == 202
    request = upstream.call_args.args[0]
    assert request.headers["Authorization"] == "token management-service-only"
    assert "X-Repository-Token" not in request.headers
    assert "repository-test-secret" not in request.content.decode()
    assert client.post("/management/api/tasks", json={"repo": "other/repo", "issue_number": 7},
                       headers={"Origin": settings["MANAGEMENT_PUBLIC_ORIGIN"], "X-Management-Request": "1"}).status_code == 403


@pytest.mark.parametrize("setting", ["MANAGEMENT_REPOSITORIES_FILE", "CODEFACTORY_AUTH_TOKEN", "GITHUB_AUTH_MODE",
                                     "REPO_GITHUB_PAT", "GITHUB_APP_PRIVATE_KEY_FILE", "GITHUB_OIDC_ISSUER"])
def test_management_rejects_obsolete_or_gateway_only_auth_settings(boundary, setting):
    settings = {**boundary[3], setting: "stale-test-only"}
    with pytest.raises(ValueError, match="^repository-authentication-configuration-invalid$"):
        create_app(settings)


@pytest.mark.parametrize("password", ["abcdefg!", "elevenchar!"])
def test_admin_password_accepts_eight_characters_with_special_character(boundary, password):
    _, _, _, settings = boundary
    settings.update({"MANAGEMENT_AUTH_MODE": "admin", "MANAGEMENT_ADMIN_PASSWORD": password})
    client = TestClient(create_app(settings))
    assert client.get("/management/api/session", auth=("admin", password)).json() == {"subject": "admin"}


@pytest.mark.parametrize("repository_config", ["absent", "empty"])
def test_admin_operations_without_repository_access(boundary, tmp_path, repository_config):
    _, upstream, _, settings = boundary
    settings.update({"MANAGEMENT_AUTH_MODE": "admin", "MANAGEMENT_ADMIN_PASSWORD": "abcdefg!"})
    if repository_config == "absent":
        settings.pop("GITHUB_REPOSITORY_POLICIES")
    else:
        settings["GITHUB_REPOSITORY_POLICIES"] = "[]"
    (tmp_path / "index.html").write_text("<html>Management</html>")
    settings["MANAGEMENT_STATIC_DIR"] = str(tmp_path)
    client = TestClient(create_app(settings, httpx.MockTransport(upstream)))
    client.auth = ("admin", "abcdefg!")
    for path in ("/", "/management/api/session", "/management/api/config"):
        assert client.get(path).status_code == 200
    assert client.get("/management/api/repositories").json() == {"repositories": []}
    upstream.assert_called_once()
    assert upstream.call_args.args[0].url.path == "/api/v1/config"
    upstream.return_value = httpx.Response(200, json={"tasks": [{"task_id": "task-1", "role": "doc", "patch": "private"}],
                                                    "total": 1, "limit": 25, "offset": 0})
    assert client.get("/management/api/tasks").json()["tasks"] == [{"task_id": "task-1", "role": "doc"}]
    upstream.return_value = httpx.Response(200, json={"task_id": "task-1", "role": "doc", "result": {"patch": "private"}})
    assert client.get("/management/api/tasks/task-1").json() == {
        "task_id": "task-1",
        "role": "doc",
        "execution": {
            "job_name": None,
            "worker_status": None,
            "validation_status": None,
            "error": None,
            "report_failed": False,
        },
        "coordination": {"plan_recorded": False, "latest_feedback": None, "judge_score": None},
        "events": None,
        "pr_url": None,
    }
    upstream.reset_mock()
    response = client.post("/management/api/tasks", json={"repo": "org/repo", "issue_number": 7},
                           headers={"Origin": settings["MANAGEMENT_PUBLIC_ORIGIN"], "X-Management-Request": "1"})
    assert response.status_code == 401
    upstream.assert_not_called()


def test_secure_defaults_and_all_routes_require_identity(boundary):
    client, upstream, _, _ = boundary
    client.headers.pop("Authorization")
    for path in ("/", "/assets/app.js", "/management/api/session", "/management/api/tasks", "/management/api/config"):
        assert client.get(path).status_code == 401
    assert client.post("/management/api/tasks", json={}).status_code == 401
    upstream.assert_not_called()
    closed = TestClient(create_app({}))
    assert closed.get("/management/api/tasks").status_code == 503
    assert closed.get("/").status_code == 503
    assert closed.get("/healthz").status_code == 200


@pytest.mark.parametrize("changes,code", [({"sub": "other"}, 403), ({"aud": "other"}, 401),
                                          ({"iss": "https://other"}, 401), ({"exp": 1}, 401)])
def test_signed_identity_and_operator_authorization(boundary, changes, code):
    client, upstream, token, _ = boundary
    client.headers["Authorization"] = "Bearer " + token(**changes)
    assert client.get("/management/api/session").status_code == code
    upstream.assert_not_called()


def test_unsigned_and_forged_identity_denied(boundary):
    client, upstream, _, _ = boundary
    client.headers["Authorization"] = "Bearer " + jwt.encode({"sub": "operator"}, key="", algorithm="none")
    client.headers["X-Auth-Request-User"] = "operator"
    assert client.get("/management/api/tasks").status_code == 401
    upstream.assert_not_called()


def test_submission_keeps_credentials_server_side_and_uses_contract(boundary):
    client, upstream, _, _ = boundary
    assert client.get("/management/api/repositories").json() == {"repositories": ["org/repo"]}
    response = client.post("/management/api/tasks", json={"repo": "ORG/repo", "issue_number": 7})
    assert response.status_code == 202
    request = upstream.call_args.args[0]
    assert request.url == "https://gateway.example/api/v1/tasks/spawn"
    assert json.loads(request.content) == {"repo": "org/repo", "issue_number": 7}
    assert request.headers["Authorization"] == "token management-service-test-only-at-least-32-characters"
    assert request.headers["X-GitHub-User-Token"] == "ghu_user-test-only"
    assert "X-Repository-Token" not in request.headers
    assert "secret" not in response.text


def test_dynamic_submission_requires_connected_github_user(boundary):
    _, upstream, _, settings = boundary
    settings.update({"GITHUB_REPOSITORY_POLICIES": "[]", "MANAGEMENT_AUTH_MODE": "admin",
                     "MANAGEMENT_ADMIN_PASSWORD": "abcdefg!"})
    client = TestClient(create_app(settings, httpx.MockTransport(upstream)))
    client.auth = ("admin", "abcdefg!")
    client.headers.update({"Origin": settings["MANAGEMENT_PUBLIC_ORIGIN"], "X-Management-Request": "1"})
    payload = {"repo": "new/project", "issue_number": 7}
    assert client.post("/management/api/tasks", json=payload).status_code == 401
    upstream.assert_not_called()
    connect_github(client)
    response = client.post("/management/api/tasks", json=payload)
    assert response.status_code == 202
    request = upstream.call_args.args[0]
    assert json.loads(request.content) == payload
    assert request.headers["X-GitHub-User-Token"] == "ghu_user-test-only"
    assert "ghu_" not in response.text
    assert client.post("/management/api/github/disconnect").status_code == 200
    assert client.post("/management/api/tasks", json=payload).status_code == 401


@pytest.mark.parametrize("kind", ["cookie", "state", "expired", "subject", "missing-code", "replay"])
def test_oauth_state_cookie_subject_expiry_and_replay(boundary, monkeypatch, kind):
    client, _, _, _ = boundary
    response = client.post("/management/api/github/connect")
    query = parse_qs(urlsplit(response.json()["authorization_url"]).query)
    params = {"state": query["state"][0], "code": "test-code"}
    if kind == "cookie":
        client.cookies.clear()
    if kind == "state":
        params["state"] = "attacker-state"
    if kind == "expired":
        now = time.time()
        monkeypatch.setattr("app.time.time", lambda: now + 301)
    if kind == "subject":
        dependency = next(route for route in client.app.routes if route.path == "/management/api/session").dependant.dependencies[0].call
        client.app.dependency_overrides[dependency] = lambda: "another-verified-subject"
    if kind == "missing-code":
        params.pop("code")
    if kind == "replay":
        assert client.get("/auth", params=params, follow_redirects=False).status_code == 303
    assert client.get("/auth", params=params, follow_redirects=False).status_code == 403


def test_github_session_expires_and_is_bound_to_management_subject(boundary, monkeypatch):
    client, upstream, _, _ = boundary
    dependency = next(route for route in client.app.routes if route.path == "/management/api/session").dependant.dependencies[0].call
    client.app.dependency_overrides[dependency] = lambda: "another-verified-subject"
    assert not client.get("/management/api/github/session").json()["connected"]
    client.app.dependency_overrides.clear()
    now = time.time()
    monkeypatch.setattr("app.time.time", lambda: now + 3601)
    assert not client.get("/management/api/github/session").json()["connected"]
    assert client.post("/management/api/tasks", json={"repo": "org/repo", "issue_number": 7}).status_code == 401
    upstream.assert_not_called()


@pytest.mark.parametrize("grant", [
    {"access_token": "ghp_not-an-app-token", "token_type": "bearer"},
    {"access_token": "ghu_test-only", "token_type": "bearer", "expires_in": 0},
    {"error": "bad_verification_code"},
])
def test_oauth_invalid_grant_is_redacted(boundary, grant):
    _, _, _, settings = boundary
    settings.update({"MANAGEMENT_AUTH_MODE": "admin", "MANAGEMENT_ADMIN_PASSWORD": "abcdefg!"})
    oauth = Mock(return_value=httpx.Response(200, json=grant))
    client = TestClient(actual_create_app(settings, github_transport=httpx.MockTransport(oauth)), base_url="https://factory.example")
    client.auth = ("admin", "abcdefg!")
    start = client.post("/management/api/github/connect", headers={"Origin": settings["MANAGEMENT_PUBLIC_ORIGIN"], "X-Management-Request": "1"})
    state = parse_qs(urlsplit(start.json()["authorization_url"]).query)["state"][0]
    response = client.get("/auth", params={"state": state, "code": "test-code"})
    assert response.status_code == 403
    assert "ghu_" not in response.text and "ghp_" not in response.text
    assert "code_verifier" in json.loads(oauth.call_args.args[0].content)


def test_oauth_connect_requires_config_and_same_origin(boundary):
    client, _, _, settings = boundary
    assert client.post("/management/api/github/connect", headers={"Origin": "https://attacker.example"}).status_code == 403
    settings.update({"MANAGEMENT_AUTH_MODE": "admin", "MANAGEMENT_ADMIN_PASSWORD": "abcdefg!",
                     "MANAGEMENT_GITHUB_CLIENT_ID": ""})
    client = TestClient(create_app(settings))
    client.auth = ("admin", "abcdefg!")
    assert client.post("/management/api/github/connect", headers={"Origin": settings["MANAGEMENT_PUBLIC_ORIGIN"], "X-Management-Request": "1"}).status_code == 503


@pytest.mark.parametrize("payload,code", [({"repo": "other/repo", "issue_number": 7}, 403),
    ({"repo": "org/repo", "issue_number": True}, 400), ({"repo": "org/repo", "issue_number": 0}, 400),
    ({"repo": "org/repo", "issue_number": 7, "token": "secret"}, 400), ([], 400)])
def test_submission_rejects_invalid_or_unapproved_input(boundary, payload, code):
    client, upstream, _, _ = boundary
    assert client.post("/management/api/tasks", json=payload).status_code == code
    upstream.assert_not_called()


def test_csrf_size_and_content_type_guards(boundary):
    client, upstream, _, _ = boundary
    assert client.post("/management/api/tasks", json={}, headers={"Origin": "https://attacker.example"}).status_code == 403
    client.headers.pop("X-Management-Request")
    assert client.post("/management/api/tasks", json={}).status_code == 403
    client.headers["X-Management-Request"] = "1"
    assert client.post("/management/api/tasks", content="{}").status_code == 415
    assert client.post("/management/api/tasks", content="x" * 4097,
                       headers={"Content-Type": "application/json"}).status_code == 413
    upstream.assert_not_called()


def test_task_and_config_summaries_are_allowlisted(boundary):
    client, upstream, _, _ = boundary
    upstream.return_value = httpx.Response(200, json={"task_id": "task-1", "repo": "org/repo", "status": "completed",
        "result": {"pr_url": "https://github.com/org/repo/pull/8", "status": "candidate_failed",
                   "validation": {"status": "failed", "output": "private"},
                   "error": "candidate-validation-not-passed", "patch": "private", "token": "secret"},
        "payload": "private", "job": {"name": "worker-job-1", "env": "secret"}, "credential_ref": "secret"})
    detail = client.get("/management/api/tasks/task-1")
    assert detail.json()["pr_url"] == "https://github.com/org/repo/pull/8"
    assert detail.json()["execution"] == {"job_name": "worker-job-1", "worker_status": "candidate_failed",
                                           "validation_status": "failed", "error": "candidate-validation-not-passed",
                                           "report_failed": False}
    assert "secret" not in detail.text and "private" not in detail.text and "token" not in detail.text
    upstream.return_value = httpx.Response(200, json={"task_id": "task-1", "status": "failed",
        "result": {"error": "request failed https://user:token@example.com"},
        "job": {"name": "invalid/name", "env": "secret"}})
    unsafe = client.get("/management/api/tasks/task-1").json()["execution"]
    assert unsafe["error"] is None and unsafe["job_name"] is None
    upstream.return_value = httpx.Response(200, json={"task_id": "task-1", "status": "failed",
        "result": {"error": "A" * 40}})
    assert client.get("/management/api/tasks/task-1").json()["execution"]["error"] is None
    upstream.return_value = httpx.Response(200, json={"task_id": "task-1", "repo": "org/repo",
                                                       "result": {"pr_url": "https://evil.example"}})
    assert client.get("/management/api/tasks/task-1").json()["pr_url"] is None
    upstream.return_value = httpx.Response(200, json={"version": "1", "hash": "a" * 64,
        "models": [{"role": "coder", "model": "configured-coder", "availability": "unknown",
                    "native_download": True},
                   {"role": "judge", "model": "configured-judge", "availability": "unknown",
                    "native_download": False}],
        "prompts": {"worker": "", "planner": "", "judge": ""}, "agents": []})
    config = client.get("/management/api/config")
    assert config.status_code == 200
    assert config.json()["models"][0]["model"] == "configured-coder"
    assert all(model["availability"] == "unknown" for model in config.json()["models"])
    assert "private" not in config.text and "example.com" not in config.text
    assert config.headers["Cache-Control"] == "no-store"


def test_listing_and_upstream_failures(boundary):
    client, upstream, _, _ = boundary
    upstream.return_value = httpx.Response(200, json={"tasks": [{"task_id": "task-1", "payload": "secret"}],
                                                     "total": 1, "limit": 25, "offset": 0})
    response = client.get("/management/api/tasks?limit=25&status=running")
    assert response.json()["tasks"] == [{"task_id": "task-1"}]
    assert upstream.call_args.args[0].url.params["status"] == "running"
    assert client.get("/management/api/tasks?token=no").status_code == 400
    upstream.return_value = httpx.Response(500, text="secret")
    response = client.get("/management/api/tasks/task-1")
    assert response.status_code == 502 and "secret" not in response.text
    upstream.return_value = httpx.Response(302, headers={"Location": "https://attacker.example"})
    assert client.get("/management/api/tasks/task-1").status_code == 502


def test_config_changes_require_same_origin_and_forward_versioned_drafts(boundary):
    client, upstream, _, _ = boundary
    draft = {"expected_hash": "a" * 64, "models": {"coder": "new/coder", "judge": "new/judge"},
             "prompts": {"worker": "Prefer concise edits.", "planner": "", "judge": ""}}
    upstream.return_value = httpx.Response(200, json={"valid": True})
    validated = client.post("/management/api/config/validate", json=draft)
    assert validated.status_code == 200 and validated.json() == {"valid": True}
    request = upstream.call_args.args[0]
    assert request.url.path == "/api/v1/config/validate"
    assert json.loads(request.content) == draft

    client.headers.pop("Origin")
    calls = upstream.call_count
    assert client.post("/management/api/config/apply", json=draft).status_code == 403
    assert upstream.call_count == calls
    client.headers["Origin"] = "https://factory.example"

    upstream.return_value = httpx.Response(409, json={"error": "config-version-conflict"})
    conflict = client.post("/management/api/config/apply", json=draft)
    assert conflict.status_code == 409
    apply_request = upstream.call_args.args[0]
    assert apply_request.url.path == "/api/v1/config/apply"
    assert json.loads(apply_request.content) == {"draft": draft, "actor": "operator"}


def test_coordination_summary_allowlists_feedback_and_review_score(boundary):
    client, upstream, _, _ = boundary
    upstream.return_value = httpx.Response(200, json={
        "task_id": "task-1", "status": "completed",
        "coordination": {"plan_recorded": True, "plan": "private",
                         "latest_feedback": {"stage": "judge", "attempt": 2, "reason": "private",
                                             "review_topics": ["tests", "private", "security", "tests"],
                                             "review": "secret"}},
        "result": {"score": 0.75, "review": "secret", "patch": "private"},
    })
    response = client.get("/management/api/tasks/task-1")
    assert response.json()["coordination"] == {
        "plan_recorded": True, "judge_score": 0.75,
        "latest_feedback": {"stage": "judge", "attempt": 2, "review_topics": ["tests", "security"]},
    }
    assert "secret" not in response.text and "private" not in response.text


def test_task_events_expose_only_bounded_operational_fields(boundary):
    client, upstream, _, _ = boundary
    upstream.return_value = httpx.Response(200, json={
        "task_id": "task-1", "status": "failed", "events": [{
            "timestamp": 1760000000, "attempt": 2, "actor": "judge", "action": "review",
            "outcome": "rejected", "reason": "judge-rejected", "score": 0.4,
            "review_topics": ["tests", "security", "private"], "review": "secret review",
            "prompt": "secret prompt", "credential": "token",
            "failure_details": {
                "stage": "validation", "code": "nonzero-exit", "exit_status": 1,
                "duration_ms": 1234, "output_bytes": 4096, "issue_text": "private issue",
                "source": "private source", "path": "/private/path", "output": "raw validation output",
                "traceback": "private traceback", "credential": "private credential",
            },
        }, {
            "timestamp": 1760000001, "attempt": 3, "actor": "worker", "action": "candidate",
            "failure_details": {"stage": "validation", "code": "timeout", "exit_status": 1},
        }, {
            "timestamp": 1760000002, "attempt": 4, "actor": "worker", "action": "candidate",
            "failure_details": {
                "stage": "validation", "code": "nonzero-exit", "exit_status": 256,
                "duration_ms": True, "output_bytes": 65537,
            },
        }, {
            "timestamp": 1760000003, "attempt": 9, "actor": "worker", "action": "candidate",
            "outcome": "returned", "reason": "private payload",
        }, {
            "timestamp": 1760000004, "attempt": 5, "actor": "judge", "action": "review",
            "outcome": "rejected", "reason": "judge-rejected",
            "failure_details": {"stage": "judge", "code": "review-rejected",
                                "summary": "Missing tests and incorrect logic."},
        }, {
            "timestamp": 1760000005, "attempt": 5, "actor": "worker", "action": "candidate",
            "outcome": "failed", "reason": "no-op-edit",
            "failure_details": {"stage": "judge", "code": "review-rejected",
                                "summary": "Worker copied private source."},
        }],
    })
    response = client.get("/management/api/tasks/task-1")
    assert response.json()["events"] == [{
        "timestamp": 1760000000, "attempt": 2, "actor": "judge", "action": "review",
        "outcome": "rejected", "reason": "judge-rejected", "validation_status": None,
        "score": 0.4, "review_topics": ["tests", "security"],
        "failure_details": {
            "stage": "validation", "code": "nonzero-exit", "exit_status": 1,
            "duration_ms": 1234, "output_bytes": 4096,
        },
    }, {
        "timestamp": 1760000001, "attempt": 3, "actor": "worker", "action": "candidate",
        "outcome": "recorded", "reason": None, "validation_status": None,
        "score": None, "review_topics": [], "failure_details": None,
    }, {
        "timestamp": 1760000002, "attempt": 4, "actor": "worker", "action": "candidate",
        "outcome": "recorded", "reason": None, "validation_status": None,
        "score": None, "review_topics": [],
        "failure_details": {"stage": "validation", "code": "nonzero-exit"},
    }, {
        "timestamp": 1760000004, "attempt": 5, "actor": "judge", "action": "review",
        "outcome": "rejected", "reason": "judge-rejected", "validation_status": None,
        "score": None, "review_topics": [],
        "failure_details": {"stage": "judge", "code": "review-rejected",
                            "summary": "Missing tests and incorrect logic."},
    }, {
        "timestamp": 1760000005, "attempt": 5, "actor": "worker", "action": "candidate",
        "outcome": "failed", "reason": "no-op-edit", "validation_status": None,
        "score": None, "review_topics": [], "failure_details": None,
    }]
    for forbidden in ("secret", "token", "private", "raw validation output", "Worker copied"):
        assert forbidden not in response.text


@pytest.mark.parametrize("coordination,score", [
    (None, None), ({}, "0.9"), ([], True),
    ({"plan_recorded": "secret", "latest_feedback": []}, -1),
    ({"latest_feedback": {"stage": "secret", "attempt": 1}}, 2),
    ({"latest_feedback": {"stage": "judge", "attempt": True}}, None),
    ({"latest_feedback": {"stage": "judge", "attempt": 9}}, None),
])
def test_coordination_summary_handles_missing_and_invalid_records(boundary, coordination, score):
    client, upstream, _, _ = boundary
    upstream.return_value = httpx.Response(200, json={
        "task_id": "task-1", "coordination": coordination, "result": {"score": score},
    })
    assert client.get("/management/api/tasks/task-1").json()["coordination"] == {
        "plan_recorded": False, "latest_feedback": None, "judge_score": None,
    }


@pytest.mark.parametrize("score", [0, 1])
def test_judge_score_accepts_range_boundaries(boundary, score):
    client, upstream, _, _ = boundary
    upstream.return_value = httpx.Response(200, json={"task_id": "task-1", "result": {"score": score}})
    assert client.get("/management/api/tasks/task-1").json()["coordination"]["judge_score"] == score


def test_no_configuration_or_deployment_mutations(boundary):
    client, upstream, _, _ = boundary
    for path in ("/management/api/config", "/management/api/deploy", "/management/api/tasks/task-1/retry"):
        assert client.post(path, json={}).status_code == 405
    assert client.post("/management/api/config/validate", json={}).status_code == 400
    assert client.post("/management/api/config/apply", json={}).status_code == 400
    upstream.assert_not_called()


@pytest.mark.parametrize("auth_mode", ["admin", "oidc"])
@pytest.mark.parametrize("github_status", [200, 401, 403, 404, 503])
def test_adapter_to_real_gateway_contract(boundary, monkeypatch, tmp_path, auth_mode, github_status):
    _, _, _, settings = boundary
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "gateway"))
    import server as gateway
    monkeypatch.setenv("MANAGEMENT_SERVICE_TOKEN", settings["MANAGEMENT_SERVICE_TOKEN"])
    app_auth = Mock()
    app_auth.policy.return_value = {"repository": "org/repo"}
    app_auth.mint.return_value = {"token": "repository-test-secret"}
    monkeypatch.setattr(gateway, "_github_app", lambda: app_auth)
    monkeypatch.setenv("TASK_DB_PATH", str(tmp_path / "tasks.sqlite3"))
    monkeypatch.setenv("GATEWAY_TRANSPORT_MODE", "local-http")
    chart_files = Path(__file__).resolve().parents[2] / "helm/orchestrator/files"
    catalog = (chart_files / "agent-catalog.yaml").read_text()
    catalog_schema = (chart_files / "agent-catalog.schema.json").read_text()
    runtime = json.dumps({"version": "1", "catalog": catalog, "coder_model": "coder-model",
                          "judge_model": "judge-model", "coder_native_download": False,
                          "judge_native_download": False, "prompts": {"worker": "", "planner": "", "judge": ""}})
    config_hash = gateway.hashlib.sha256((catalog + catalog_schema + runtime).encode()).hexdigest()
    for name, value in (("AGENT_CONFIG_HASH", config_hash), ("AGENT_CONFIG_VERSION", "1"),
                        ("AGENT_CATALOG_YAML", catalog), ("AGENT_CATALOG_SCHEMA_JSON", catalog_schema),
                        ("AGENT_RUNTIME_CONFIG_JSON", runtime)):
        monkeypatch.setenv(name, value)
    def github(method, path, *, token):
        assert method == "GET" and token == "repository-test-secret"
        if path == "/repos/org/repo":
            return {"full_name": "org/repo", "permissions": {"pull": True}}
        if github_status != 200:
            raise urllib.error.HTTPError("https://api.github.com" + path, github_status, "denied", {}, None)
        if path == "/repos/org/repo/issues/7":
                return {"number": 7, "html_url": "https://github.com/org/repo/issues/7", "labels": []}
        return []

    monkeypatch.setattr(gateway, "_github", Mock(side_effect=github))
    monkeypatch.setattr(gateway, "_validate_repository", Mock(wraps=gateway._validate_repository))
    monkeypatch.setattr(gateway, "_create_task_secret", Mock(return_value="test-uid"))
    http_server = gateway.ThreadingHTTPServer(("127.0.0.1", 0), gateway.Handler)
    thread = threading.Thread(target=http_server.serve_forever, daemon=True)
    thread.start()
    settings["MANAGEMENT_GATEWAY_URL"] = f"http://127.0.0.1:{http_server.server_port}"
    from jwt import PyJWK
    public_key = jwt.algorithms.RSAAlgorithm.from_jwk(jwt.algorithms.RSAAlgorithm.to_jwk(
        boundary[0].app.dependency_overrides.get("unused", rsa.generate_private_key(public_exponent=65537, key_size=2048)).public_key()))
    del public_key, PyJWK
    original_keys = boundary[0].app
    del original_keys
    try:
        settings.update({"MANAGEMENT_AUTH_MODE": auth_mode, "MANAGEMENT_ADMIN_PASSWORD": "abcdefg!"})
        app = create_app(settings)
        identity_dependency = next(route for route in app.routes if route.path == "/management/api/session").dependant.dependencies[0].call
        if auth_mode == "oidc":
            app.dependency_overrides[identity_dependency] = lambda: "operator"
        client = TestClient(app)
        if auth_mode == "admin":
            client.auth = ("admin", "abcdefg!")
        client.headers.update({"Origin": settings["MANAGEMENT_PUBLIC_ORIGIN"], "X-Management-Request": "1"})
        connect_github(client)
        accepted = client.post("/management/api/tasks", json={"repo": "org/repo", "issue_number": 7})
        if github_status != 200:
            assert accepted.status_code == 403
            gateway._create_task_secret.assert_not_called()
            assert client.get("/management/api/tasks").json()["total"] == 0
            return
        assert accepted.status_code == 202
        task_id = accepted.json()["task_id"]
        assert client.get("/management/api/tasks").json()["tasks"][0]["task_id"] == task_id
        assert client.get("/management/api/tasks/" + task_id).json()["issue_number"] == 7
        db = gateway._task_db()
        try:
            db.execute("UPDATE tasks SET plan=?, retry_feedback=? WHERE task_id=?", (
                "private plan", json.dumps({"stage": "judge", "attempt": 1,
                                            "review_topics": ["tests"], "review": "secret review"}), task_id,
            ))
            db.commit()
        finally:
            db.close()
        detail = client.get("/management/api/tasks/" + task_id)
        assert detail.json()["coordination"] == {
            "plan_recorded": True, "judge_score": None,
            "latest_feedback": {"stage": "judge", "attempt": 1, "review_topics": ["tests"]},
        }
        assert "private plan" not in detail.text and "secret review" not in detail.text
        assert client.post("/management/api/tasks", json={"repo": "org/repo", "issue_number": 7}).json()["task_id"] == task_id
        gateway._validate_repository.assert_called_with("org/repo", 7, "repository-test-secret")
    finally:
        http_server.shutdown()
        http_server.server_close()
        thread.join()


@pytest.mark.parametrize("command", ["template", "lint"])
@pytest.mark.parametrize("gateway_url,shared", [("", True), ("https://factory.example/", True),
                                               ("https://gateway.example", False)])
def test_management_helm_shared_origin_routes_gateway(tmp_path, command, gateway_url, shared):
    chart = Path(__file__).resolve().parents[2] / "helm" / "orchestrator"
    values = tmp_path / "management.yaml"
    values.write_text(yaml.safe_dump({**HELM_TEST_VALUES,
        "supervisor": {"config": {"acceptor-url": "https://factory.example"}},
        "management": {"enabled": True, "publicOrigin": "https://factory.example",
                       "gatewayUrl": gateway_url, "admin": {"username": "admin"},
                       "ingress": {"enabled": True, "host": "factory.example",
                                   "tlsTermination": "external"}}}))
    arguments = ["helm", command, *(["test"] if command == "template" else []), str(chart), "-f", str(values)]
    rendered = subprocess.run(arguments, text=True, capture_output=True, check=True).stdout
    if command == "lint":
        return
    ingress = next(document for document in yaml.safe_load_all(rendered) if document
                   and document["kind"] == "Ingress" and document["metadata"]["name"].endswith("-management-ingress"))
    paths = {path["path"]: path["backend"]["service"] for path in ingress["spec"]["rules"][0]["http"]["paths"]}
    assert paths["/"] == {"name": "orchestrator-management", "port": {"number": 8081}}
    auth_routes = [path for path in ingress["spec"]["rules"][0]["http"]["paths"]
                   if path["pathType"] == "Prefix" and "/auth".startswith(path["path"])]
    assert len(auth_routes) == 1
    assert auth_routes[0]["backend"]["service"] == paths["/"]
    assert ("/api/v1" in paths) is shared
    if shared:
        assert paths["/api/v1"] == {"name": "orchestrator-orchestrator-gateway", "port": {"number": 8000}}


def test_management_helm_opt_in_and_snapshot(tmp_path):
    chart = Path(__file__).resolve().parents[2] / "helm" / "orchestrator"
    values = tmp_path / "management.yaml"
    values.write_text(yaml.safe_dump(HELM_TEST_VALUES))
    base = ["helm", "template", "test", str(chart), "-f", str(values)]
    default = subprocess.run(base, text=True, capture_output=True, check=True).stdout
    assert "component: management" not in default
    assert subprocess.run([*base, "--set", "management.enabled=true"], capture_output=True, check=False).returncode != 0
    values.write_text(yaml.safe_dump({**HELM_TEST_VALUES,
        "management": {"enabled": True, "authMode": "oidc", "image": "example/management:test",
        "publicOrigin": "https://factory.example",
        "oidc": {"issuer": "https://identity.example", "audience": "management",
                 "jwksUrl": "https://identity.example/keys", "allowedSubjects": ["operator"]}}}))
    rendered = subprocess.run(base, text=True, capture_output=True, check=True).stdout
    documents = [document for document in yaml.safe_load_all(rendered) if document]
    deployment = next(document for document in documents if document["kind"] == "Deployment" and document["metadata"]["name"].endswith("-management"))
    pod = deployment["spec"]["template"]["spec"]
    assert pod["automountServiceAccountToken"] is False
    assert pod["containers"][0]["securityContext"]["readOnlyRootFilesystem"] is True
    assert not any(document["kind"] == "Secret" for document in documents)
    assert not any(document["kind"] == "Ingress" and "management" in document["metadata"]["name"] for document in documents)
    snapshot = next(document for document in documents if document["kind"] == "ConfigMap" and "runtime-config.json" in document.get("data", {}))
    runtime = json.loads(snapshot["data"]["runtime-config.json"])
    assert runtime["coder_model"] and runtime["judge_model"]
    assert not pod.get("volumes")


@pytest.mark.parametrize("command", ["template", "lint"])
def test_management_helm_admin_password_uses_external_ref(tmp_path, command):
    chart = Path(__file__).resolve().parents[2] / "helm" / "orchestrator"
    values = tmp_path / "management.yaml"
    refs = {**SECRET_REFS, "managementAdminPassword": {
        "name": "external-admin-password", "key": "password",
    }}
    values.write_text(yaml.safe_dump({**HELM_TEST_VALUES, "secretRefs": refs, "management": {
        "enabled": True, "publicOrigin": "https://factory.example", "image": "example/management:test",
        "authMode": "admin", "admin": {"username": "admin"}}}))
    arguments = ["helm", command, *(["test"] if command == "template" else []), str(chart), "-f", str(values)]
    result = subprocess.run(arguments, text=True, capture_output=True, check=False)
    assert result.returncode == 0, result.stderr + result.stdout
    if command == "template":
        documents = [document for document in yaml.safe_load_all(result.stdout) if document]
        assert not any(document["kind"] == "Secret" for document in documents)
        deployment = next(document for document in documents if document["kind"] == "Deployment"
                          and document["metadata"]["name"] == "orchestrator-management")
        password_env = next(entry for entry in deployment["spec"]["template"]["spec"]["containers"][0]["env"]
                            if entry["name"] == "MANAGEMENT_ADMIN_PASSWORD")
        assert password_env["valueFrom"]["secretKeyRef"] == refs["managementAdminPassword"]