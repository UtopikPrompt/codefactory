import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

CHART = Path(__file__).resolve().parents[2] / "helm/orchestrator"
WORKFLOW = CHART.parents[1] / ".github/workflows/ci.yml"


def _matrix_environments(workflow):
    matrix = workflow["jobs"]["test"]["strategy"]["matrix"]
    entries = [{"slice": name} for name in matrix["slice"]]
    defaults = workflow["env"]
    environments = []
    for entry in entries:
        environments.append({
            "TEST_REQUIREMENTS": defaults["TEST_REQUIREMENTS_FORMAT"].format(entry["slice"]),
            "TEST_PATH": defaults["TEST_PATH_FORMAT"].format(entry["slice"]),
            "TEST_ARTIFACT": f"python-test-results-{entry['slice']}",
        })
    return entries, environments


@pytest.mark.parametrize("service", ["supervisor", "worker", "gateway", "management", "shared"])
@pytest.mark.parametrize("report_exists", [False, True])
def test_ci_workflow_reporting(tmp_path, report_exists, service):
    workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
    jobs = workflow["jobs"]
    assert workflow["permissions"] == {"contents": "read"}
    assert jobs["lint"]["name"] == "Lint Python"
    assert jobs["test"]["name"] == "test-${{ matrix.slice }}"
    assert jobs["chart-validation"]["name"] == "Validate Helm chart"
    assert jobs["publish-chart"]["needs"] == ["lint", "test", "chart-validation"]
    assert "HELM_CHART_PUBLISH_ENABLED == 'true'" in jobs["publish-chart"]["if"]
    assert set(jobs["summary"]["needs"]) == set(jobs) - {"summary"}
    assert jobs["summary"]["if"] == "always()"
    for job in jobs.values():
        for step in job["steps"]:
            assert step.get("name")
            if "run" not in step:
                continue
            script = step["run"]
            if step.get("shell") == "python":
                compile(script, "<workflow>", "exec")
            else:
                subprocess.run(
                    ["bash", "-n"], input=re.sub(r"\$\{\{.*?\}\}", "ci-value", script),
                    text=True, check=True, capture_output=True,
                )
    steps = {step["name"]: step for step in jobs["test"]["steps"]}
    assert steps["Run tests"]["id"] == "pytest"
    assert steps["Run tests"]["run"] == (
        'mkdir -p test-results\npytest "$TEST_PATH" -v -ra --durations=10 --junitxml=test-results/pytest.xml\n'
    )
    assert steps["Summarize test results"]["if"] == "always()"
    upload = steps["Upload test results"]
    assert upload["if"] == "always() && hashFiles('test-results/pytest.xml') != ''"
    assert upload["with"] == {
        "name": "${{ format('python-test-results-{0}', matrix.slice) }}",
        "path": "test-results/pytest.xml", "retention-days": "14",
    }
    assert steps["Summarize test results"]["env"]["TEST_ARTIFACT"] == upload["with"]["name"]
    if report_exists:
        report = tmp_path / "test-results/pytest.xml"
        report.parent.mkdir()
        report.write_text(
            '<testsuites><testsuite tests="3" failures="1" errors="0" skipped="1" time="1.5">'
            '<testcase classname="module" name="passes" time="0.5"/>'
            '<testcase classname="module" name="fails" time="0.5">'
            '<failure message="bad | &lt;value&gt;"/></testcase>'
            '<testcase classname="module" name="skips" time="0.5">'
            '<skipped message="optional"/></testcase></testsuite></testsuites>',
            encoding="utf-8",
        )
    summary = tmp_path / "summary.md"
    environment = {
        **os.environ, "GITHUB_STEP_SUMMARY": str(summary), "TEST_OUTCOME": "failure",
        "TEST_ARTIFACT": f"python-test-results-{service}",
    }
    subprocess.run(
        [sys.executable, "-c", steps["Summarize test results"]["run"]],
        cwd=tmp_path, env=environment, check=True,
    )
    text = summary.read_text()
    if report_exists:
        assert "| 3 | 1 | 1 | 0 | 1 | 1.50s |" in text
        assert "bad &#124; &lt;value&gt;" in text
        assert f"**python-test-results-{service}** artifact" in text
    else:
        assert "No test report was produced" in text
    environment.update({
        "JOB_RESULTS": json.dumps({"test": {"result": "failure"}, "publish-chart": {"result": "skipped"}}),
        "SOURCE_COMMIT": "a" * 40, "GITHUB_SERVER_URL": "https://github.com",
        "GITHUB_REPOSITORY": "owner/product", "GITHUB_RUN_ID": "123",
    })
    subprocess.run(
        [sys.executable, "-c", jobs["summary"]["steps"][0]["run"]],
        cwd=tmp_path, env=environment, check=True,
    )
    text = summary.read_text()
    assert "| test | failure |" in text
    assert "| publish-chart | skipped |" in text
    assert "https://github.com/owner/product/actions/runs/123" in text


@pytest.mark.parametrize("service", ["supervisor", "worker", "gateway", "management", "shared"])
def test_ci_workflow_matrix_partition_and_dependencies(tmp_path, service):
    workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
    jobs = workflow["jobs"]
    test_job = jobs["test"]
    services = ["supervisor", "worker", "gateway", "management", "shared"]
    assert test_job["strategy"]["fail-fast"] == "false"
    matrix = test_job["strategy"]["matrix"]
    assert set(matrix) == {"slice"}
    assert matrix["slice"] == services
    entries, environments = _matrix_environments(workflow)
    assert [entry["slice"] for entry in entries] == services
    assert len(entries) == 5
    effective = environments[services.index(service)]
    assert "TEST_PACKAGES" not in workflow["env"]
    assert "TEST_DISCOVERY" not in workflow["env"]
    assert workflow["env"]["TEST_PATH_FORMAT"] == "orchestrator/{0}"
    assert workflow["env"]["TEST_REQUIREMENTS_FORMAT"] == "orchestrator/{0}/requirements-test.txt"
    repository = WORKFLOW.parents[2]
    all_tests = {path for pattern in ("test_*.py", "*_test.py")
                 for path in (repository / "orchestrator").rglob(pattern)}
    slice_tests = [path for environment in environments
                   for path in (repository / environment["TEST_PATH"]).rglob("*.py")
                   if path.match("test_*.py") or path.match("*_test.py")]
    assert len(slice_tests) == len(set(slice_tests))
    assert set(slice_tests) == all_tests
    assert Path(__file__).resolve() in all_tests
    assert effective["TEST_PATH"] == f"orchestrator/{service}"
    assert not (repository / "orchestrator/requirements-test.txt").exists()
    expected_requirements = ["pytest", "PyYAML>=6,<7"] if service == "shared" else ["-r requirements.txt"]
    if service == "management":
        expected_requirements.append("-r ../gateway/requirements.txt")
    if service != "shared":
        expected_requirements.append("pytest")
    if service == "gateway":
        expected_requirements.append("PyYAML>=6,<7")
    requirements_path = WORKFLOW.parents[2] / effective["TEST_REQUIREMENTS"]
    assert requirements_path.read_text().splitlines() == expected_requirements
    for requirement in expected_requirements:
        if requirement.startswith("-r "):
            assert (requirements_path.parent / requirement[3:]).is_file()
    for downstream in ("publish-chart", "build-images"):
        assert "test" in jobs[downstream]["needs"]
    steps = {step["name"]: step for step in test_job["steps"]}
    assert steps["Set up Helm"]["uses"] == "azure/setup-helm@v4"
    assert steps["Install dependencies"]["env"] == {
        "TEST_REQUIREMENTS": "${{ format(env.TEST_REQUIREMENTS_FORMAT, matrix.slice) }}",
    }
    assert steps["Install dependencies"]["run"] == (
        'python -m pip install --upgrade pip\npython -m pip install -r "$TEST_REQUIREMENTS"\n'
    )
    assert steps["Run tests"]["env"] == {
        "TEST_PATH": "${{ format(env.TEST_PATH_FORMAT, matrix.slice) }}",
    }
    assert steps["Upload test results"]["with"]["name"] == "${{ format('python-test-results-{0}', matrix.slice) }}"
    assert [environment["TEST_ARTIFACT"] for environment in environments] == [
        f"python-test-results-{name}" for name in services
    ]

    binaries = tmp_path / "bin"
    binaries.mkdir()
    log = tmp_path / "commands.jsonl"
    for command in ("python", "pytest"):
        executable = binaries / command
        executable.write_text(
            f"#!{sys.executable}\n"
            "import json, os, sys\n"
            "from pathlib import Path\n"
            "with open(os.environ['COMMAND_LOG'], 'a') as log:\n"
            "    log.write(json.dumps([Path(sys.argv[0]).name, *sys.argv[1:]]) + '\\n')\n",
            encoding="utf-8",
        )
        executable.chmod(0o755)
    environment = {
        **os.environ, "PATH": f"{binaries}:{os.environ['PATH']}",
        "COMMAND_LOG": str(log), **effective,
    }
    for step in ("Install dependencies", "Run tests"):
        subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", steps[step]["run"]],
            cwd=tmp_path, env=environment, check=True, capture_output=True, text=True,
        )
    commands = [json.loads(line) for line in log.read_text().splitlines()]
    assert commands[:2] == [
        ["python", "-m", "pip", "install", "--upgrade", "pip"],
        ["python", "-m", "pip", "install", "-r", effective["TEST_REQUIREMENTS"]],
    ]
    assert len(commands) == 3
    test_command = commands[2]
    assert test_command[0] == "pytest"
    assert test_command[-4:] == ["-v", "-ra", "--durations=10", "--junitxml=test-results/pytest.xml"]
    paths = [Path(path) for path in test_command[1:-4]]
    assert paths == [Path("orchestrator") / service]

    environment.update({
        "TEST_REQUIREMENTS": "requirements with spaces $(touch injected).txt",
        "TEST_PATH": "tests with spaces",
    })
    for step in ("Install dependencies", "Run tests"):
        subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", steps[step]["run"]],
            cwd=tmp_path, env=environment, check=True, capture_output=True, text=True,
        )
    commands = [json.loads(line) for line in log.read_text().splitlines()]
    assert commands[-2] == [
        "python", "-m", "pip", "install", "-r", "requirements with spaces $(touch injected).txt",
    ]
    assert commands[-1][1:-4] == ["tests with spaces"]
    assert not (tmp_path / "injected").exists()


def test_ci_workflow_helm_configuration(tmp_path):
    workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
    jobs = workflow["jobs"]
    prepare = [next(step["run"] for step in jobs[job]["steps"]
                    if step["name"] == "Prepare Helm dependencies")
               for job in ("test", "chart-validation", "publish-chart")]
    assert len(set(prepare)) == 1
    binaries = tmp_path / "bin"
    binaries.mkdir()
    log = tmp_path / "commands.jsonl"
    executable = binaries / "helm"
    executable.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "with open(os.environ['COMMAND_LOG'], 'a') as log:\n"
        "    log.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "if sys.argv[1] == 'template':\n"
        "    print(os.environ.get('RENDERED_IMAGES', ''))\n",
        encoding="utf-8",
    )
    executable.chmod(0o755)
    environment = {
        **os.environ, **workflow["env"], "PATH": f"{binaries}:{os.environ['PATH']}",
        "COMMAND_LOG": str(log), "RENDERED_IMAGES": workflow["env"]["CHART_EXPECTED_IMAGES"],
    }
    script = next(step["run"] for step in jobs["chart-validation"]["steps"]
                  if step["name"] == "Lint and render Helm chart")
    for command in (prepare[0], script):
        subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", command],
            cwd=tmp_path, env=environment, check=True, capture_output=True, text=True,
        )
    commands = [json.loads(line) for line in log.read_text().splitlines()]
    assert commands == [
        ["lint", "helm/orchestrator", "--values", "helm/orchestrator/example.values.yaml"],
        ["template", "orchestrator", "helm/orchestrator", "--namespace", "orchestrator",
         "--values", "helm/orchestrator/example.values.yaml", "--set", "management.enabled=true"],
    ]
    assert environment["CHART_EXPECTED_IMAGES"].splitlines() == [
        f"ghcr.io/utopikprompt/codefactory-{service}:latest"
        for service in ("gateway", "management", "supervisor", "worker")
    ]
    environment["RENDERED_IMAGES"] = ""
    result = subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", script],
        cwd=tmp_path, env=environment, capture_output=True, text=True, check=False,
    )
    assert result.returncode != 0
    assert "Expected image missing" in result.stderr


def test_namespace_is_not_a_chart_value():
    for filename in ("values.yaml", "example.values.yaml"):
        assert "namespace" not in yaml.safe_load((CHART / filename).read_text())
    schema = yaml.safe_load((CHART / "values.schema.json").read_text())
    assert "namespace" not in schema["properties"]


def test_empty_image_pull_secrets_renders(tmp_path):
    values = yaml.safe_load((CHART / "example.values.yaml").read_text())
    values["imagePullSecrets"] = []
    values_file = tmp_path / "values.yaml"
    values_file.write_text(yaml.safe_dump(values), encoding="utf-8")
    result = subprocess.run(
        ["helm", "template", "orchestrator", str(CHART), "--namespace", "orchestrator",
         "--values", str(values_file), "--set", "management.enabled=true"],
        check=False, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "imagePullSecrets:\n        []" in result.stdout


def test_empty_github_auth_renders_and_invalid_values_fail(tmp_path):
    values = yaml.safe_load((CHART / "example.values.yaml").read_text())
    values["githubAuth"].update(appId="", audience="")
    values_file = tmp_path / "values.yaml"
    values_file.write_text(yaml.safe_dump(values), encoding="utf-8")
    base = ["helm", "template", "orchestrator", str(CHART), "--namespace", "orchestrator",
            "--values", str(values_file), "--set", "management.enabled=true"]
    result = subprocess.run(base, check=False, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    for setting in ("githubAuth.appId=abc", "githubAuth.audience=http://x"):
        result = subprocess.run([*base, "--set", setting], check=False, capture_output=True, text=True)
        assert result.returncode != 0, setting


@pytest.mark.parametrize("event", ["push", "workflow_dispatch", "pull_request", "release"])
@pytest.mark.parametrize("ref", ["refs/heads/main", "refs/tags/v1.2.3", "refs/heads/other", "refs/pull/1/merge"])
@pytest.mark.parametrize("enabled", ["true", "false", "TRUE"])
@pytest.mark.parametrize("validated", [True, False])
def test_chart_publish_gate(event, ref, enabled, validated):
    job = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)["jobs"]["publish-chart"]
    expression = " ".join(job["if"].removeprefix("${{").removesuffix("}}").split())
    assert expression == (
        "success() && (github.event_name == 'push' || github.event_name == 'workflow_dispatch') && "
        "(github.ref == 'refs/heads/main' || startsWith(github.ref, 'refs/tags/')) && "
        "vars.HELM_CHART_PUBLISH_ENABLED == 'true'"
    )
    expression = expression.replace("startsWith(github.ref, 'refs/tags/')", str(ref.startswith("refs/tags/")))
    for key, value in {
        "success()": validated, "github.event_name": event, "github.ref": ref,
        "vars.HELM_CHART_PUBLISH_ENABLED": enabled,
    }.items():
        expression = expression.replace(key, repr(value))
    actual = eval(expression.replace("&&", "and").replace("||", "or"), {"__builtins__": {}})
    assert actual == (validated and event in ("push", "workflow_dispatch")
                      and (ref == "refs/heads/main" or ref.startswith("refs/tags/")) and enabled == "true")
    assert job["needs"] == ["lint", "test", "chart-validation"]


@pytest.mark.parametrize("ref,version", [
    ("refs/heads/main", "0.0.0-dev"),
    ("refs/tags/v1.2.3", "1.2.3"), ("refs/tags/1.2.3", "1.2.3"),
    ("refs/tags/v0.0.0", "0.0.0"), ("refs/tags/10.20.30", "10.20.30"),
    ("refs/tags/v0.0.0-dev", "0.0.0-dev"), ("refs/tags/1.2.3-rc.1", "1.2.3-rc.1"),
    *[("refs/tags/" + tag, None) for tag in (
        "v01.2.3", "1.02.3", "1.2.03", "v1.2", "vv1.2.3",
        "v1.2.3-", "v1.2.3-.rc", "v1.2.3-rc.", "v1.2.3-rc..1", "v1.2.3-rc.01",
        "v1.2.3-01", "v1.2.3-rc_1",
        "1.2.3+build", "dev", "1.2.3/other", "1.2.3\ninjected", "",
    )],
    ("refs/heads/other", None), ("refs/pull/1/merge", None),
])
def test_chart_version_selection(tmp_path, ref, version):
    job = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)["jobs"]["publish-chart"]
    selector = job["steps"][0]
    assert selector["id"] == "chart-version"
    output = tmp_path / "output"
    result = subprocess.run(
        ["bash", "-euo", "pipefail", "-c", selector["run"]], check=False, capture_output=True, text=True,
        env={**os.environ, "GITHUB_REF": ref, "GITHUB_SHA": "a" * 40, "GITHUB_OUTPUT": str(output)},
    )
    assert (result.returncode == 0) == (version is not None), result.stderr
    if version is None:
        assert not output.exists()
    else:
        assert dict(line.split("=", 1) for line in output.read_text().splitlines()) == {
            "package_version": version, "source_version": "0.0.0-sha." + "a" * 40,
        }


@pytest.mark.parametrize("tagged", [False, True])
@pytest.mark.parametrize("scenario", [
    "fresh", "same", "dev-exists", "mismatch", "release-exists", "pull-error", "bad-digest", "bad-revision", "digest-mismatch",
])
def test_chart_publisher_versions_and_verification(tmp_path, tagged, scenario):
    workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
    job = workflow["jobs"]["publish-chart"]
    steps = {step["name"]: step for step in job["steps"]}
    binaries = tmp_path / "bin"
    binaries.mkdir()
    stub = binaries / "publication-stub"
    stub.write_text(f"#!{sys.executable}\n" + '''import json, os, sys
from pathlib import Path
args = sys.argv[1:]
command = Path(sys.argv[0]).name
with open(os.environ["COMMAND_LOG"], "a") as log:
    log.write(json.dumps([command, *args]) + "\\n")
scenario = os.environ["SCENARIO"]
digest = "sha256:" + "b" * 64
if command == "oras":
    if args[0] == "resolve":
        print("sha256:" + "c" * 64 if scenario == "digest-mismatch" else digest)
    sys.exit(0)
if args[0] == "pull":
    version = args[args.index("--version") + 1]
    destination = Path(args[args.index("--destination") + 1])
    source = version.startswith("0.0.0-sha.")
    if scenario == "pull-error":
        sys.exit("unauthorized")
    exists = (Path(os.environ["RUNNER_TEMP"]) / ("pushed-" + version)).exists()
    if not exists and not (source and scenario in ("same", "mismatch")) and not (not source and scenario == "release-exists"):
        sys.exit("manifest unknown: 404 not found")
    (destination / (os.environ["CHART_NAME"] + "-" + version + ".tgz")).touch()
    print("Digest: " + ("invalid" if scenario == "bad-digest" else digest), file=sys.stderr)
elif args[:2] == ["show", "chart"]:
    revision = os.environ["SOURCE_COMMIT"]
    if scenario == "mismatch" or (scenario == "bad-revision" and "source-chart/" in args[2]):
        revision = "c" * 40
    print("annotations:\\n  org.opencontainers.image.revision: " + revision)
elif args[0] == "package":
    version = args[args.index("--version") + 1]
    destination = Path(args[args.index("--destination") + 1])
    (destination / (os.environ["CHART_NAME"] + "-" + version + ".tgz")).touch()
elif args[0] == "push":
    version = Path(args[1]).name.removeprefix(os.environ["CHART_NAME"] + "-").removesuffix(".tgz")
    (Path(os.environ["RUNNER_TEMP"]) / ("pushed-" + version)).touch()
else:
    sys.exit("Unexpected publication command")
''', encoding="utf-8")
    stub.chmod(0o755)
    for command in ("helm", "oras"):
        (binaries / command).symlink_to(stub)
    source_version = "0.0.0-sha." + "a" * 40
    if scenario == "dev-exists":
        (tmp_path / "pushed-0.0.0-dev").touch()
    output = tmp_path / "output"
    log = tmp_path / "commands.jsonl"
    environment = {
        **os.environ, **workflow["env"], **job["env"],
        "CHART_PATH": str(CHART), "RUNNER_TEMP": str(tmp_path), "SCENARIO": scenario,
        "COMMAND_LOG": str(log), "PATH": f"{Path(sys.executable).parent}:{binaries}:{os.environ['PATH']}",
        "GITHUB_REF": "refs/tags/v1.2.3" if tagged else "refs/heads/main",
        "SOURCE_COMMIT": "a" * 40, "SOURCE_CHART_VERSION": source_version,
        "PACKAGE_VERSION": "1.2.3" if tagged else "0.0.0-dev",
        "GITHUB_OUTPUT": str(output), "GITHUB_STEP_SUMMARY": str(tmp_path / "summary.md"),
    }
    result = subprocess.run(
        ["bash", "-euo", "pipefail", "-c", steps["Package and publish chart"]["run"]],
        env=environment, check=False, capture_output=True, text=True,
    )
    succeeds = scenario in ("fresh", "same", "dev-exists") or (scenario == "release-exists" and not tagged)
    assert (result.returncode == 0) == succeeds, result.stdout + result.stderr
    commands = [json.loads(line) for line in log.read_text().splitlines()]
    pushes = [command for command in commands if command[:2] == ["helm", "push"]]
    aliases = [command for command in commands if command[:2] == ["oras", "tag"]]
    assert len(aliases) == int(succeeds and not tagged)
    if aliases:
        assert aliases[0] == ["oras", "tag", f"{job['env']['CHART_OCI_REPOSITORY']}/{job['env']['CHART_NAME']}@sha256:" + "b" * 64, "dev"]
    if scenario in ("pull-error", "mismatch") and not tagged or scenario == "release-exists" and tagged:
        assert not pushes
    if not succeeds:
        assert not output.exists()
        assert not (tmp_path / "summary.md").exists()
        assert not any(Path(command[2]).name.endswith("-0.0.0-dev.tgz") for command in pushes)
        return
    assert len(pushes) == 1 + int(scenario != "same")
    expected_versions = ((["1.2.3"] if tagged else [])
                         + ([source_version] if scenario != "same" else [])
                         + ([] if tagged else ["0.0.0-dev"]))
    assert [Path(command[2]).name for command in pushes] == [
        f"{job['env']['CHART_NAME']}-{version}.tgz" for version in expected_versions
    ]
    assert all(command[3] == f"oci://{job['env']['CHART_OCI_REPOSITORY']}" for command in pushes)
    if not tagged:
        resolution = next(command for command in commands if command[:2] == ["oras", "resolve"])
        assert commands.index(resolution) < commands.index(pushes[-1]) < commands.index(aliases[0])
    else:
        assert not any("0.0.0-dev" in argument for command in commands for argument in command)
    assert yaml.safe_load((tmp_path / "chart-source/Chart.yaml").read_text())["annotations"][
        "org.opencontainers.image.revision"
    ] == "a" * 40
    assert not output.exists()
    summary = (tmp_path / "summary.md").read_text()
    label = "immutable" if tagged else "development"
    assert f"Published {label} chart version: `{environment['PACKAGE_VERSION']}`" in summary
    assert ("Published immutable chart version:" in summary) == tagged
    assert f"Source chart version: `{source_version}`" in summary
    assert f"Source digest: `sha256:{'b' * 64}`" in summary


def test_chart_publication_is_commit_addressed_and_refuses_overwrite():
    workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
    publish_chart = workflow["jobs"]["publish-chart"]
    version_step = next(
        step for step in publish_chart["steps"] if step.get("id") == "chart-version")
    package_step = next(
        step for step in publish_chart["steps"] if step["name"] == "Package and publish chart")
    assert 'source_version="0.0.0-sha.${GITHUB_SHA}"' in version_step["run"]
    assert package_step["env"]["PACKAGE_VERSION"] == "${{ steps.chart-version.outputs.package_version }}"
    assert 'publish_chart "$PACKAGE_VERSION" release' in package_step["run"]
    assert 'helm package "$chart_source" --version "$package_version"' in package_step["run"]
    assert 'org.opencontainers.image.revision' in package_step["run"]
    assert "Existing source chart $version does not map to" in package_step["run"]
    assert 'oras tag "$CHART_OCI_REPOSITORY/$CHART_NAME@$chart_digest" "$CHART_ALIAS"' \
        in package_step["run"]
    assert any(step.get("name") == "Set up ORAS" for step in publish_chart["steps"])
    login_step = next(
        step for step in publish_chart["steps"] if step.get("name") == "Log in to GHCR")
    assert 'oras login ghcr.io --username "$GITHUB_ACTOR" --password-stdin' \
        in login_step["run"]

    assert workflow["env"]["PUBLISH_IMAGES"] == (
        "${{ (github.event_name == 'push' || github.event_name == 'workflow_dispatch') && "
        "github.ref == 'refs/heads/main' && vars.GHCR_PUBLISH_ENABLED == 'true' }}"
    )
    build_images = workflow["jobs"]["build-images"]
    image_step = next(
        step for step in build_images["steps"] if step.get("name") == "Build and push")
    assert build_images["strategy"]["matrix"]["service"] == ["supervisor", "worker", "gateway", "management"]
    assert image_step["uses"] == "docker/build-push-action@v7"
    assert image_step["with"]["push"] == "${{ env.PUBLISH_IMAGES == 'true' }}"
    assert image_step["with"]["labels"] == "org.opencontainers.image.revision=${{ github.sha }}\n"
    assert image_step["with"]["tags"].splitlines() == [
        "${{ env.REGISTRY }}/utopikprompt/codefactory-${{ matrix.service }}:" + tag
        for tag in ("dev", "${{ needs.set-image-tag.outputs.tag }}", "${{ github.sha }}", "latest")
    ]


def test_ci_has_no_release_manifest_pipeline():
    workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
    assert "release-manifest" not in workflow["jobs"]
    assert "release-manifest" not in workflow["jobs"]["summary"]["needs"]
    assert "outputs" not in workflow["jobs"]["publish-chart"]
    for job_name in ("publish-chart", "build-images"):
        for step in workflow["jobs"][job_name]["steps"]:
            assert "manifest" not in step["name"].lower()
            assert not step.get("uses", "").startswith(("actions/upload-artifact@", "actions/download-artifact@"))
            assert "id" not in step or step["id"] == "chart-version"
    for obsolete in ("codefactory-chart-", "codefactory-image-", "codefactory-release-", "chart_digest:"):
        assert obsolete not in WORKFLOW.read_text()


@pytest.mark.parametrize("event", ["push", "workflow_dispatch", "pull_request", "release"])
@pytest.mark.parametrize("ref", ["refs/heads/main", "refs/tags/v1.2.3", "refs/heads/other", "refs/pull/1/merge"])
@pytest.mark.parametrize("enabled", ["true", "false", "TRUE"])
def test_image_publish_gate(event, ref, enabled):
    workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
    expression = workflow["env"]["PUBLISH_IMAGES"].removeprefix("${{").removesuffix("}}")
    for key, value in {
        "github.event_name": event, "github.ref": ref, "vars.GHCR_PUBLISH_ENABLED": enabled,
    }.items():
        expression = expression.replace(key, repr(value))
    actual = eval(expression.replace("&&", "and").replace("||", "or"), {"__builtins__": {}})
    assert actual == (event in ("push", "workflow_dispatch") and ref == "refs/heads/main" and enabled == "true")
    build_images = workflow["jobs"]["build-images"]
    assert build_images["needs"] == ["lint", "test", "chart-validation", "set-image-tag"]
    login_step = next(step for step in build_images["steps"] if step["name"] == "Log in to GHCR")
    assert login_step["if"] == "env.PUBLISH_IMAGES == 'true'"


@pytest.mark.parametrize("create", [False, True])
def test_release_namespace_controls_resources_and_rbac_subjects(create):
    rendered = subprocess.run(
        [
            "helm", "template", "orchestrator", str(CHART),
            "--namespace", "tenant-agents",
            "--values", str(CHART / "example.values.yaml"),
            "--set", f"namespaces.create={str(create).lower()}",
            "--set", "management.enabled=true",
            "--set", "management.ingress.enabled=true",
            "--set", "metrics.enabled=true",
        ],
        check=True, capture_output=True, text=True,
    )
    resources = [resource for resource in yaml.safe_load_all(rendered.stdout) if resource]
    assert sum(resource["kind"] == "Namespace" for resource in resources) == int(create)
    for resource in resources:
        if resource["kind"] == "Namespace":
            assert resource["metadata"]["name"] == "tenant-agents"
            assert resource["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"
        else:
            assert resource["metadata"]["namespace"] == "tenant-agents"
        for subject in resource.get("subjects", []):
            if subject["kind"] == "ServiceAccount":
                assert subject["namespace"] == "tenant-agents"