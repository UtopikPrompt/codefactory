import subprocess
from pathlib import Path

import pytest
import yaml

CHART = Path(__file__).resolve().parents[1] / "helm/orchestrator"
WORKFLOW = CHART.parents[1] / ".github/workflows/ci.yml"


def test_namespace_is_not_a_chart_value():
    for filename in ("values.yaml", "example.values.yaml"):
        assert "namespace" not in yaml.safe_load((CHART / filename).read_text())
    schema = yaml.safe_load((CHART / "values.schema.json").read_text())
    assert "namespace" not in schema["properties"]


def test_main_packages_are_published_with_mutable_main_tags():
    workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
    publish_chart = workflow["jobs"]["publish-orchestrator-chart"]
    package_step = next(
        step for step in publish_chart["steps"] if step.get("id") == "package-and-push")
    assert "helm package helm/orchestrator --version main" in package_step["run"]

    build_images = workflow["jobs"]["build-images"]
    image_step = next(
        step for step in build_images["steps"] if step.get("name") == "Build and push")
    assert "codefactory-${{ matrix.service }}:main" in image_step["with"]["tags"]


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