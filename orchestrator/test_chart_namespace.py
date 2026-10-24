import subprocess
from pathlib import Path

import pytest
import yaml

CHART = Path(__file__).resolve().parents[1] / "helm/orchestrator"


def test_namespace_is_not_a_chart_value():
    for filename in ("values.yaml", "example.values.yaml"):
        assert "namespace" not in yaml.safe_load((CHART / filename).read_text())
    schema = yaml.safe_load((CHART / "values.schema.json").read_text())
    assert "namespace" not in schema["properties"]


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