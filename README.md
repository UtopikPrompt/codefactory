# code-factory

Application source for the Code Factory task orchestration system. This
repository owns the gateway, management UI/API, supervisor, worker, and the
`orchestrator` Helm chart.

- Runtime components: `orchestrator/`
- Application chart: `helm/orchestrator/`
- Infrastructure selection and deployment operations: the
	[infra-codefactory repository](https://github.com/UtopikLab/infra-codefactory)

The repository split does not itself publish artifacts, change a cluster, or
perform a deployment cutover.

## CI Pipeline

Both product repositories use matching lint, Python test, Helm validation,
optional chart publication, and CI summary jobs. The test jobs share the same
step scripts, configured by a `slice` matrix dimension and repository-level
test defaults. Jobs are named `test-<slice>`. Code Factory derives service paths
and `requirements-test.txt` paths from every slice, including shared under
`orchestrator/shared/`. Inference uses one shared slice with
repository-level defaults and no overrides. Artifact names derive from the slice.
Both workflows install one requirements file per slice. Tests run with pytest,
including unittest cases, and emit a JUnit report plus a GitHub job summary.
Code Factory's Python test job uses a non-fail-fast matrix for supervisor,
worker, gateway, management, and shared tests. Service slices collect their
entire directory, including shared under `orchestrator/shared/`, without
duplicating tests. Full `pytest orchestrator/` discovery includes every slice. Each slice
installs its own runtime requirements plus test dependencies; management also
installs gateway requirements for its real-gateway contract tests. Helm remains
available to every slice. The `python-test-results-<slice>` artifacts are
retained for 14 days, including failed test runs that produce a report.
The final summary includes publication outcomes, even when jobs fail or are skipped.

For local service tests, install the matching test requirements, for example:

```sh
python3 -m pip install -r orchestrator/management/requirements-test.txt
python3 -m pytest orchestrator/management/ -q
```

Use a separate virtual environment per slice. Shared tests use
`orchestrator/shared/requirements-test.txt`. Worker runtime requirements retain pytest
because automatic repository validation invokes it inside the worker sandbox.

Both workflows use the same Helm dependency preparation and lint/render scripts.
Workflow environment settings select locked dependency builds, strict lint,
values files, render settings, and expected images. Code Factory validates its
example values and renders with management enabled, checking all four images;
inference builds its locked NVIDIA dependency and uses strict lint.

Code Factory additionally validates four service image builds. Image publication
requires `GHCR_PUBLISH_ENABLED=true`; chart publication separately requires
`HELM_CHART_PUBLISH_ENABLED=true`. Image publication remains restricted to
main-branch pushes or manual runs on main. Main-branch image builds publish the
`dev` tag; the short-SHA, full-SHA, and `latest` tags are retained. Image
coordinates, build behavior, and source revision labels are unchanged.

Both repositories use the same chart publication policy after lint, tests, and
chart validation succeed: push or manual runs on main or version tags are
eligible, with publication still opt-in. Tags must be strictly `vMAJOR.MINOR.PATCH`
or `MAJOR.MINOR.PATCH` (no leading zeroes except `0`); the optional `v` is removed.
Release versions are immutable and existing releases are never overwritten.
Main publishes the valid SemVer `0.0.0-sha.<full-commit>` and moves the OCI `main`
alias to that verified source digest. Tag releases never move `main`.
Every publication retains that commit-addressed source chart; an existing source
version is reused only if its revision annotation matches the requested commit.
Source commit and registry digest are verified before updating the main alias
or reporting successful publication. Infra consumes the OCI chart repository and
exact version configured for the environment, using `helm pull --version`.
Product release-manifest JSON and intermediate chart/image manifest artifacts
are no longer generated; chart coordinates and test-result artifacts are unchanged.
Tag runs publish charts only, not images.
Inference uses its locked NVIDIA chart dependency and upstream runtime image;
it has no container-build job.

## Release Namespace

Helm's `--namespace` is authoritative for all chart resources and RBAC
ServiceAccount subjects. There is no top-level `namespace` chart value.
`namespaces.create` defaults to `true` for standalone installs; setting it to
`false` omits the Namespace resource. Infra uses `false` and requires the
selected namespace to be preprovisioned separately; routine deployment never
passes `--create-namespace` or adopts an existing namespace. A chart-created
namespace is retained on uninstall, not backed up or automatically adopted.

Inference endpoint URLs are application configuration, not release namespace
selectors. Set the supported `supervisor.config` URLs for the actual inference
namespace; moving this release does not relocate another release or rewrite
those endpoints.

Focused namespace checks:

```sh
python3 -m pip install -r orchestrator/shared/requirements-test.txt
python3 -m pytest orchestrator/shared/test_chart_namespace.py -q
ruff check orchestrator/
helm lint helm/orchestrator --values helm/orchestrator/example.values.yaml
```