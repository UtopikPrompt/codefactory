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
python3 -m pytest orchestrator/test_chart_namespace.py -q
ruff check orchestrator/
helm lint helm/orchestrator --values helm/orchestrator/example.values.yaml
```