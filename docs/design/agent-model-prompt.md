# Agent model + prompt + tools — the "each agent needs a spec" prompt

> A spec to implement before coding the supervisor loop. Each agent must have its
> **model**, **prompt**, and **tools** defined — the current code only defines the
> model matrix (partial) and leaves prompts and tools entirely implicit.

## The gap

The current design documents *which model* each role should use (see
`thinktank-plan.md` §2.4), but runtime configuration currently provides only
`supervisor.config.coder-model` and `supervisor.config.judge-model`. The worker
Job receives `coder-model`; worker roles do not yet have distinct model
settings. Three things are **missing or partial**:

- **Model** — *partial.* Helm configures the judge model and one coder model
  shared by worker roles. There is no per-role model map yet.
- **Prompt** — *missing.* There is no role-specific instruction text. The
  `instruction` field in the spawn payload is absent.
- **Tools** — *missing.* There is no supervisor tool registry. The supervisor
  loop calls `worker.call()` but does not define *which tools* each worker may use
  (edit, validate, git/PR, etc.).

## The three implementation items

1. **Per-role model.** As a future change, add a model map to `values.yaml`
  (e.g. `workers.models: { coder: "...", refactor: "...", test: "..." }`)
  so each worker role can resolve its own model instead of sharing
  `supervisor.config.coder-model`.
2. **Role-specific prompts.** Add a prompt map to `values.yaml` (e.g.
   `workers.worker-prompts: { coder: "...", test: "...", ... }`) and render it
   into the spawn payload so `build_role_payload` carries the right instruction.
3. **Supervisor tool registry.** Define a registry (e.g. `edit`, `validate`,
   `git`, `PR`) that declares, per role, which tools are available and their
   semantics, so the supervisor loop can bind tools to roles.
