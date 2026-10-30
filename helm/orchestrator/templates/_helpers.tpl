{{- define "orchestrator.agentRuntimeConfigJson" -}}
{{- $coderUrl := index .Values.supervisor.config "coder-url" -}}
{{- $coderNative := eq $coderUrl "http://inference-p40.inference-dev.svc.cluster.local:8000/v1" -}}
{{- if kindIs "bool" .Values.worker.coderNativeDownload -}}
{{- $coderNative = .Values.worker.coderNativeDownload -}}
{{- end -}}
{{- $judgeNative := eq (index .Values.supervisor.config "judge-url") "http://inference-cpu.inference-dev.svc.cluster.local:8000/v1" -}}
{{- if kindIs "bool" .Values.worker.judgeNativeDownload -}}
{{- $judgeNative = .Values.worker.judgeNativeDownload -}}
{{- end -}}
{{- $runtimeConfig := .Values.supervisor.config -}}
{{- $workerEndpoints := index $runtimeConfig "worker-inference-endpoints" -}}
{{- $workerParallelism := int (index $runtimeConfig "worker-parallelism") -}}
{{- if or (lt (len $workerEndpoints) 1) (gt (len $workerEndpoints) 2) -}}
{{- fail "supervisor.config.worker-inference-endpoints must contain one or two endpoints" -}}
{{- end -}}
{{- if or (lt $workerParallelism 1) (gt $workerParallelism 2) -}}
{{- fail "supervisor.config.worker-parallelism must be between one and two" -}}
{{- end -}}
{{- $coderUrlConfigured := false -}}
{{- $workerEndpointUrls := dict -}}
{{- range $endpoint := $workerEndpoints -}}
{{- $endpointUrl := index $endpoint "url" -}}
{{- if not (regexMatch "^https?://[^/?#@]+/v1$" $endpointUrl) -}}
{{- fail "worker-inference-endpoints URLs must be valid HTTP(S) /v1 URLs" -}}
{{- end -}}
{{- if hasKey $workerEndpointUrls $endpointUrl -}}
{{- fail "worker-inference-endpoints URLs must be unique" -}}
{{- end -}}
{{- $_ := set $workerEndpointUrls $endpointUrl true -}}
{{- if eq $endpointUrl $coderUrl -}}
{{- $coderUrlConfigured = true -}}
{{- end -}}
{{- if ne (index $endpoint "model") (index $runtimeConfig "coder-model") -}}
{{- fail "worker-inference-endpoints model aliases must match supervisor.config.coder-model" -}}
{{- end -}}
{{- if eq $endpointUrl (index $runtimeConfig "judge-url") -}}
{{- fail "worker-inference-endpoints must remain separate from the judge review URL" -}}
{{- end -}}
{{- end -}}
{{- if not $coderUrlConfigured -}}
{{- fail "worker-inference-endpoints must include the effective coder URL" -}}
{{- end -}}
{{- $config := dict
	"catalog" (.Files.Get "files/agent-catalog.yaml")
	"version" (.Values.agentCatalog.version | default "1")
	"prompts" (dict "worker" "" "planner" "" "judge" "")
	"acceptor_url" (index .Values.supervisor.config "acceptor-url")
	"coder_url" $coderUrl
	"coder_native_download" $coderNative
	"worker_inference_endpoints" (index .Values.supervisor.config "worker-inference-endpoints")
	"judge_url" (index .Values.supervisor.config "judge-url")
	"judge_native_download" $judgeNative
	"coder_model" (index .Values.supervisor.config "coder-model")
	"judge_model" (index .Values.supervisor.config "judge-model")
	"threshold" (index .Values.supervisor.config "acceptance-threshold")
	"judge_review_margin" (index .Values.supervisor.config "judge-review-margin")
	"max_total_attempts" (index .Values.supervisor.config "max-total-attempts")
	"allowed_base_branches" (index .Values.supervisor.config "allowed-base-branches")
	"reviewers" (index .Values.supervisor.config "reviewers")
	"repo_policies" .Values.worker.repoPolicies
	"worker_parallelism" (index .Values.supervisor.config "worker-parallelism")
-}}
{{- toJson $config -}}
{{- end -}}
{{- define "orchestrator.agentCatalogHash" -}}
{{- printf "%s%s%s" (.Files.Get "files/agent-catalog.yaml") (.Files.Get "files/agent-catalog.schema.json") (include "orchestrator.agentRuntimeConfigJson" .) | sha256sum -}}
{{- end -}}