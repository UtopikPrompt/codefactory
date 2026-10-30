export interface Task {
    task_id: string
    repo: string | null
    issue_number: number | null
    status: string
    role?: 'coder' | 'doc' | null
    lifecycle?: string
    credential_cleanup?: string
    attempts?: number
    retry_of?: string | null
    retry_task_id?: string | null
    config_version?: string | null
    config_hash?: string | null
    pr_url?: string | null
    events?: OperationalEvent[] | null
    coordination?: {
        plan_recorded: boolean
        judge_score: number | null
        latest_feedback: {
            stage: 'supervisor' | 'worker' | 'judge'
            attempt: number
            review_topics: string[]
        } | null
    }
    execution?: {
        job_name: string | null
        worker_status: 'candidate' | 'candidate_failed' | null
        validation_status: 'passed' | 'failed' | 'not-run' | null
        error: string | null
        report_failed: boolean
    }
}
export type FailureDetails = (
    | { stage: 'validation'; code: 'nonzero-exit' }
    | { stage: 'execution'; code: 'timeout' | 'os-error' | 'json-error' | 'http-error' | 'unexpected' }
    | { stage: 'judge'; code: 'review-rejected'; summary: string }
) & {
    exit_status?: number
    duration_ms?: number
    output_bytes?: number
}
export interface OperationalEvent {
    timestamp: number
    attempt: number
    actor: 'supervisor' | 'worker' | 'judge'
    action: 'claim' | 'plan' | 'dispatch' | 'candidate' | 'review' | 'publish' | 'task'
    outcome: 'started' | 'recorded' | 'reused' | 'dispatched' | 'returned' | 'failed' | 'accepted' | 'rejected' | 'retrying' | 'completed'
    reason: string | null
    validation_status: 'passed' | 'failed' | 'not-run' | null
    score: number | null
    review_topics: string[]
    failure_details: FailureDetails | null
}
export interface TaskPage { tasks: Task[]; total: number; limit: number; offset: number }
export interface Config {
    source: string
    version: string
    hash: string | null
    models: { role: string; model: string; native_download: boolean; availability: string }[]
    prompts: { worker: string; planner: string; judge: string }
    agents: { id: string; role: string; endpoint_alias: string; max_files: number; tools: string[]; availability: string }[]
}
export interface ConfigDraft {
    expected_hash: string
    models: { coder: string; judge: string }
    prompts: { worker: string; planner: string; judge: string }
}
export class ApiError extends Error {
    readonly status: number
    constructor(status: number) {
        const messages: Record<number, string> = {
            400: 'The request or configuration draft is invalid.',
            401: 'Sign in is required.', 403: 'Access denied.', 404: 'Task not found.',
            409: 'Configuration or task state changed. Refresh and try again.',
            503: 'Management access or server configuration is unavailable.',
        }
        super(messages[status] || 'Request failed. Try again.')
        this.status = status
    }
}
export async function api<Result>(path: string, payload?: unknown): Promise<Result> {
    const response = await fetch(`/management/api${path}`, {
        method: payload === undefined ? 'GET' : 'POST', credentials: 'same-origin', redirect: 'error',
        headers: payload === undefined ? { Accept: 'application/json' } : { Accept: 'application/json', 'Content-Type': 'application/json', 'X-Management-Request': '1' },
        body: payload === undefined ? undefined : JSON.stringify(payload),
    })
    if (!response.ok) throw new ApiError(response.status)
    if (!response.headers.get('content-type')?.includes('application/json')) throw new Error('Session response unavailable. Sign in again.')
    return response.json() as Promise<Result>
}