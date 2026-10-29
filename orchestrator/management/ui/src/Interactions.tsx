import type { FailureDetails, OperationalEvent, Task } from './api'

const eventReasons: Record<string, string> = {
    'no-op-edit': 'The worker produced no effective change.',
    'empty-or-invalid-edits': 'The worker returned empty or invalid edits.',
    'invalid-generation-output': 'The generated response did not match the required format.',
    'invalid-generated-edit': 'A generated edit did not pass validation.',
    'edit-outside-policy': 'An edit fell outside the configured policy.',
    'unsafe-edit-path': 'An edit used a disallowed file path.',
    'protected-edit-path': 'An edit targeted a protected file.',
    'secret-in-model-context': 'A secret was detected in model context.',
    'edits-not-applied': 'The worker could not apply the proposed edits.',
    'empty-patch': 'The worker returned an empty patch.',
    'validation-failed': 'Sandbox validation failed.',
    'validation-output-too-large': 'Validation output exceeded its limit.',
    'coder-http-error': 'The coder service returned an error.',
    'worker-result-timeout': 'No worker result arrived before the timeout.',
    'worker-candidate-failed': 'The worker could not produce a candidate.',
    'candidate-patch-edits-mismatch': 'The candidate patch did not match its edits.',
    'candidate-edit-set-mismatch': 'The candidate file set did not match its edits.',
    'candidate-validation-not-passed': 'The candidate did not pass validation.',
    'task-execution-failed': 'The task failed during execution.',
    'max-total-attempts-exhausted': 'The maximum number of attempts was reached.',
    'planner-credential-required': 'The planner credential was unavailable.',
    'planner-output-invalid': 'The planner returned an invalid result.',
    'judge-credential-required': 'The judge credential was unavailable.',
    'judge-score-invalid': 'The judge returned an invalid score.',
    'task-authentication-unsupported': 'The task authentication mode is unsupported.',
    'task-catalog-hash-mismatch': 'The pinned agent configuration did not match.',
    'task-catalog-schema-invalid': 'The pinned agent configuration was invalid.',
    'task-catalog-version-unsupported': 'The pinned agent configuration version is unsupported.',
    'task-catalog-role-set-invalid': 'The configured agent roles were invalid.',
    'task-catalog-role-invalid': 'A configured agent role was invalid.',
    'task-catalog-capability-boundary-invalid': 'The configured agent capabilities were invalid.',
    'pr-policy-required': 'Pull request policy was incomplete.',
    'pr-base-branch-not-allowed': 'The base branch was not allowed.',
    'pr-reviewer-policy-invalid': 'The reviewer policy was invalid.',
    'pr-publication-credential-required': 'The publication credential was unavailable.',
    'candidate-publish-data-missing': 'Required candidate publication data was missing.',
    'base-branch-changed': 'The base branch changed during publication.',
    'existing-task-branch-conflict': 'The existing task branch did not match this candidate.',
    'existing-pr-policy-conflict': 'The existing pull request did not match publication policy.',
    'pr-creation-unverified': 'Pull request creation could not be verified.',
    'pr-identity-invalid': 'The created pull request identity was invalid.',
    'judge-rejected': 'The judge scored the candidate below the acceptance threshold.',
}

function eventTitle(event: OperationalEvent) {
    if (event.action === 'claim') return 'Supervisor claimed this attempt'
    if (event.action === 'plan') return event.outcome === 'reused' ? 'Saved plan reused' : 'Plan recorded'
    if (event.action === 'dispatch') return 'Worker job dispatched'
    if (event.action === 'candidate') return event.outcome === 'failed' ? 'Coder worker could not return a candidate' : 'Coder worker returned a candidate'
    if (event.action === 'review') return event.outcome === 'accepted' ? 'Judge accepted the candidate' : 'Judge rejected the candidate'
    if (event.action === 'publish') return 'Candidate published for human review'
    if (event.outcome === 'retrying') return 'Task queued for another attempt'
    if (event.outcome === 'failed') return 'Task failed'
    if (event.outcome === 'completed') return 'Task completed'
    return 'Task event recorded'
}

function eventDescription(event: OperationalEvent) {
    if (event.action === 'claim') return 'The supervisor claimed the task and started this attempt.'
    if (event.action === 'plan') return event.outcome === 'reused'
        ? 'The supervisor reused the bounded plan saved earlier. Plan text is withheld.'
        : 'A bounded plan was saved. Plan text is withheld.'
    if (event.action === 'dispatch') return 'The supervisor created a worker job for this attempt.'
    if (event.action === 'candidate') return `Coder worker result received. Sandbox validation: ${event.validation_status || 'not recorded'}.`
    if (event.action === 'review') return 'The judge recorded a decision. Full review text is withheld.'
    if (event.action === 'publish') return 'The supervisor recorded publication for human review.'
    if (event.action === 'task' && event.outcome === 'retrying') return 'The supervisor recorded that this task will be retried.'
    if (event.action === 'task' && event.outcome === 'failed') return 'The supervisor recorded a terminal task failure.'
    if (event.action === 'task' && event.outcome === 'completed') return 'The supervisor recorded task completion.'
    return 'The task outcome was recorded for this attempt.'
}

function eventActor(event: OperationalEvent) {
    const actor = event.actor === 'worker' ? 'Coder (worker)' : event.actor === 'judge' ? 'Judge' : 'Supervisor'
    return event.action === 'task' && event.actor !== 'supervisor' ? `Failure stage: ${actor}` : actor
}

const failureDetailLabels: Record<string, string> = {
    'validation:nonzero-exit': 'Validation failed',
    'execution:timeout': 'Execution timed out',
    'execution:os-error': 'Execution OS error',
    'execution:json-error': 'Execution returned invalid JSON',
    'execution:http-error': 'Execution returned an HTTP error',
    'execution:unexpected': 'Execution failed unexpectedly',
}

function failureDetailDescription(details: FailureDetails) {
    const description = failureDetailLabels[`${details.stage}:${details.code}`]
    if (!description) return null

    const parts = [description]
    if (typeof details.exit_status === 'number' && Number.isInteger(details.exit_status)
        && details.exit_status >= -255 && details.exit_status <= 255) {
        parts.push(`Exit code ${details.exit_status}`)
    }
    if (typeof details.duration_ms === 'number' && Number.isInteger(details.duration_ms)
        && details.duration_ms >= 0 && details.duration_ms <= 600000) {
        parts.push(`Duration ${details.duration_ms} ms`)
    }
    if (typeof details.output_bytes === 'number' && Number.isInteger(details.output_bytes)
        && details.output_bytes >= 0 && details.output_bytes <= 65536) {
        parts.push(`Output ${details.output_bytes} bytes`)
    }
    return parts.join(' · ')
}

function ExecutionFlow({ events }: { events: OperationalEvent[] | null | undefined }) {
    if (events === undefined || events === null) {
        return <p className="timeline-state" role="status">Operational event history is unavailable from this response.</p>
    }
    if (events.length === 0) {
        return <p className="timeline-state" role="status">No operational events are retained for this task. Task status alone does not confirm which agents ran.</p>
    }
    return <ol className="interaction-timeline" aria-label="Recorded execution flow, newest first">
        {events.slice().reverse().map((event, index, newestFirstEvents) => {
            const nextEvent = newestFirstEvents[index + 1]
            return <li key={`${event.timestamp}-${event.attempt}-${event.actor}-${event.action}-${index}`}>
                <div className="timeline-event-heading">
                    <strong>{eventTitle(event)}</strong>
                    <span>Attempt {event.attempt} · {eventActor(event)}</span>
                </div>
                <time dateTime={new Date(event.timestamp * 1000).toISOString()}>
                    {new Date(event.timestamp * 1000).toLocaleString()}
                </time>
                <p>{eventDescription(event)}</p>
                {event.failure_details && failureDetailDescription(event.failure_details) && <p>
                    <strong>Failure details:</strong> {failureDetailDescription(event.failure_details)}
                </p>}
                {event.score !== null && <p>Recorded score: {event.score.toFixed(2)} / 1</p>}
                {event.review_topics.length > 0 && <p>Review topics: {event.review_topics.join(', ')}</p>}
                <p className="timeline-reason"><strong>Why:</strong> {event.reason ? eventReasons[event.reason] || 'A safe operational reason was recorded.' : 'No reason was recorded for this event.'}</p>
                <p className="timeline-next"><strong>Next older event:</strong> {nextEvent
                    ? `${eventTitle(nextEvent)} · Attempt ${nextEvent.attempt} · ${eventActor(nextEvent)}`
                    : 'No earlier event is available in retained history.'}</p>
            </li>
        })}
    </ol>
}

export default function Interactions({ task }: { task: Task }) {
    const execution = task.execution
    const coordination = task.coordination
    const feedback = coordination?.latest_feedback
    const score = coordination?.judge_score
    const hasScore = typeof score === 'number'
    const recorded = Boolean(task.events?.length || coordination?.plan_recorded || execution?.job_name || execution?.worker_status || execution?.validation_status || execution?.report_failed || hasScore || feedback)
    return <section className="interactions" aria-labelledby={`interactions-${task.task_id}`}>
        <div className="interaction-heading">
            <h3 id={`interactions-${task.task_id}`}>Agent interactions</h3>
            <span className="muted">Task snapshot · {task.attempts ? `Attempt count: ${task.attempts}` : 'No attempts recorded'}</span>
        </div>
        <p className="interaction-note">Sanitized operational history, not a live conversation. Jobs and feedback may belong to earlier attempts. Refreshes every 10 seconds.</p>
        {!recorded && <p className="interaction-empty" role="status">No agent interactions recorded yet. A queued task does not confirm that an agent has started.</p>}
        <ul className="interaction-grid" aria-label="Recorded agent activity">
            <li>
                <h4>Supervisor</h4>
                <strong>{coordination?.plan_recorded ? 'Plan recorded' : 'No plan recorded'}</strong>
                <p>Coordinates planning, worker dispatch and candidate review. Plan contents are not exposed.</p>
            </li>
            <li>
                <h4>Supervisor → worker</h4>
                <strong>{execution?.job_name ? 'Worker job recorded' : 'No worker job recorded'}</strong>
                {execution?.job_name && <p className="mono">{execution.job_name}</p>}
                <p>Latest recorded job, not proof that a worker is currently running.</p>
            </li>
            <li>
                <h4>Coder (worker) → supervisor</h4>
                <strong>{execution?.worker_status === 'candidate' ? 'Candidate returned' : execution?.worker_status === 'candidate_failed' ? 'Candidate failed' : 'No worker result retained'}</strong>
                <p>Validation: {execution?.validation_status || 'not recorded'}. The worker executes coding and sandbox checks; individual coder calls are not recorded.</p>
                {execution?.report_failed && <p className="error">Result reporting failed.</p>}
            </li>
            <li>
                <h4>Judge → supervisor</h4>
                <strong>{hasScore ? `Recorded score: ${score.toFixed(2)} / 1` : 'No judge score retained'}</strong>
                <p>Only persisted review results are shown. A candidate or completed task alone does not confirm a judge interaction.</p>
            </li>
        </ul>
        <div className="interaction-feedback">
            <h4>Feedback loop</h4>
            {feedback ? <>
                <p><strong>Source: {feedback.stage}</strong> · Feedback from attempt {feedback.attempt}</p>
                {feedback.review_topics.length > 0 && <p>Review topics: {feedback.review_topics.join(', ')}</p>}
                <p>Latest saved feedback only; delivery to a later coder call is not tracked. Raw reviews are not exposed.</p>
                {task.retry_of && <p>Feedback can be inherited from the original task linked in Retry of below; its attempt number may refer to that task.</p>}
            </> : <p>No retry feedback recorded. Events below belong only to this task; events from a parent retry are not copied here.</p>}
        </div>
        <div className="interaction-outcome">
            <h4>Outcome</h4>
            <p>Task state: <strong>{task.status}</strong>. {task.status === 'queued' ? 'Waiting for a supervisor claim or retry.' : task.status === 'running' ? 'Execution is in progress; the active agent is not tracked.' : task.status === 'completed' ? 'Execution completed; human approval is not tracked here.' : 'Execution failed; see failure details below.'}</p>
            {task.pr_url && <p>A pull request is available via Human review below; this does not mean it was approved or merged.</p>}
        </div>
        <div className="interaction-timeline-section">
            <h4>Execution flow</h4>
            <p className="timeline-caption">Newest retained events first. Only persisted facts are shown.</p>
            <ExecutionFlow events={task.events} />
        </div>
    </section>
}
