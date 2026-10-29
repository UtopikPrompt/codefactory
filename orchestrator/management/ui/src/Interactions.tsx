import type { Task } from './api'

export default function Interactions({ task }: { task: Task }) {
    const execution = task.execution
    const coordination = task.coordination
    const feedback = coordination?.latest_feedback
    const score = coordination?.judge_score
    const hasScore = typeof score === 'number'
    const recorded = Boolean(coordination?.plan_recorded || execution?.job_name || execution?.worker_status || execution?.validation_status || execution?.report_failed || hasScore || feedback)
    return <section className="interactions" aria-labelledby={`interactions-${task.task_id}`}>
        <div className="interaction-heading">
            <h3 id={`interactions-${task.task_id}`}>Agent interactions</h3>
            <span className="muted">Task snapshot · {task.attempts ? `Attempt count: ${task.attempts}` : 'No attempts recorded'}</span>
        </div>
        <p className="interaction-note">Recorded task state, not a live conversation or event timeline. Jobs and feedback may belong to earlier attempts. Refreshes every 10 seconds.</p>
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
                <h4>Coder / worker → supervisor</h4>
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
            </> : <p>No retry feedback recorded. Earlier attempt history is not available in this snapshot.</p>}
        </div>
        <div className="interaction-outcome">
            <h4>Outcome</h4>
            <p>Task state: <strong>{task.status}</strong>. {task.status === 'queued' ? 'Waiting for a supervisor claim or retry.' : task.status === 'running' ? 'Execution is in progress; the active agent is not tracked.' : task.status === 'completed' ? 'Execution completed; human approval is not tracked here.' : 'Execution failed; see failure details below.'}</p>
            {task.pr_url && <p>A pull request is available via Human review below; this does not mean it was approved or merged.</p>}
        </div>
    </section>
}
