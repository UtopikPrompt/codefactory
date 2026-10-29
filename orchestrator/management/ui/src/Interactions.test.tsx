import { render, screen, within } from '@testing-library/react'
import { expect, test } from 'vitest'
import Interactions from './Interactions'
import type { Task } from './api'

const task: Task = { task_id: 'task-1', repo: 'org/repo', issue_number: 1, status: 'queued', attempts: 0 }

test('queued tasks do not invent agent activity', () => {
    render(<Interactions task={task} />)
    expect(screen.getByText(/No agent interactions recorded yet/)).toBeInTheDocument()
    expect(screen.getByText('No plan recorded')).toBeInTheDocument()
    expect(screen.getByText('No worker job recorded')).toBeInTheDocument()
    expect(screen.getByText('No worker result retained')).toBeInTheDocument()
    expect(screen.getByText('No judge score retained')).toBeInTheDocument()
    expect(screen.queryByText('Candidate returned')).not.toBeInTheDocument()
    expect(screen.getByText('Operational event history is unavailable from this response.')).toBeInTheDocument()
})

test('shows actual dispatch, candidate validation and score, including zero', () => {
    render(<Interactions task={{
        ...task, status: 'failed', attempts: 1,
        execution: { job_name: 'worker-1', worker_status: 'candidate', validation_status: 'passed', error: null, report_failed: true },
        coordination: { plan_recorded: true, judge_score: 0, latest_feedback: null },
    }} />)
    const activity = screen.getByRole('list', { name: 'Recorded agent activity' })
    expect(within(activity).getByText('Plan recorded')).toBeInTheDocument()
    expect(within(activity).getByText('worker-1')).toBeInTheDocument()
    expect(within(activity).getByText('Candidate returned')).toBeInTheDocument()
    expect(within(activity).getByText(/Validation: passed/)).toBeInTheDocument()
    expect(within(activity).getByText('Recorded score: 0.00 / 1')).toBeInTheDocument()
    expect(within(activity).getByText('Result reporting failed.')).toBeInTheDocument()
})

test('retained retry feedback is not presented as current activity or full history', () => {
    const { rerender } = render(<Interactions task={{
        ...task, attempts: 2,
        coordination: { plan_recorded: true, judge_score: null, latest_feedback: { stage: 'judge', attempt: 1, review_topics: ['tests', 'security'] } },
    }} />)
    expect(screen.getByText(/Feedback from attempt 1/)).toBeInTheDocument()
    expect(screen.getByText('Review topics: tests, security')).toBeInTheDocument()
    expect(screen.getByText(/delivery to a later coder call is not tracked/)).toBeInTheDocument()
    expect(screen.getByText(/Jobs and feedback may belong to earlier attempts/)).toBeInTheDocument()
    expect(screen.getByText('No judge score retained')).toBeInTheDocument()
    rerender(<Interactions task={{ ...task, status: 'completed', pr_url: 'https://github.com/org/repo/pull/2' }} />)
    expect(screen.getByText(/does not mean it was approved or merged/)).toBeInTheDocument()
    expect(screen.getByText('No worker result retained')).toBeInTheDocument()
    expect(screen.getByText('No judge score retained')).toBeInTheDocument()
})

test('shows failed worker validation and explains feedback inherited by a manual retry', () => {
    render(<Interactions task={{
        ...task, status: 'running', attempts: 1, retry_of: 'original-task',
        execution: { job_name: null, worker_status: 'candidate_failed', validation_status: 'failed', error: null, report_failed: false },
        coordination: { plan_recorded: false, judge_score: null, latest_feedback: { stage: 'worker', attempt: 8, review_topics: [] } },
    }} />)
    expect(screen.getByText('Candidate failed')).toBeInTheDocument()
    expect(screen.getByText(/Validation: failed/)).toBeInTheDocument()
    expect(screen.getByText('Source: worker')).toBeInTheDocument()
    expect(screen.getByText(/Feedback can be inherited from the original task/)).toBeInTheDocument()
    expect(screen.getByText(/the active agent is not tracked/)).toBeInTheDocument()
    expect(screen.queryByText(/Review topics:/)).not.toBeInTheDocument()
})

test('shows newest operational events first with accurate older-event context and retained rationale', () => {
    render(<Interactions task={{
        ...task, status: 'failed', attempts: 2, events: [
            { timestamp: 1760000000, attempt: 1, actor: 'worker', action: 'candidate', outcome: 'failed', reason: 'validation-failed', validation_status: 'failed', score: null, review_topics: [], failure_details: null },
            { timestamp: 1760000050, attempt: 2, actor: 'supervisor', action: 'plan', outcome: 'reused', reason: null, validation_status: null, score: null, review_topics: [], failure_details: null },
            { timestamp: 1760000100, attempt: 2, actor: 'judge', action: 'review', outcome: 'rejected', reason: 'judge-rejected', validation_status: null, score: 0.4, review_topics: ['tests', 'correctness'], failure_details: null },
        ],
    }} />)
    const flow = screen.getByRole('list', { name: 'Recorded execution flow, newest first' })
    const items = within(flow).getAllByRole('listitem')
    expect(items[0]).toHaveTextContent('Judge rejected the candidate')
    expect(items[1]).toHaveTextContent('Saved plan reused')
    expect(items[2]).toHaveTextContent('Coder worker could not return a candidate')
    expect(within(flow).getByText('Attempt 1 · Coder (worker)')).toBeInTheDocument()
    expect(within(flow).getByText('Attempt 2 · Judge')).toBeInTheDocument()
    expect(within(flow).getByText('Saved plan reused')).toBeInTheDocument()
    expect(within(flow).getByText('Sandbox validation failed.')).toBeInTheDocument()
    expect(within(flow).getByText('The judge scored the candidate below the acceptance threshold.')).toBeInTheDocument()
    expect(within(flow).getByText('Review topics: tests, correctness')).toBeInTheDocument()
    expect(within(flow).getByText('Recorded score: 0.40 / 1')).toBeInTheDocument()
    expect(within(flow).getByText(/No reason was recorded for this event\./)).toBeInTheDocument()
    expect(within(items[0]).getByText(/Next older event:/)).toHaveTextContent('Saved plan reused · Attempt 2 · Supervisor')
    expect(within(items[2]).getByText(/No earlier event is available in retained history/)).toBeInTheDocument()
    expect(within(flow).queryByText(/prompt|private review/i)).not.toBeInTheDocument()
})

test('distinguishes an empty retained timeline from an unavailable timeline', () => {
    const { rerender } = render(<Interactions task={{ ...task, events: [] }} />)
    expect(screen.getByText(/No operational events are retained for this task/)).toBeInTheDocument()
    rerender(<Interactions task={{ ...task, status: 'completed', events: [] }} />)
    expect(screen.getByText(/Task status alone does not confirm which agents ran/)).toBeInTheDocument()
    rerender(<Interactions task={{ ...task, events: null }} />)
    expect(screen.getByText('Operational event history is unavailable from this response.')).toBeInTheDocument()
})

test('shows a worker-originated task failure as a failure stage, not the transition actor', () => {
    render(<Interactions task={{
        ...task, status: 'failed', attempts: 1,
        events: [{
            timestamp: 1760000000, attempt: 1, actor: 'worker', action: 'task', outcome: 'failed',
            reason: 'validation-failed', validation_status: null, score: null, review_topics: [], failure_details: null
        }],
    }} />)
    const flow = screen.getByRole('list', { name: 'Recorded execution flow, newest first' })
    expect(within(flow).getByText('Attempt 1 · Failure stage: Coder (worker)')).toBeInTheDocument()
    expect(within(flow).getByText('The supervisor recorded a terminal task failure.')).toBeInTheDocument()
    expect(within(flow).getByText('Sandbox validation failed.')).toBeInTheDocument()
})
