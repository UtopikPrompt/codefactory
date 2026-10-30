import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, beforeEach, expect, test, vi } from 'vitest'
import Management from './Management'

const task = { task_id: 'task-1', repo: 'org/repo', issue_number: 7, status: 'queued', attempts: 0, lifecycle: 'pending' }
let calls: [string, RequestInit | undefined][]
let taskResponse: unknown
let taskDetailResponse: unknown
let detailStatus = 200
let status = 200
let repositories: string[]
let connected = true
let configResponse: unknown

beforeEach(() => {
    window.location.hash = '/tasks'
    calls = []; status = 200; detailStatus = 200; repositories = ['org/repo']; connected = true; taskResponse = { tasks: [task], total: 1, limit: 25, offset: 0 }
    taskDetailResponse = {
        ...task, status: 'completed', pr_url: 'https://github.com/org/repo/pull/8', execution: {
            job_name: 'worker-job-1', worker_status: 'candidate', validation_status: 'passed', error: null, report_failed: false
        }
    }
    configResponse = {
        source: 'Durable task config', version: '1', hash: 'a'.repeat(64),
        models: [
            { role: 'coder', model: 'verified-model', native_download: true, availability: 'unknown' },
            { role: 'judge', model: 'review-model', native_download: false, availability: 'unknown' },
        ], prompts: { worker: '', planner: '', judge: '' }, agents: [],
    }
    vi.stubGlobal('fetch', vi.fn(async (url: string, options?: RequestInit) => {
        calls.push([url, options])
        const body = url.endsWith('/github/session') ? { connected, configured: true }
            : url.endsWith('/session') ? { subject: 'operator' } : url.endsWith('/repositories') ? { repositories }
                : url.endsWith('/config/validate') ? { valid: true }
                    : url.endsWith('/config/apply') ? configResponse
                        : url.includes('/config') ? configResponse
                            : options?.method === 'POST' ? task : url.includes('/tasks/task-1') ? taskDetailResponse : taskResponse
        return new Response(JSON.stringify(body), { status: url.includes('/tasks/task-1') ? detailStatus : status, headers: { 'Content-Type': 'application/json' } })
    }))
})
afterEach(() => vi.unstubAllGlobals())

function mount() {
    return render(<QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })}><Management /></QueryClientProvider>)
}

test('opens task details inline and preserves task routes', async () => {
    const user = userEvent.setup()
    mount()
    await user.click(await screen.findByRole('link', { name: 'task-1' }))
    expect(await screen.findByRole('region', { name: 'Task detail for task-1' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Collapse task task-1' })).toHaveAttribute('aria-expanded', 'true')
    expect(await screen.findByRole('link', { name: 'Human review' })).toHaveAttribute('href', 'https://github.com/org/repo/pull/8')
    const taskRow = screen.getByRole('link', { name: 'task-1' }).closest('tr')
    expect(taskRow?.nextElementSibling).toHaveClass('detail-row')
    await user.click(screen.getByRole('link', { name: 'Collapse task detail' }))
    expect(screen.queryByRole('region', { name: 'Task detail for task-1' })).not.toBeInTheDocument()
    await user.selectOptions(screen.getByLabelText('Status'), 'running')
    await waitFor(() => expect(calls.some(([url]) => url.includes('status=running'))).toBe(true))
})

test('shows the server-selected documentation role and names its candidate events', async () => {
    taskResponse = { tasks: [{ ...task, role: 'doc' }], total: 1, limit: 25, offset: 0 }
    taskDetailResponse = {
        ...task, role: 'doc', attempts: 1, events: [{
            timestamp: 1760000000, attempt: 1, actor: 'worker',
            action: 'candidate', outcome: 'returned', reason: null, validation_status: 'passed',
            score: null, review_topics: [], failure_details: null
        }],
    }
    const user = userEvent.setup()
    mount()
    expect(await screen.findByRole('columnheader', { name: 'Role' })).toBeInTheDocument()
    expect(await screen.findByText('Documentation')).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Expand task task-1' }))
    const detail = await screen.findByRole('region', { name: 'Task detail for task-1' })
    expect(within(detail).getByText('Documentation')).toBeInTheDocument()
    const activity = within(detail).getByRole('list', { name: 'Recorded agent activity' })
    expect(within(activity).getByRole('heading', { name: 'Documentation worker → supervisor' })).toBeInTheDocument()
    const flow = within(detail).getByRole('list', { name: 'Recorded execution flow, newest first' })
    expect(within(flow).getByText('Documentation worker returned a candidate')).toBeInTheDocument()
    expect(within(flow).getByText('Attempt 1 · Documentation (worker)')).toBeInTheDocument()
})

test('copies the task name to the clipboard', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    vi.stubGlobal('navigator', Object.assign(Object.create(navigator), { clipboard: { writeText } }))
    const user = userEvent.setup()
    mount()

    await user.click(await screen.findByRole('button', { name: 'Copy task name task-1' }))

    expect(writeText).toHaveBeenCalledWith('task-1')
    expect(await screen.findByText('Task name copied')).toHaveAttribute('role', 'status')
})

test('shows task details and interaction summaries before the newest-first execution flow', async () => {
    taskDetailResponse = {
        ...task, status: 'running', attempts: 1,
        events: [
            { timestamp: 1760000000, attempt: 1, actor: 'supervisor', action: 'claim', outcome: 'started', reason: null, validation_status: null, score: null, review_topics: [] },
            { timestamp: 1760000001, attempt: 1, actor: 'supervisor', action: 'plan', outcome: 'recorded', reason: null, validation_status: null, score: null, review_topics: [] },
        ],
    }
    const user = userEvent.setup()
    mount()
    await user.click(await screen.findByRole('button', { name: 'Expand task task-1' }))
    const detail = await screen.findByRole('region', { name: 'Task detail for task-1' })
    const flow = within(detail).getByRole('list', { name: 'Recorded execution flow, newest first' })
    expect(within(flow).getByText('Supervisor claimed this attempt')).toBeInTheDocument()
    expect(within(flow).getByText(/Plan recorded · Attempt 1 · Supervisor/)).toBeInTheDocument()
    const flowItems = within(flow).getAllByRole('listitem')
    expect(flowItems[0]).toHaveTextContent('Plan recorded')
    expect(flowItems[1]).toHaveTextContent('Supervisor claimed this attempt')
    expect(within(detail).getByText('No reason was recorded for this event.')).toBeInTheDocument()
    const detailGrid = within(detail).getByRole('heading', { name: 'Execution steps' })
    const activity = within(detail).getByRole('list', { name: 'Recorded agent activity' })
    const feedback = within(detail).getByRole('heading', { name: 'Feedback loop' })
    const timelineSection = flow.closest('.interaction-timeline-section')
    expect(detailGrid.compareDocumentPosition(activity) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    expect(activity.compareDocumentPosition(feedback) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    expect(feedback.compareDocumentPosition(flow) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    expect(timelineSection?.parentElement?.lastElementChild).toBe(timelineSection)
})

test('shows structured execution steps and a failed task error without raw payloads', async () => {
    taskResponse = { tasks: [{ ...task, status: 'failed', attempts: 2, lifecycle: 'synced' }], total: 1, limit: 25, offset: 0 }
    taskDetailResponse = {
        ...task, status: 'failed', attempts: 2, lifecycle: 'synced', execution: {
            job_name: 'worker-job-1', worker_status: 'candidate_failed', validation_status: 'failed',
            error: 'candidate-validation-not-passed', report_failed: false
        }
    }
    const user = userEvent.setup()
    mount()
    await user.click(await screen.findByRole('button', { name: 'Expand task task-1' }))
    const detail = await screen.findByRole('region', { name: 'Task detail for task-1' })
    expect(within(detail).getByRole('heading', { name: 'Execution steps' })).toBeInTheDocument()
    expect(within(detail).getByText('candidate failed')).toBeInTheDocument()
    expect(within(detail).getByText('candidate-validation-not-passed')).toBeInTheDocument()
    expect(within(detail).getByRole('heading', { name: 'Failure details' })).toBeInTheDocument()
    expect(within(detail).queryByText(/patch payload/i)).not.toBeInTheDocument()
})

test('submits only repository and issue', async () => {
    const user = userEvent.setup()
    mount()
    await user.click(await screen.findByRole('button', { name: 'New task' }))
    expect(screen.getByRole('button', { name: 'Submit task' })).toBeDisabled()
    await screen.findByRole('option', { name: 'org/repo' })
    await user.selectOptions(screen.getByLabelText('Repository', { selector: 'select' }), 'org/repo')
    await user.type(screen.getByLabelText('Issue number'), '7')
    await user.click(screen.getByRole('button', { name: 'Submit task' }))
    await waitFor(() => expect(calls.some(([, options]) => options?.method === 'POST')).toBe(true))
    const post = calls.find(([, options]) => options?.method === 'POST')!
    expect(JSON.parse(post[1]!.body as string)).toEqual({ repo: 'org/repo', issue_number: 7 })
    expect(post[1]!.headers).not.toHaveProperty('Authorization')
    expect(post[1]!.headers).toHaveProperty('X-Management-Request', '1')
})

test('interaction details recover from an API error without presenting fabricated activity', async () => {
    detailStatus = 502
    const user = userEvent.setup()
    mount()
    await user.click(await screen.findByRole('button', { name: 'Expand task task-1' }))
    const detail = await screen.findByRole('region', { name: 'Task detail for task-1' })
    expect(await within(detail).findByRole('alert')).toHaveTextContent('Request failed. Try again.')
    expect(within(detail).queryByRole('heading', { name: 'Agent interactions' })).not.toBeInTheDocument()
    detailStatus = 200
    await user.click(within(detail).getByRole('button', { name: 'Retry' }))
    expect(await within(detail).findByRole('heading', { name: 'Agent interactions' })).toBeInTheDocument()
})

test('has honest empty and read-only model states', async () => {
    taskResponse = { tasks: [], total: 0, limit: 25, offset: 0 }
    const user = userEvent.setup()
    mount()
    expect(await screen.findByText('No tasks found.')).toBeInTheDocument()
    await user.click(screen.getByRole('link', { name: 'Models' }))
    expect(await screen.findByText('verified-model')).toBeInTheDocument()
    expect(screen.getByText('unknown')).toBeInTheDocument()
    expect(screen.getByRole('textbox', { name: 'coder model' })).toHaveValue('verified-model')
    expect(screen.getByRole('textbox', { name: 'Worker guidance' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Apply for future tasks' })).toBeDisabled()
    await user.click(screen.getByRole('link', { name: 'Agents' }))
    expect(screen.getByText('Read only')).toBeInTheDocument()
})

test('validates a model and prompt draft before applying it to future tasks', async () => {
    const user = userEvent.setup()
    mount()
    await user.click(await screen.findByRole('link', { name: 'Models' }))
    const model = await screen.findByRole('textbox', { name: 'coder model' })
    await user.clear(model)
    await user.type(model, 'new/coder-model')
    const prompt = screen.getByRole('textbox', { name: 'Worker guidance' })
    await user.type(prompt, 'Prefer concise changes.')
    await user.click(screen.getByRole('button', { name: 'Validate draft' }))
    await screen.findByText('Draft validated against the active version.')
    const validationCall = calls.find(([url]) => url.endsWith('/config/validate'))
    expect(JSON.parse(validationCall?.[1]?.body as string)).toMatchObject({
        expected_hash: 'a'.repeat(64), models: { coder: 'new/coder-model', judge: 'review-model' },
        prompts: { worker: 'Prefer concise changes.', planner: '', judge: '' },
    })
    await user.click(screen.getByRole('button', { name: 'Apply for future tasks' }))
    await waitFor(() => expect(calls.some(([url]) => url.endsWith('/config/apply'))).toBe(true))
    const applyCall = calls.find(([url]) => url.endsWith('/config/apply'))
    expect(JSON.parse(applyCall?.[1]?.body as string)).toMatchObject({
        expected_hash: 'a'.repeat(64), models: { coder: 'new/coder-model' },
        prompts: { worker: 'Prefer concise changes.' },
    })
    expect(applyCall?.[1]?.headers).toHaveProperty('X-Management-Request', '1')
})

test('places management route links in a top menu with an active page', async () => {
    const user = userEvent.setup()
    mount()
    const navigation = await screen.findByRole('navigation', { name: 'Management' })
    const main = screen.getByRole('main', { name: '' })
    expect(navigation.parentElement).toBe(main.parentElement)
    expect(navigation.nextElementSibling).toBe(main)
    expect(within(navigation).getByRole('link', { name: 'Tasks' })).toHaveAttribute('href', '#/tasks')
    expect(within(navigation).getByRole('link', { name: 'Tasks' })).toHaveAttribute('aria-current', 'page')
    expect(within(navigation).getByRole('link', { name: 'Models' })).toHaveAttribute('href', '#/models')
    expect(within(navigation).getByRole('link', { name: 'Agents' })).toHaveAttribute('href', '#/agents')

    await user.click(within(navigation).getByRole('link', { name: 'Agents' }))
    expect(within(navigation).getByRole('link', { name: 'Agents' })).toHaveAttribute('aria-current', 'page')
    expect(within(navigation).getByRole('link', { name: 'Tasks' })).not.toHaveAttribute('aria-current')
})

test('dynamic submission accepts a repository name after GitHub connection', async () => {
    repositories = []
    const user = userEvent.setup()
    mount()
    await user.click(await screen.findByRole('button', { name: 'New task' }))
    await waitFor(() => expect(within(screen.getByRole('dialog')).getByPlaceholderText('owner/repository')).toBeInTheDocument())
    await user.type(within(screen.getByRole('dialog')).getByLabelText('Repository'), 'other/project')
    await user.type(screen.getByLabelText('Issue number'), '12')
    await user.click(screen.getByRole('button', { name: 'Submit task' }))
    await waitFor(() => expect(calls.some(([url, options]) => url.endsWith('/tasks') && options?.method === 'POST')).toBe(true))
    const post = calls.find(([url, options]) => url.endsWith('/tasks') && options?.method === 'POST')!
    expect(JSON.parse(post[1]!.body as string)).toEqual({ repo: 'other/project', issue_number: 12 })
})

test('management login alone cannot submit', async () => {
    connected = false
    const user = userEvent.setup()
    mount()
    await user.click(await screen.findByRole('button', { name: 'New task' }))
    expect(await screen.findByRole('button', { name: 'Connect GitHub' })).toBeEnabled()
    await user.selectOptions(screen.getByLabelText('Repository', { selector: 'select' }), 'org/repo')
    await user.type(screen.getByLabelText('Issue number'), '7')
    expect(screen.getByRole('button', { name: 'Submit task' })).toBeDisabled()
    expect(calls.some(([, options]) => options?.method === 'POST')).toBe(false)
})

test('denied session never loads management data', async () => {
    status = 401
    mount()
    expect(await screen.findByRole('alert')).toHaveTextContent('Sign in is required.')
    expect(calls).toHaveLength(1)
    expect(screen.queryByRole('button', { name: 'New task' })).not.toBeInTheDocument()
})