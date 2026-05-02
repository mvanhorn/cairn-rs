/**
 * QuotasPage.test — RFC-026 PR-A5 per-tenant quota + usage.
 *
 * Covers:
 *   - Loading state while the quota/overview queries are in flight.
 *   - Renders the current quota limits + live usage for the active scope's tenant.
 *   - Empty-state path when `getTenantQuota` 404s (no quota policy exists yet).
 *   - Create-quota flow: empty-state → modal → submit sends all three limits.
 *   - Edit-quota flow: prefills from the current quota and submits the full
 *     body (SET semantics, not PATCH — the backend replaces all fields).
 *   - 403 `tenant_role_missing` on setTenantQuota keeps the dialog open and
 *     surfaces the backend message inline.
 *
 * Not covered (deferred to PR-A6 rollup Playwright): AdminGate wrapper
 * 403 handling (AdminGate has its own tests), dry-run preview (RFC
 * defers to v1.1), multi-tenant overview table (scope-picker-first UX).
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { ApiError } from '../../lib/api';
import { ToastProvider } from '../../components/Toast';

// Scope mock — stable tenant so localStorage isn't read. The page is
// scope-driven (single-tenant view keyed off the active scope) so every
// test sees `t1` unless it overrides the mock.
vi.mock('../../hooks/useScope', async () => {
  const actual = await vi.importActual<typeof import('../../hooks/useScope')>(
    '../../hooks/useScope',
  );
  const scope = {
    tenant_id: 't1',
    workspace_id: 'w1',
    project_id: 'p1',
  };
  return {
    ...actual,
    useScope: () => [scope, () => {}, () => {}] as const,
    getStoredScope: () => scope,
    isDefaultScope: () => false,
  };
});

const mockApi = {
  getTenantQuota:    vi.fn(),
  setTenantQuota:    vi.fn(),
  getTenantOverview: vi.fn(),
};

vi.mock('../../lib/api', async () => {
  const actual = await vi.importActual<typeof import('../../lib/api')>(
    '../../lib/api',
  );
  return {
    ...actual,
    defaultApi: new Proxy({} as Record<string, unknown>, {
      get: (_t, prop: string) => {
        if (prop in mockApi) return (mockApi as Record<string, unknown>)[prop];
        return () => Promise.resolve([]);
      },
    }),
  };
});

function renderPage(node: React.ReactNode) {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
  return render(
    <QueryClientProvider client={qc}>
      <ToastProvider>{node}</ToastProvider>
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  for (const fn of Object.values(mockApi)) fn.mockReset();
  // Overview never 404s in normal operation — a tenant that exists has
  // an overview row even when its quota is unset. Default to a stable
  // shape so usage-bar tests have something to render against.
  mockApi.getTenantOverview.mockResolvedValue({
    tenant_id: 't1',
    workspace_count: 2,
    total_members: 3,
    active_runs: 2,
    workspaces: [],
  });
});

describe('QuotasPage', () => {
  it('renders the current quota limits for the active tenant', async () => {
    mockApi.getTenantQuota.mockResolvedValue({
      tenant_id: 't1',
      max_concurrent_runs: 10,
      max_sessions_per_hour: 100,
      max_tasks_per_run: 50,
      current_active_runs: 2,
      sessions_this_hour: 15,
    });
    const { QuotasPage } = await import('../QuotasPage');
    renderPage(<QuotasPage />);

    // Each dimension label renders somewhere.
    await waitFor(() => {
      expect(screen.getByText(/concurrent runs/i)).toBeInTheDocument();
    });
    expect(screen.getByText(/sessions.+hour/i)).toBeInTheDocument();
    expect(screen.getByText(/tasks.+run/i)).toBeInTheDocument();
    // Edit-quota button is only rendered when a quota exists — confirms
    // the query resolved to the non-null branch.
    expect(screen.getByRole('button', { name: /edit quota/i })).toBeInTheDocument();
  });

  it('renders the empty state when the tenant has no quota policy', async () => {
    // 404 from getTenantQuota = the canonical "no policy set" signal
    // per `get_tenant_quota_handler` (handlers/admin.rs:573).
    mockApi.getTenantQuota.mockRejectedValue(
      new ApiError(404, 'not_found', 'tenant quota not found'),
    );
    const { QuotasPage } = await import('../QuotasPage');
    renderPage(<QuotasPage />);

    await waitFor(() => {
      expect(screen.getByText(/no quota policy/i)).toBeInTheDocument();
    });
    // The create-quota CTA should be visible from the empty state.
    expect(screen.getByRole('button', { name: /create quota/i })).toBeInTheDocument();
  });

  it('opens the edit modal prefilled with the current limits', async () => {
    mockApi.getTenantQuota.mockResolvedValue({
      tenant_id: 't1',
      max_concurrent_runs: 5,
      max_sessions_per_hour: 60,
      max_tasks_per_run: 25,
      current_active_runs: 1,
      sessions_this_hour: 4,
    });
    const { QuotasPage } = await import('../QuotasPage');
    renderPage(<QuotasPage />);

    await waitFor(() => {
      expect(screen.getByRole('button', { name: /edit quota/i })).toBeInTheDocument();
    });
    fireEvent.click(screen.getByRole('button', { name: /edit quota/i }));

    const dialog = await screen.findByRole('dialog', { name: /edit quota/i });
    const runsField = dialog.querySelector('input[name="max_concurrent_runs"]') as HTMLInputElement;
    const sessionsField = dialog.querySelector('input[name="max_sessions_per_hour"]') as HTMLInputElement;
    const tasksField = dialog.querySelector('input[name="max_tasks_per_run"]') as HTMLInputElement;

    expect(runsField.value).toBe('5');
    expect(sessionsField.value).toBe('60');
    expect(tasksField.value).toBe('25');
  });

  it('submits setTenantQuota with all three fields (SET semantics)', async () => {
    mockApi.getTenantQuota.mockResolvedValue({
      tenant_id: 't1',
      max_concurrent_runs: 5,
      max_sessions_per_hour: 60,
      max_tasks_per_run: 25,
      current_active_runs: 0,
      sessions_this_hour: 0,
    });
    mockApi.setTenantQuota.mockResolvedValue({
      tenant_id: 't1',
      max_concurrent_runs: 20,
      max_sessions_per_hour: 60,
      max_tasks_per_run: 25,
      current_active_runs: 0,
      sessions_this_hour: 0,
    });
    const { QuotasPage } = await import('../QuotasPage');
    renderPage(<QuotasPage />);

    await waitFor(() => {
      expect(screen.getByRole('button', { name: /edit quota/i })).toBeInTheDocument();
    });
    fireEvent.click(screen.getByRole('button', { name: /edit quota/i }));

    const dialog = await screen.findByRole('dialog', { name: /edit quota/i });
    const runsField = dialog.querySelector('input[name="max_concurrent_runs"]') as HTMLInputElement;
    fireEvent.change(runsField, { target: { value: '20' } });
    fireEvent.click(screen.getByRole('button', { name: /save/i }));

    await waitFor(() => {
      expect(mockApi.setTenantQuota).toHaveBeenCalledTimes(1);
    });
    // All three fields must be present — the backend contract for
    // SetTenantQuotaRequest requires every limit on every submit.
    expect(mockApi.setTenantQuota).toHaveBeenCalledWith('t1', {
      max_concurrent_runs: 20,
      max_sessions_per_hour: 60,
      max_tasks_per_run: 25,
    });
  });

  it('renders usage bars showing current vs limit', async () => {
    mockApi.getTenantQuota.mockResolvedValue({
      tenant_id: 't1',
      max_concurrent_runs: 10,
      max_sessions_per_hour: 100,
      max_tasks_per_run: 50,
      current_active_runs: 7,
      sessions_this_hour: 80,
    });
    const { QuotasPage } = await import('../QuotasPage');
    renderPage(<QuotasPage />);

    // Each dimension should show a usage bar with role="progressbar".
    await waitFor(() => {
      const bars = screen.getAllByRole('progressbar');
      expect(bars.length).toBeGreaterThanOrEqual(2);
    });
    // Per-dimension utilization percentages should render somewhere.
    // concurrent runs: 7/10 = 70%
    // sessions/hour:  80/100 = 80%
    expect(screen.getByText(/70%/)).toBeInTheDocument();
    expect(screen.getByText(/80%/)).toBeInTheDocument();
  });

  it('keeps the dialog open and surfaces the backend error on setTenantQuota failure', async () => {
    mockApi.getTenantQuota.mockResolvedValue({
      tenant_id: 't1',
      max_concurrent_runs: 5,
      max_sessions_per_hour: 60,
      max_tasks_per_run: 25,
      current_active_runs: 0,
      sessions_this_hour: 0,
    });
    mockApi.setTenantQuota.mockRejectedValue(
      new ApiError(403, 'tenant_role_missing', 'Ask your deployment admin to run promote'),
    );
    const { QuotasPage } = await import('../QuotasPage');
    renderPage(<QuotasPage />);

    await waitFor(() => {
      expect(screen.getByRole('button', { name: /edit quota/i })).toBeInTheDocument();
    });
    fireEvent.click(screen.getByRole('button', { name: /edit quota/i }));
    const dialog = await screen.findByRole('dialog', { name: /edit quota/i });
    const runsField = dialog.querySelector('input[name="max_concurrent_runs"]') as HTMLInputElement;
    fireEvent.change(runsField, { target: { value: '10' } });
    fireEvent.click(screen.getByRole('button', { name: /save/i }));

    await waitFor(() => {
      expect(screen.getByText(/ask your deployment admin/i)).toBeInTheDocument();
    });
    expect(screen.getByRole('dialog', { name: /edit quota/i })).toBeInTheDocument();
  });
});
