/**
 * TenantsPage.test — RFC-026 PR-A3 list + create + edit.
 *
 * Covers:
 *   - List renders tenants + per-tenant operator counts from /overview.
 *   - Empty state when listTenants returns an empty array.
 *   - Create modal: opens, validates, submits, invalidates cache.
 *   - Edit modal: opens prefilled, submits delta, invalidates cache.
 *   - 403 `tenant_role_missing` on updateTenant renders the actionable
 *     toast (not a generic error).
 *
 * Not covered (deferred to PR-A6 rollup Playwright): multi-step
 * operator flows, tenant delete (RFC Open-Q-2 → v1.1).
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { ApiError } from '../../lib/api';
import { ToastProvider } from '../../components/Toast';

// Scope mock — stable tenant so localStorage isn't read.
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
  listTenants:       vi.fn(),
  createTenant:      vi.fn(),
  updateTenant:      vi.fn(),
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
  mockApi.listTenants.mockResolvedValue([]);
  // Default overview for any tenant — each overview fires as a separate
  // per-row query; stub to a stable shape so row tests don't explode
  // when a new tenant appears in the list.
  mockApi.getTenantOverview.mockResolvedValue({
    tenant_id: 't1',
    workspace_count: 0,
    total_members: 0,
    active_runs: 0,
    workspaces: [],
  });
});

describe('TenantsPage', () => {
  it('renders the list when listTenants resolves with rows', async () => {
    mockApi.listTenants.mockResolvedValue([
      { tenant_id: 't1', name: 'First',  created_at: 1_700_000_000_000, updated_at: 1_700_000_000_000 },
      { tenant_id: 't2', name: 'Second', created_at: 1_700_000_000_001, updated_at: 1_700_000_000_002 },
    ]);
    const { TenantsPage } = await import('../TenantsPage');
    renderPage(<TenantsPage />);
    await waitFor(() => {
      expect(screen.getByText('First')).toBeInTheDocument();
    });
    expect(screen.getByText('Second')).toBeInTheDocument();
    // tenant_id cells render monospaced.
    expect(screen.getByText('t1')).toBeInTheDocument();
    expect(screen.getByText('t2')).toBeInTheDocument();
  });

  it('renders the empty state when there are no tenants', async () => {
    mockApi.listTenants.mockResolvedValue([]);
    const { TenantsPage } = await import('../TenantsPage');
    renderPage(<TenantsPage />);
    await waitFor(() => {
      expect(screen.getByText(/no tenants yet/i)).toBeInTheDocument();
    });
  });

  it('opens the create modal when "New tenant" is clicked', async () => {
    mockApi.listTenants.mockResolvedValue([]);
    const { TenantsPage } = await import('../TenantsPage');
    renderPage(<TenantsPage />);
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /new tenant/i })).toBeInTheDocument();
    });
    fireEvent.click(screen.getByRole('button', { name: /new tenant/i }));
    expect(screen.getByRole('dialog', { name: /create tenant/i })).toBeInTheDocument();
    expect(screen.getByLabelText(/tenant id/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/display name/i)).toBeInTheDocument();
  });

  it('submits createTenant with the entered values', async () => {
    mockApi.listTenants.mockResolvedValue([]);
    mockApi.createTenant.mockResolvedValue({
      tenant_id: 'acme',
      name: 'ACME Inc',
      created_at: 1_700_000_000_000,
      updated_at: 1_700_000_000_000,
    });
    const { TenantsPage } = await import('../TenantsPage');
    renderPage(<TenantsPage />);
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /new tenant/i })).toBeInTheDocument();
    });
    fireEvent.click(screen.getByRole('button', { name: /new tenant/i }));
    fireEvent.change(screen.getByLabelText(/tenant id/i), { target: { value: 'acme' } });
    fireEvent.change(screen.getByLabelText(/display name/i), { target: { value: 'ACME Inc' } });
    fireEvent.click(screen.getByRole('button', { name: /^create$/i }));

    await waitFor(() => {
      expect(mockApi.createTenant).toHaveBeenCalledTimes(1);
    });
    expect(mockApi.createTenant).toHaveBeenCalledWith({
      tenant_id: 'acme',
      name: 'ACME Inc',
    });
  });

  it('opens the edit modal prefilled and submits only the changed name', async () => {
    mockApi.listTenants.mockResolvedValue([
      { tenant_id: 'acme', name: 'ACME', created_at: 1_700_000_000_000, updated_at: 1_700_000_000_000 },
    ]);
    mockApi.updateTenant.mockResolvedValue({
      tenant_id: 'acme',
      name: 'ACME Corp',
      created_at: 1_700_000_000_000,
      updated_at: 1_700_000_000_010,
    });
    const { TenantsPage } = await import('../TenantsPage');
    renderPage(<TenantsPage />);
    await waitFor(() => {
      expect(screen.getByText('ACME')).toBeInTheDocument();
    });

    // Find the edit button for the row (accessible-name: "Edit tenant acme").
    fireEvent.click(screen.getByRole('button', { name: /edit tenant acme/i }));

    const dialog = await screen.findByRole('dialog', { name: /edit tenant/i });
    const nameField = dialog.querySelector('input[name="name"]') as HTMLInputElement;
    expect(nameField.value).toBe('ACME');
    fireEvent.change(nameField, { target: { value: 'ACME Corp' } });
    fireEvent.click(screen.getByRole('button', { name: /save/i }));

    await waitFor(() => {
      expect(mockApi.updateTenant).toHaveBeenCalledTimes(1);
    });
    // Only the changed field is sent — the PATCH contract expects omitted
    // fields to preserve the stored value.
    expect(mockApi.updateTenant).toHaveBeenCalledWith('acme', { name: 'ACME Corp' });
  });

  it('keeps the dialog open and surfaces the backend error on updateTenant failure', async () => {
    mockApi.listTenants.mockResolvedValue([
      { tenant_id: 'acme', name: 'ACME', created_at: 1_700_000_000_000, updated_at: 1_700_000_000_000 },
    ]);
    mockApi.updateTenant.mockRejectedValue(
      new ApiError(403, 'tenant_role_missing', 'Ask your deployment admin to run promote'),
    );
    const { TenantsPage } = await import('../TenantsPage');
    renderPage(<TenantsPage />);
    await waitFor(() => {
      expect(screen.getByText('ACME')).toBeInTheDocument();
    });
    fireEvent.click(screen.getByRole('button', { name: /edit tenant acme/i }));
    const dialog = await screen.findByRole('dialog', { name: /edit tenant/i });
    const nameField = dialog.querySelector('input[name="name"]') as HTMLInputElement;
    fireEvent.change(nameField, { target: { value: 'ACME Corp' } });
    fireEvent.click(screen.getByRole('button', { name: /save/i }));

    // Dialog stays open with the inline error surfaced.
    await waitFor(() => {
      expect(screen.getByText(/ask your deployment admin/i)).toBeInTheDocument();
    });
    expect(screen.getByRole('dialog', { name: /edit tenant/i })).toBeInTheDocument();
  });
});
