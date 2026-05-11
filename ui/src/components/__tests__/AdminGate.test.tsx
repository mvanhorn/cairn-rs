/**
 * AdminGate.test — RFC-026 PR-A3 role gate.
 *
 * AdminGate probes `GET /v1/admin/tenants/:id` against the active scope
 * to determine whether the operator holds `TenantRole::Admin` on that
 * tenant.  Three branches:
 *
 *   1. probe pending  → render a loading spinner (children NOT rendered).
 *   2. probe 200      → render children (the admin page).
 *   3. probe 403 with `tenant_role_missing` → render the role-required
 *      banner, NOT <NotFoundPage> (per RFC-026 §Open-Q-1 resolution).
 *
 * Also verifies `useIsTenantAdmin()` returns the right state in each
 * branch so the <Sidebar> can hide admin links client-side.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { ApiError } from '../../lib/api';

// Scope — pin to a stable tenant so the component doesn't read localStorage.
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
  getTenant: vi.fn(),
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

// Deferred promise helper to keep a query "pending".
function deferred<T>(): { promise: Promise<T>; resolve: (v: T) => void; reject: (e: unknown) => void } {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function renderGate(node: React.ReactNode) {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
  return render(<QueryClientProvider client={qc}>{node}</QueryClientProvider>);
}

beforeEach(() => {
  mockApi.getTenant.mockReset();
});

describe('AdminGate', () => {
  it('renders the loading branch while the admin probe is pending', async () => {
    const d = deferred<unknown>();
    mockApi.getTenant.mockReturnValueOnce(d.promise);

    const { AdminGate } = await import('../AdminGate');
    renderGate(
      <AdminGate>
        <div>admin content</div>
      </AdminGate>,
    );

    // Spinner visible; children NOT rendered.
    expect(screen.getByRole('status', { name: /checking admin access/i })).toBeInTheDocument();
    expect(screen.queryByText('admin content')).not.toBeInTheDocument();

    // Cleanup the pending promise so the test tears down cleanly.
    d.resolve({ tenant_id: 't1', name: 't1', created_at: 0, updated_at: 0 });
  });

  it('renders children once the admin probe returns 200', async () => {
    mockApi.getTenant.mockResolvedValueOnce({
      tenant_id: 't1',
      name: 'Tenant 1',
      created_at: 0,
      updated_at: 0,
    });

    const { AdminGate } = await import('../AdminGate');
    renderGate(
      <AdminGate>
        <div>admin content</div>
      </AdminGate>,
    );

    await waitFor(() => {
      expect(screen.getByText('admin content')).toBeInTheDocument();
    });
    expect(screen.queryByText(/tenant-admin role/i)).not.toBeInTheDocument();
  });

  it('renders the role-required banner on 403 tenant_role_missing', async () => {
    mockApi.getTenant.mockRejectedValueOnce(
      new ApiError(
        403,
        'tenant_role_missing',
        'Ask your deployment admin to run `cairn-app admin promote op-1`',
      ),
    );

    const { AdminGate } = await import('../AdminGate');
    renderGate(
      <AdminGate>
        <div>admin content</div>
      </AdminGate>,
    );

    await waitFor(() => {
      expect(screen.getByText(/tenant-admin role/i)).toBeInTheDocument();
    });
    // Children are NOT rendered — this is the gate, not a soft warning.
    expect(screen.queryByText('admin content')).not.toBeInTheDocument();
    // The structured hint from the backend is surfaced to the operator.
    expect(screen.getByText(/admin promote op-1/i)).toBeInTheDocument();
  });

  it('renders the generic error banner on non-403 failures', async () => {
    mockApi.getTenant.mockRejectedValueOnce(
      new ApiError(500, 'internal_error', 'boom'),
    );

    const { AdminGate } = await import('../AdminGate');
    renderGate(
      <AdminGate>
        <div>admin content</div>
      </AdminGate>,
    );

    await waitFor(() => {
      // Non-403 = something else went wrong — render an error state,
      // not the misleading "you need tenant-admin role" message.
      expect(screen.getByText(/couldn.?t verify admin access/i)).toBeInTheDocument();
    });
    expect(screen.queryByText('admin content')).not.toBeInTheDocument();
  });
});

describe('useIsTenantAdmin', () => {
  // Hook-harness. `useIsTenantAdmin` is imported at module level below;
  // re-exporting it through a tiny probe component lets assertions run
  // against the rendered output without pulling in all the async/ESM
  // dynamic-import ceremony in the test body.
  type Harness = { tenantId?: string };
  let HookProbe: (props: Harness) => React.ReactElement;

  beforeEach(async () => {
    const mod = await import('../AdminGate');
    HookProbe = ({ tenantId }: Harness) => {
      const state = mod.useIsTenantAdmin(tenantId);
      return (
        <div>
          <span data-testid="loading">{String(state.isLoading)}</span>
          <span data-testid="admin">{String(state.isAdmin)}</span>
          <span data-testid="code">{state.errorCode ?? ''}</span>
        </div>
      );
    };
  });

  it('returns isAdmin:true when the probe succeeds', async () => {
    mockApi.getTenant.mockResolvedValueOnce({
      tenant_id: 't1',
      name: 'Tenant 1',
      created_at: 0,
      updated_at: 0,
    });
    renderGate(<HookProbe tenantId="t1" />);
    await waitFor(() => {
      expect(screen.getByTestId('admin').textContent).toBe('true');
    });
    expect(screen.getByTestId('code').textContent).toBe('');
  });

  it('returns isAdmin:false and errorCode:tenant_role_missing on 403', async () => {
    mockApi.getTenant.mockRejectedValueOnce(
      new ApiError(403, 'tenant_role_missing', 'need role'),
    );
    renderGate(<HookProbe tenantId="t1" />);
    // `admin === 'false'` is true in the loading branch too, so wait
    // for the settled state by waiting on the errorCode being populated.
    await waitFor(() => {
      expect(screen.getByTestId('code').textContent).toBe('tenant_role_missing');
    });
    expect(screen.getByTestId('admin').textContent).toBe('false');
    expect(screen.getByTestId('loading').textContent).toBe('false');
  });
});
