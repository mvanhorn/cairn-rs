/**
 * OperatorsPage.test — RFC-026 PR-A4 list + create + role edit +
 * per-operator tenant-role grants.
 *
 * Covers:
 *   - List renders operators + per-operator tenant-role counts.
 *   - Empty state when listOperatorProfiles returns an empty array.
 *   - Create modal: opens, validates, submits, invalidates cache.
 *   - Edit modal: opens prefilled, submits delta (PATCH), invalidates.
 *   - Tenant-role grants expand: shows active + revoked grants,
 *     "Revoke" calls revokeOperatorTenantRole, "Grant" submits a
 *     new role via promoteOperatorTenantRole.
 *   - 403 `tenant_role_missing` on updateOperatorProfile renders the
 *     inline banner (not a generic error) and keeps the dialog open.
 *
 * Not covered here (deferred to the RFC-026 rollup Playwright spec):
 *   - Cross-tenant operator list isolation (server-gated in PR-A2 +
 *     backend PR-A4 404 tests).
 *   - Full multi-operator flow (Playwright fixtures from PR-A0a).
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
  listOperatorProfiles:      vi.fn(),
  createOperatorProfile:     vi.fn(),
  updateOperatorProfile:     vi.fn(),
  listOperatorTenantRoles:   vi.fn(),
  promoteOperatorTenantRole: vi.fn(),
  revokeOperatorTenantRole:  vi.fn(),
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
  // Default: empty list, empty grant per operator — keeps per-row
  // useQuery calls stable when a test adds rows.
  mockApi.listOperatorProfiles.mockResolvedValue({ items: [], has_more: false });
  mockApi.listOperatorTenantRoles.mockResolvedValue({ items: [], has_more: false });
});

describe('OperatorsPage', () => {
  it('renders the list when listOperatorProfiles resolves with rows', async () => {
    mockApi.listOperatorProfiles.mockResolvedValue({
      items: [
        { operator_id: 'op_a', tenant_id: 't1', display_name: 'Alice', email: 'alice@example.com', role: 'admin' },
        { operator_id: 'op_b', tenant_id: 't1', display_name: 'Bob',   email: 'bob@example.com',   role: 'member' },
      ],
      has_more: false,
    });
    const { OperatorsPage } = await import('../OperatorsPage');
    renderPage(<OperatorsPage />);
    await waitFor(() => {
      expect(screen.getByText('Alice')).toBeInTheDocument();
    });
    expect(screen.getByText('Bob')).toBeInTheDocument();
    // operator_id rendered monospaced.
    expect(screen.getByText('op_a')).toBeInTheDocument();
    expect(screen.getByText('op_b')).toBeInTheDocument();
  });

  it('renders the empty state when there are no operators', async () => {
    mockApi.listOperatorProfiles.mockResolvedValue({ items: [], has_more: false });
    const { OperatorsPage } = await import('../OperatorsPage');
    renderPage(<OperatorsPage />);
    await waitFor(() => {
      expect(screen.getByText(/no operators yet/i)).toBeInTheDocument();
    });
  });

  it('opens the create modal when "New operator" is clicked', async () => {
    mockApi.listOperatorProfiles.mockResolvedValue({ items: [], has_more: false });
    const { OperatorsPage } = await import('../OperatorsPage');
    renderPage(<OperatorsPage />);
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /new operator/i })).toBeInTheDocument();
    });
    fireEvent.click(screen.getByRole('button', { name: /new operator/i }));
    expect(screen.getByRole('dialog', { name: /create operator/i })).toBeInTheDocument();
    expect(screen.getByLabelText(/display name/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/email/i)).toBeInTheDocument();
    // WorkspaceRole selector surface.
    expect(screen.getByLabelText(/role/i)).toBeInTheDocument();
  });

  it('submits createOperatorProfile with the entered values', async () => {
    mockApi.listOperatorProfiles.mockResolvedValue({ items: [], has_more: false });
    mockApi.createOperatorProfile.mockResolvedValue({
      operator_id: 'op_new',
      tenant_id: 't1',
      display_name: 'New Op',
      email: 'new@example.com',
      role: 'member',
    });
    const { OperatorsPage } = await import('../OperatorsPage');
    renderPage(<OperatorsPage />);
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /new operator/i })).toBeInTheDocument();
    });
    fireEvent.click(screen.getByRole('button', { name: /new operator/i }));
    fireEvent.change(screen.getByLabelText(/display name/i), {
      target: { value: 'New Op' },
    });
    fireEvent.change(screen.getByLabelText(/email/i), {
      target: { value: 'new@example.com' },
    });
    fireEvent.change(screen.getByLabelText(/role/i), {
      target: { value: 'member' },
    });
    fireEvent.click(screen.getByRole('button', { name: /^create$/i }));

    await waitFor(() => {
      expect(mockApi.createOperatorProfile).toHaveBeenCalledTimes(1);
    });
    expect(mockApi.createOperatorProfile).toHaveBeenCalledWith('t1', {
      display_name: 'New Op',
      email: 'new@example.com',
      role: 'member',
    });
  });

  it('opens the edit modal prefilled and submits only the changed role', async () => {
    mockApi.listOperatorProfiles.mockResolvedValue({
      items: [
        { operator_id: 'op_a', tenant_id: 't1', display_name: 'Alice', email: 'alice@example.com', role: 'member' },
      ],
      has_more: false,
    });
    mockApi.updateOperatorProfile.mockResolvedValue({
      operator_id: 'op_a',
      tenant_id: 't1',
      display_name: 'Alice',
      email: 'alice@example.com',
      role: 'admin',
    });
    const { OperatorsPage } = await import('../OperatorsPage');
    renderPage(<OperatorsPage />);
    await waitFor(() => {
      expect(screen.getByText('Alice')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole('button', { name: /edit operator op_a/i }));
    const dialog = await screen.findByRole('dialog', { name: /edit operator/i });
    const roleField = dialog.querySelector('select[name="role"]') as HTMLSelectElement;
    expect(roleField.value).toBe('member');
    fireEvent.change(roleField, { target: { value: 'admin' } });
    fireEvent.click(screen.getByRole('button', { name: /save/i }));

    await waitFor(() => {
      expect(mockApi.updateOperatorProfile).toHaveBeenCalledTimes(1);
    });
    // PATCH only sends the changed field.
    expect(mockApi.updateOperatorProfile).toHaveBeenCalledWith('t1', 'op_a', {
      role: 'admin',
    });
  });

  it('keeps the dialog open and surfaces tenant_role_missing on updateOperatorProfile failure', async () => {
    mockApi.listOperatorProfiles.mockResolvedValue({
      items: [
        { operator_id: 'op_a', tenant_id: 't1', display_name: 'Alice', email: 'alice@example.com', role: 'member' },
      ],
      has_more: false,
    });
    mockApi.updateOperatorProfile.mockRejectedValue(
      new ApiError(403, 'tenant_role_missing', 'Ask your deployment admin to run promote'),
    );
    const { OperatorsPage } = await import('../OperatorsPage');
    renderPage(<OperatorsPage />);
    await waitFor(() => {
      expect(screen.getByText('Alice')).toBeInTheDocument();
    });
    fireEvent.click(screen.getByRole('button', { name: /edit operator op_a/i }));
    const dialog = await screen.findByRole('dialog', { name: /edit operator/i });
    const roleField = dialog.querySelector('select[name="role"]') as HTMLSelectElement;
    fireEvent.change(roleField, { target: { value: 'admin' } });
    fireEvent.click(screen.getByRole('button', { name: /save/i }));

    await waitFor(() => {
      expect(screen.getByText(/ask your deployment admin/i)).toBeInTheDocument();
    });
    expect(screen.getByRole('dialog', { name: /edit operator/i })).toBeInTheDocument();
  });

  it('expands a row to show tenant-role grants and allows revoke', async () => {
    mockApi.listOperatorProfiles.mockResolvedValue({
      items: [
        { operator_id: 'op_a', tenant_id: 't1', display_name: 'Alice', email: 'alice@example.com', role: 'admin' },
      ],
      has_more: false,
    });
    mockApi.listOperatorTenantRoles.mockResolvedValue({
      items: [
        {
          tenant_id: 't1', operator_id: 'op_a', role: 'admin',
          granted_at_ms: 1_700_000_000_000, granted_by: 'system',
          revoked_at_ms: null, revoked_by: null,
        },
        {
          tenant_id: 't2', operator_id: 'op_a', role: 'member',
          granted_at_ms: 1_700_000_000_100, granted_by: 'op_admin',
          revoked_at_ms: 1_700_000_000_500, revoked_by: 'op_admin',
        },
      ],
      has_more: false,
    });
    mockApi.revokeOperatorTenantRole.mockResolvedValue({
      tenant_id: 't1', operator_id: 'op_a', role: 'admin',
      granted_at_ms: 1_700_000_000_000, granted_by: 'system',
      revoked_at_ms: 1_700_000_000_999, revoked_by: 'god_token',
    });

    const { OperatorsPage } = await import('../OperatorsPage');
    renderPage(<OperatorsPage />);
    await waitFor(() => {
      expect(screen.getByText('Alice')).toBeInTheDocument();
    });

    // Click the expand button for Alice's row.
    fireEvent.click(screen.getByRole('button', { name: /tenant roles for op_a/i }));

    // The revoked t2 row appears after the grants load — using /revoked/i
    // as the first wait-for target avoids racing on the active-tenant
    // value ("t1") which also appears elsewhere on the page (header).
    await waitFor(() => {
      expect(screen.getByText(/revoked/i)).toBeInTheDocument();
    });
    // t2 tenant_id cell renders monospaced inside the grants table.
    expect(screen.getByText('t2')).toBeInTheDocument();

    // Revoke the active grant.
    fireEvent.click(screen.getByRole('button', { name: /revoke tenant role t1 for op_a/i }));

    await waitFor(() => {
      expect(mockApi.revokeOperatorTenantRole).toHaveBeenCalledWith('op_a', 't1');
    });
  });

  it('grants a new tenant-role via the expanded form', async () => {
    mockApi.listOperatorProfiles.mockResolvedValue({
      items: [
        { operator_id: 'op_a', tenant_id: 't1', display_name: 'Alice', email: 'alice@example.com', role: 'admin' },
      ],
      has_more: false,
    });
    mockApi.listOperatorTenantRoles.mockResolvedValue({ items: [], has_more: false });
    mockApi.promoteOperatorTenantRole.mockResolvedValue({
      tenant_id: 't_new', operator_id: 'op_a', role: 'member',
      granted_at_ms: 1_700_000_000_000, granted_by: 'god_token',
    });

    const { OperatorsPage } = await import('../OperatorsPage');
    renderPage(<OperatorsPage />);
    await waitFor(() => {
      expect(screen.getByText('Alice')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole('button', { name: /tenant roles for op_a/i }));

    await waitFor(() => {
      expect(screen.getByLabelText(/grant tenant id/i)).toBeInTheDocument();
    });
    fireEvent.change(screen.getByLabelText(/grant tenant id/i), {
      target: { value: 't_new' },
    });
    fireEvent.change(screen.getByLabelText(/grant role/i), {
      target: { value: 'member' },
    });
    fireEvent.click(screen.getByRole('button', { name: /^grant$/i }));

    await waitFor(() => {
      expect(mockApi.promoteOperatorTenantRole).toHaveBeenCalledWith('op_a', 't_new', 'member');
    });
  });
});
