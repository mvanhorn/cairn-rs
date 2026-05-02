/**
 * RetentionPage.test — RFC-026 PR-A6 per-tenant retention policy +
 * manual apply-retention trigger.
 *
 * Covers:
 *   - Renders the current retention policy for the active scope's tenant.
 *   - Empty-state path when `getRetentionPolicy` 404s (no policy set yet).
 *   - Edit modal prefills from the existing policy.
 *   - Submit invokes `setRetentionPolicy` with the full request body
 *     (SET semantics, mirrors QuotasPage contract).
 *   - Apply-retention is gated behind a confirmation modal; clicking the
 *     top-level button alone must not fire the destructive mutation.
 *   - Apply-retention success shows the result summary (events_pruned /
 *     entities_affected) via toast or inline surface.
 *
 * Not covered here (deferred to PR-A6 Playwright rollup):
 *   - AdminGate wrapper 403 handling (AdminGate has its own tests).
 *   - Snapshots / event-log compaction UI — RFC defers to v1.1.
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
  getRetentionPolicy: vi.fn(),
  setRetentionPolicy: vi.fn(),
  applyRetention:     vi.fn(),
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
});

describe('RetentionPage', () => {
  it('renders the current retention policy for the active tenant', async () => {
    mockApi.getRetentionPolicy.mockResolvedValue({
      policy_id: 'rp1',
      tenant_id: 't1',
      full_history_days: 90,
      current_state_days: 30,
      max_events_per_entity: 10_000,
    });
    const { RetentionPage } = await import('../RetentionPage');
    renderPage(<RetentionPage />);

    // Each dimension label renders somewhere.
    await waitFor(() => {
      expect(screen.getByText(/full history/i)).toBeInTheDocument();
    });
    expect(screen.getByText(/current state/i)).toBeInTheDocument();
    expect(screen.getByText(/max events/i)).toBeInTheDocument();
    // Edit-policy button is only rendered when a policy exists.
    expect(screen.getByRole('button', { name: /edit policy/i })).toBeInTheDocument();
    // Apply-now is visible too.
    expect(screen.getByRole('button', { name: /apply retention/i })).toBeInTheDocument();
  });

  it('renders the empty state when the tenant has no retention policy', async () => {
    // 404 from getRetentionPolicy = the canonical "no policy set" signal
    // per `get_retention_policy_handler` (handlers/admin.rs:615).
    mockApi.getRetentionPolicy.mockRejectedValue(
      new ApiError(404, 'not_found', 'tenant retention policy not found'),
    );
    const { RetentionPage } = await import('../RetentionPage');
    renderPage(<RetentionPage />);

    await waitFor(() => {
      expect(screen.getByText(/no retention policy/i)).toBeInTheDocument();
    });
    // The create-policy CTA should be visible from the empty state.
    expect(screen.getByRole('button', { name: /create policy/i })).toBeInTheDocument();
    // Apply-now must not be offered when there's no policy to apply.
    expect(screen.queryByRole('button', { name: /apply retention/i })).not.toBeInTheDocument();
  });

  it('opens the edit modal prefilled with the current policy values', async () => {
    mockApi.getRetentionPolicy.mockResolvedValue({
      policy_id: 'rp1',
      tenant_id: 't1',
      full_history_days: 45,
      current_state_days: 14,
      max_events_per_entity: 5_000,
    });
    const { RetentionPage } = await import('../RetentionPage');
    renderPage(<RetentionPage />);

    await waitFor(() => {
      expect(screen.getByRole('button', { name: /edit policy/i })).toBeInTheDocument();
    });
    fireEvent.click(screen.getByRole('button', { name: /edit policy/i }));

    const dialog = await screen.findByRole('dialog', { name: /edit retention/i });
    const fullField = dialog.querySelector('input[name="full_history_days"]') as HTMLInputElement;
    const currentField = dialog.querySelector('input[name="current_state_days"]') as HTMLInputElement;
    const maxField = dialog.querySelector('input[name="max_events_per_entity"]') as HTMLInputElement;

    expect(fullField.value).toBe('45');
    expect(currentField.value).toBe('14');
    expect(maxField.value).toBe('5000');
  });

  it('submits setRetentionPolicy with all three fields (SET semantics)', async () => {
    mockApi.getRetentionPolicy.mockResolvedValue({
      policy_id: 'rp1',
      tenant_id: 't1',
      full_history_days: 45,
      current_state_days: 14,
      max_events_per_entity: 5_000,
    });
    mockApi.setRetentionPolicy.mockResolvedValue({
      policy_id: 'rp1',
      tenant_id: 't1',
      full_history_days: 90,
      current_state_days: 14,
      max_events_per_entity: 5_000,
    });
    const { RetentionPage } = await import('../RetentionPage');
    renderPage(<RetentionPage />);

    await waitFor(() => {
      expect(screen.getByRole('button', { name: /edit policy/i })).toBeInTheDocument();
    });
    fireEvent.click(screen.getByRole('button', { name: /edit policy/i }));

    const dialog = await screen.findByRole('dialog', { name: /edit retention/i });
    const fullField = dialog.querySelector('input[name="full_history_days"]') as HTMLInputElement;
    fireEvent.change(fullField, { target: { value: '90' } });
    fireEvent.click(screen.getByRole('button', { name: /save/i }));

    await waitFor(() => {
      expect(mockApi.setRetentionPolicy).toHaveBeenCalledTimes(1);
    });
    // All three fields must be present — the backend contract for
    // SetRetentionPolicyRequest requires every value on every submit.
    expect(mockApi.setRetentionPolicy).toHaveBeenCalledWith('t1', {
      full_history_days: 90,
      current_state_days: 14,
      max_events_per_entity: 5_000,
    });
  });

  it('requires an explicit confirmation modal before firing apply-retention', async () => {
    mockApi.getRetentionPolicy.mockResolvedValue({
      policy_id: 'rp1',
      tenant_id: 't1',
      full_history_days: 90,
      current_state_days: 30,
      max_events_per_entity: 10_000,
    });
    const { RetentionPage } = await import('../RetentionPage');
    renderPage(<RetentionPage />);

    await waitFor(() => {
      expect(screen.getByRole('button', { name: /apply retention/i })).toBeInTheDocument();
    });

    // Click #1 — the top-level apply button. This must only open the
    // confirmation dialog; it MUST NOT fire the destructive mutation.
    fireEvent.click(screen.getByRole('button', { name: /apply retention/i }));

    // A confirmation dialog with the destructive warning is now visible.
    const dialog = await screen.findByRole('dialog', { name: /apply retention/i });
    expect(dialog).toHaveTextContent(/cannot be undone/i);

    // The mutation MUST NOT have fired yet — only the confirm button
    // inside the dialog is authoritative.
    expect(mockApi.applyRetention).not.toHaveBeenCalled();
  });

  it('fires apply-retention only after the confirm button is clicked, and shows the result summary', async () => {
    mockApi.getRetentionPolicy.mockResolvedValue({
      policy_id: 'rp1',
      tenant_id: 't1',
      full_history_days: 90,
      current_state_days: 30,
      max_events_per_entity: 10_000,
    });
    mockApi.applyRetention.mockResolvedValue({
      events_pruned: 1_234,
      entities_affected: 7,
    });
    const { RetentionPage } = await import('../RetentionPage');
    renderPage(<RetentionPage />);

    await waitFor(() => {
      expect(screen.getByRole('button', { name: /apply retention/i })).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole('button', { name: /apply retention/i }));
    const dialog = await screen.findByRole('dialog', { name: /apply retention/i });

    // Click the in-dialog confirm (testid keeps the query unambiguous
    // even if the outer button and the confirm share a label).
    const confirmBtn = dialog.querySelector(
      '[data-testid="apply-retention-confirm-btn"]',
    ) as HTMLButtonElement;
    expect(confirmBtn).not.toBeNull();
    fireEvent.click(confirmBtn);

    await waitFor(() => {
      expect(mockApi.applyRetention).toHaveBeenCalledWith('t1');
    });

    // Result surface: events_pruned + entities_affected are shown.
    // The number appears in both the inline result panel and the
    // transient toast ack, so match any occurrence.
    await waitFor(() => {
      expect(screen.getAllByText(/1,234/).length).toBeGreaterThan(0);
    });
    expect(screen.getAllByText(/7/).length).toBeGreaterThan(0);
  });
});
