/**
 * TenantsPage — RFC-026 PR-A3 admin surface.
 *
 * List every tenant visible to the current operator (admin-gated) and
 * expose Create + Edit flows. The page assumes it is wrapped in
 * <AdminGate>, which probes role membership before rendering — we still
 * handle errors defensively because the active scope's tenant is not
 * necessarily the same tenant being edited (operator can be admin of
 * some tenants but not others). The server-side `TenantAdminGuard`
 * remains the authoritative gate.
 *
 * Backend contract (all live as of PR-A2):
 *   GET    /v1/admin/tenants                     list
 *   POST   /v1/admin/tenants                     create { tenant_id, name }
 *   GET    /v1/admin/tenants/:id/overview        per-row operator_count
 *   PATCH  /v1/admin/tenants/:id                 edit { name? }
 *
 * `metadata` editing is deferred per PR-A2 scope: the backend table
 * still has no metadata column, so the current PATCH accepts name only.
 * When metadata lands, this page grows a JSON key-value editor.
 */

import { useMemo, useState } from 'react';
import { useMutation, useQueries, useQuery, useQueryClient } from '@tanstack/react-query';
import { Building2, Plus, RefreshCw, Pencil, Check, X, Users, Briefcase, Play as PlayIcon } from 'lucide-react';
import { clsx } from 'clsx';

import { defaultApi } from '../lib/api';
import type { TenantRecord, TenantOverview } from '../lib/types';
import { errorMessage } from '../lib/errors';
import { EntityExplainer } from '../components/EntityExplainer';
import { ErrorFallback } from '../components/ErrorFallback';

// ── Validation ────────────────────────────────────────────────────────────────

/** Mirror the backend regex for tenant_id — keep it in one place so
 *  the UI never accepts an id the API will subsequently reject. */
const TENANT_ID_RE = /^[a-z0-9][a-z0-9_-]{0,62}$/;

function validateTenantId(id: string): string | null {
  if (!id.trim()) return 'Tenant ID is required.';
  if (!TENANT_ID_RE.test(id)) return 'Use lowercase letters, digits, hyphens, underscores (max 63 chars).';
  return null;
}

function validateName(name: string): string | null {
  if (!name.trim()) return 'Display name is required.';
  if (name.length > 128) return 'Display name must be 128 characters or fewer.';
  return null;
}

// ── Date formatting ──────────────────────────────────────────────────────────

function fmtDate(ms: number): string {
  if (!ms) return '—';
  return new Date(ms).toLocaleString(undefined, {
    year:  'numeric',
    month: 'short',
    day:   'numeric',
  });
}

// ── Create form ──────────────────────────────────────────────────────────────

interface CreateFormProps {
  onSubmit: (body: { tenant_id: string; name: string }) => Promise<void>;
  onCancel: () => void;
  submitting: boolean;
}

function CreateForm({ onSubmit, onCancel, submitting }: CreateFormProps) {
  const [tenantId, setTenantId] = useState('');
  const [name, setName]         = useState('');
  const [err, setErr]           = useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const idErr   = validateTenantId(tenantId.trim());
    const nameErr = validateName(name.trim());
    if (idErr)   { setErr(idErr); return; }
    if (nameErr) { setErr(nameErr); return; }
    try {
      await onSubmit({ tenant_id: tenantId.trim(), name: name.trim() });
    } catch (e2) {
      setErr(errorMessage(e2, 'Failed to create tenant.'));
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4" aria-label="Create tenant form">
      <div>
        <label htmlFor="create-tenant-id" className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1.5">
          Tenant ID <span className="text-red-500">*</span>
        </label>
        <input
          id="create-tenant-id"
          name="tenant_id"
          autoFocus
          spellCheck={false}
          value={tenantId}
          onChange={e => { setTenantId(e.target.value); setErr(null); }}
          placeholder="acme"
          className="w-full rounded border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-950 font-mono text-[13px] text-gray-800 dark:text-zinc-200 px-3 py-2 focus:outline-none focus:border-indigo-500 transition-colors"
        />
        <p className="text-[10px] text-gray-300 dark:text-zinc-600 mt-1">
          Lowercase letters, digits, hyphens, underscores · max 63 chars · immutable
        </p>
      </div>
      <div>
        <label htmlFor="create-tenant-name" className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1.5">
          Display name <span className="text-red-500">*</span>
        </label>
        <input
          id="create-tenant-name"
          name="name"
          value={name}
          onChange={e => { setName(e.target.value); setErr(null); }}
          placeholder="ACME Inc"
          className="w-full rounded border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-950 text-[13px] text-gray-800 dark:text-zinc-200 px-3 py-2 focus:outline-none focus:border-indigo-500 transition-colors"
        />
        <p className="text-[10px] text-gray-300 dark:text-zinc-600 mt-1">
          Operator-facing label. Editable after creation.
        </p>
      </div>
      {err && <p className="text-[11px] text-red-400">{err}</p>}
      <div className="flex justify-end gap-2 pt-2">
        <button
          type="button"
          onClick={onCancel}
          className="px-3 py-1.5 rounded text-[12px] text-gray-400 dark:text-zinc-500 hover:text-gray-700 dark:hover:text-zinc-300 transition-colors"
        >
          Cancel
        </button>
        <button
          type="submit"
          disabled={submitting}
          className={clsx(
            'flex items-center gap-1.5 rounded px-3 py-1.5 text-[12px] font-medium text-white',
            'bg-indigo-600 hover:bg-indigo-500',
            'disabled:bg-gray-100 disabled:text-gray-400 dark:disabled:bg-zinc-800 dark:disabled:text-zinc-600 disabled:cursor-not-allowed transition-colors',
          )}
        >
          <Check size={11} /> {submitting ? 'Creating…' : 'Create'}
        </button>
      </div>
    </form>
  );
}

// ── Edit form ────────────────────────────────────────────────────────────────

interface EditFormProps {
  tenant: TenantRecord;
  onSubmit: (delta: { name?: string }) => Promise<void>;
  onCancel: () => void;
  submitting: boolean;
}

function EditForm({ tenant, onSubmit, onCancel, submitting }: EditFormProps) {
  const [name, setName] = useState(tenant.name);
  const [err, setErr]   = useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = name.trim();
    const nameErr = validateName(trimmed);
    if (nameErr) { setErr(nameErr); return; }
    // Only send changed fields. PATCH contract: omitted fields preserve
    // the stored value; an all-undefined body is rejected with 422.
    const delta: { name?: string } = {};
    if (trimmed !== tenant.name) delta.name = trimmed;
    if (Object.keys(delta).length === 0) {
      setErr('No changes to save.');
      return;
    }
    try {
      await onSubmit(delta);
    } catch (e2) {
      setErr(errorMessage(e2, 'Failed to update tenant.'));
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4" aria-label="Edit tenant form">
      <div>
        <label className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1.5">
          Tenant ID
        </label>
        <input
          value={tenant.tenant_id}
          disabled
          className="w-full rounded border border-gray-200 dark:border-zinc-800 bg-gray-100 dark:bg-zinc-900 font-mono text-[13px] text-gray-500 dark:text-zinc-500 px-3 py-2 cursor-not-allowed"
        />
        <p className="text-[10px] text-gray-300 dark:text-zinc-600 mt-1">Immutable.</p>
      </div>
      <div>
        <label htmlFor="edit-tenant-name" className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1.5">
          Display name <span className="text-red-500">*</span>
        </label>
        <input
          id="edit-tenant-name"
          name="name"
          autoFocus
          value={name}
          onChange={e => { setName(e.target.value); setErr(null); }}
          className="w-full rounded border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-950 text-[13px] text-gray-800 dark:text-zinc-200 px-3 py-2 focus:outline-none focus:border-indigo-500 transition-colors"
        />
      </div>
      {err && <p className="text-[11px] text-red-400">{err}</p>}
      <div className="flex justify-end gap-2 pt-2">
        <button
          type="button"
          onClick={onCancel}
          className="px-3 py-1.5 rounded text-[12px] text-gray-400 dark:text-zinc-500 hover:text-gray-700 dark:hover:text-zinc-300 transition-colors"
        >
          Cancel
        </button>
        <button
          type="submit"
          disabled={submitting}
          className={clsx(
            'flex items-center gap-1.5 rounded px-3 py-1.5 text-[12px] font-medium text-white',
            'bg-indigo-600 hover:bg-indigo-500',
            'disabled:bg-gray-100 disabled:text-gray-400 dark:disabled:bg-zinc-800 dark:disabled:text-zinc-600 disabled:cursor-not-allowed transition-colors',
          )}
        >
          <Check size={11} /> {submitting ? 'Saving…' : 'Save'}
        </button>
      </div>
    </form>
  );
}

// ── Modal shell ──────────────────────────────────────────────────────────────

function Modal({
  title,
  onClose,
  children,
}: {
  title: string;
  onClose: () => void;
  children: React.ReactNode;
}) {
  return (
    <>
      <div
        className="fixed inset-0 z-40 bg-black/60"
        onClick={onClose}
        aria-hidden="true"
      />
      <div
        role="dialog"
        aria-modal="true"
        aria-label={title}
        className="fixed inset-0 z-50 flex items-center justify-center p-4 pointer-events-none"
      >
        <div className="pointer-events-auto w-full max-w-md rounded-xl border border-gray-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 shadow-2xl">
          <div className="flex items-center justify-between px-4 h-11 border-b border-gray-200 dark:border-zinc-800">
            <span className="text-[13px] font-medium text-gray-800 dark:text-zinc-200">{title}</span>
            <button
              onClick={onClose}
              aria-label="Close"
              className="p-1 rounded text-gray-400 dark:text-zinc-600 hover:text-gray-700 dark:hover:text-zinc-300 hover:bg-gray-100 dark:hover:bg-zinc-800 transition-colors"
            >
              <X size={14} />
            </button>
          </div>
          <div className="p-4">{children}</div>
        </div>
      </div>
    </>
  );
}

// ── Row ──────────────────────────────────────────────────────────────────────

interface RowProps {
  tenant: TenantRecord;
  overview?: TenantOverview;
  onEdit: () => void;
}

function TenantRow({ tenant, overview, onEdit }: RowProps) {
  // operator count = sum of per-workspace member counts. Not the same
  // as "distinct operators" (PR-A4 will surface the operator list
  // directly), but it's the aggregate the overview endpoint gives us
  // today and it's the number operators recognise from the workspace
  // members page.
  const operatorTotal = overview?.total_members ?? 0;

  return (
    <tr className="border-t border-gray-100 dark:border-zinc-900 hover:bg-gray-50/60 dark:hover:bg-zinc-900/40 transition-colors">
      <td className="px-3 py-2 font-mono text-[12px] text-gray-800 dark:text-zinc-200">{tenant.tenant_id}</td>
      <td className="px-3 py-2 text-[12px] text-gray-700 dark:text-zinc-300">{tenant.name}</td>
      <td className="px-3 py-2 text-[12px] text-gray-500 dark:text-zinc-500 tabular-nums">
        {fmtDate(tenant.created_at)}
      </td>
      <td className="px-3 py-2 text-[12px] text-gray-700 dark:text-zinc-300 tabular-nums">
        {overview ? (
          <span className="inline-flex items-center gap-1">
            <Users size={11} className="text-gray-400 dark:text-zinc-600" />
            {operatorTotal}
          </span>
        ) : (
          <span className="text-gray-300 dark:text-zinc-700">—</span>
        )}
      </td>
      <td className="px-3 py-2 text-[12px] text-gray-700 dark:text-zinc-300 tabular-nums">
        {overview ? (
          <span className="inline-flex items-center gap-2.5 text-gray-500 dark:text-zinc-500">
            <span className="inline-flex items-center gap-1" title="Workspaces">
              <Briefcase size={11} />
              {overview.workspace_count}
            </span>
            <span className="inline-flex items-center gap-1" title="Active runs">
              <PlayIcon size={11} />
              {overview.active_runs}
            </span>
          </span>
        ) : (
          <span className="text-gray-300 dark:text-zinc-700">—</span>
        )}
      </td>
      <td className="px-3 py-2 text-right">
        <button
          type="button"
          onClick={onEdit}
          aria-label={`Edit tenant ${tenant.tenant_id}`}
          className="inline-flex items-center gap-1 px-2 py-1 rounded text-[11px] text-gray-500 dark:text-zinc-500 hover:text-indigo-500 dark:hover:text-indigo-400 hover:bg-indigo-500/10 transition-colors"
        >
          <Pencil size={11} /> Edit
        </button>
      </td>
    </tr>
  );
}

// ── Page ─────────────────────────────────────────────────────────────────────

export function TenantsPage() {
  const qc = useQueryClient();

  const [showCreate, setShowCreate] = useState(false);
  const [editing, setEditing]       = useState<TenantRecord | null>(null);
  const [filter, setFilter]         = useState('');

  const tenantsQuery = useQuery({
    queryKey: ['tenants'],
    queryFn:  () => defaultApi.listTenants({ limit: 200 }),
    staleTime: 10_000,
  });

  // Stable reference so downstream `useMemo` deps don't churn on every
  // render from the fresh `[]` fallback literal.
  const tenants = useMemo<TenantRecord[]>(
    () => tenantsQuery.data ?? [],
    [tenantsQuery.data],
  );

  // Fetch per-tenant overview in parallel so row counts populate. These
  // are independent queries; failures on any single tenant don't block
  // the others (the row falls back to `—`).
  const overviewQueries = useQueries({
    queries: tenants.map((t) => ({
      queryKey: ['tenant-overview', t.tenant_id] as const,
      queryFn:  () => defaultApi.getTenantOverview(t.tenant_id),
      staleTime: 30_000,
      retry: false,
    })),
  });
  const overviewById = useMemo(() => {
    const m = new Map<string, TenantOverview>();
    overviewQueries.forEach((q, idx) => {
      if (q.data) m.set(tenants[idx].tenant_id, q.data);
    });
    return m;
  }, [overviewQueries, tenants]);

  // Mutations ------------------------------------------------------------------

  const createMutation = useMutation({
    mutationFn: (body: { tenant_id: string; name: string }) => defaultApi.createTenant(body),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['tenants'] });
    },
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, delta }: { id: string; delta: { name?: string } }) =>
      defaultApi.updateTenant(id, delta),
    onSuccess: (_, { id }) => {
      void qc.invalidateQueries({ queryKey: ['tenants'] });
      void qc.invalidateQueries({ queryKey: ['tenant-overview', id] });
    },
  });

  // Handlers -------------------------------------------------------------------

  async function handleCreate(body: { tenant_id: string; name: string }) {
    await createMutation.mutateAsync(body);
    setShowCreate(false);
  }

  async function handleUpdate(delta: { name?: string }) {
    if (!editing) return;
    await updateMutation.mutateAsync({ id: editing.tenant_id, delta });
    setEditing(null);
  }

  // Filtering ------------------------------------------------------------------

  const filtered = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return tenants;
    return tenants.filter(
      t => t.tenant_id.toLowerCase().includes(q) || t.name.toLowerCase().includes(q),
    );
  }, [tenants, filter]);

  // Render ---------------------------------------------------------------------

  if (tenantsQuery.isError) {
    // 403 on listTenants is *possible* (operator isn't admin on any
    // tenant visible to this token) but rare — the AdminGate probe is
    // against a single tenant, and listTenants is unfiltered. Render
    // the same error fallback we use everywhere else.
    return (
      <ErrorFallback
        error={tenantsQuery.error}
        resource="tenants"
        onRetry={() => void tenantsQuery.refetch()}
      />
    );
  }

  return (
    <div className="flex flex-col h-full bg-white dark:bg-zinc-950 overflow-y-auto">
      <div className="max-w-5xl mx-auto px-5 py-5 space-y-5 w-full">

        {/* Header */}
        <div className="flex items-start justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-lg bg-indigo-500/10 flex items-center justify-center shrink-0">
              <Building2 size={16} className="text-indigo-400" />
            </div>
            <div>
              <h1 className="text-[15px] font-semibold text-gray-900 dark:text-zinc-100">Tenants</h1>
              <p className="text-[11px] text-gray-400 dark:text-zinc-600 mt-0.5">
                Admin-only. Every tenant you hold <code className="font-mono">TenantRole::Admin</code> on.
              </p>
              <EntityExplainer className="mt-1">
                Tenants are the top-level isolation boundary. Each tenant owns
                its own workspaces, projects, operators, quotas, and retention
                policy — nothing crosses the tenant boundary.
              </EntityExplainer>
            </div>
          </div>

          <div className="flex items-center gap-2 shrink-0">
            <button
              type="button"
              onClick={() => void tenantsQuery.refetch()}
              disabled={tenantsQuery.isFetching}
              aria-label="Refresh tenants"
              className="p-1.5 rounded text-gray-400 dark:text-zinc-500 hover:text-gray-700 dark:hover:text-zinc-300 hover:bg-gray-100 dark:hover:bg-zinc-800 transition-colors disabled:opacity-40"
              title="Refresh"
            >
              <RefreshCw size={14} className={tenantsQuery.isFetching ? 'animate-spin' : ''} />
            </button>
            <button
              type="button"
              onClick={() => setShowCreate(true)}
              className="flex items-center gap-1.5 rounded px-3 py-1.5 text-[12px] font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors"
            >
              <Plus size={12} /> New tenant
            </button>
          </div>
        </div>

        {/* Filter */}
        <div className="relative">
          <input
            value={filter}
            onChange={e => setFilter(e.target.value)}
            placeholder="Filter tenants…"
            aria-label="Filter tenants"
            className="w-full rounded-lg border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-900 text-[13px] text-gray-800 dark:text-zinc-200 placeholder-gray-400 dark:placeholder-zinc-600 px-3 py-2 focus:outline-none focus:border-indigo-500 transition-colors"
          />
          {filter && (
            <button
              onClick={() => setFilter('')}
              aria-label="Clear filter"
              className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 dark:text-zinc-600 hover:text-gray-500 dark:hover:text-zinc-400"
            >
              ×
            </button>
          )}
        </div>

        {/* Table / states */}
        {tenantsQuery.isLoading ? (
          <div className="space-y-2">
            {[1, 2, 3].map(i => (
              <div key={i} className="h-9 rounded bg-gray-100 dark:bg-zinc-900 animate-pulse" />
            ))}
          </div>
        ) : tenants.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-16 gap-3 text-center">
            <Building2 size={28} className="text-gray-300 dark:text-zinc-700" />
            <p className="text-[13px] text-gray-400 dark:text-zinc-500">No tenants yet.</p>
            <p className="text-[11px] text-gray-300 dark:text-zinc-600 max-w-md">
              Click <span className="font-medium">New tenant</span> above to create one.
              Tenants are the top-level isolation boundary — each has its own
              workspaces, projects, and operators.
            </p>
          </div>
        ) : filtered.length === 0 ? (
          <div className="flex items-center justify-center py-10 text-[13px] text-gray-400 dark:text-zinc-500">
            No tenants match &ldquo;{filter}&rdquo;.
          </div>
        ) : (
          <div className="rounded-lg border border-gray-200 dark:border-zinc-800 overflow-hidden">
            <table className="w-full border-collapse">
              <thead>
                <tr className="bg-gray-50 dark:bg-zinc-900/60 text-left">
                  <th className="px-3 py-2 text-[10px] font-medium uppercase tracking-wider text-gray-400 dark:text-zinc-500">
                    tenant_id
                  </th>
                  <th className="px-3 py-2 text-[10px] font-medium uppercase tracking-wider text-gray-400 dark:text-zinc-500">
                    Name
                  </th>
                  <th className="px-3 py-2 text-[10px] font-medium uppercase tracking-wider text-gray-400 dark:text-zinc-500">
                    Created
                  </th>
                  <th className="px-3 py-2 text-[10px] font-medium uppercase tracking-wider text-gray-400 dark:text-zinc-500">
                    Operators
                  </th>
                  <th className="px-3 py-2 text-[10px] font-medium uppercase tracking-wider text-gray-400 dark:text-zinc-500">
                    Scope
                  </th>
                  <th className="px-3 py-2" />
                </tr>
              </thead>
              <tbody>
                {filtered.map(t => (
                  <TenantRow
                    key={t.tenant_id}
                    tenant={t}
                    overview={overviewById.get(t.tenant_id)}
                    onEdit={() => setEditing(t)}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Footer hint */}
        {tenants.length > 0 && (
          <p className="text-[11px] text-gray-400 dark:text-zinc-600 text-center pb-2">
            {tenants.length} tenant{tenants.length === 1 ? '' : 's'} visible
            {filter && ` · ${filtered.length} match "${filter}"`}
          </p>
        )}
      </div>

      {/* Create modal */}
      {showCreate && (
        <Modal title="Create tenant" onClose={() => setShowCreate(false)}>
          <CreateForm
            onSubmit={async (body) => {
              // Surface the backend error inside the form so the dialog
              // remains open and operators can retry without losing input.
              await handleCreate(body);
            }}
            onCancel={() => setShowCreate(false)}
            submitting={createMutation.isPending}
          />
        </Modal>
      )}

      {/* Edit modal */}
      {editing && (
        <Modal title="Edit tenant" onClose={() => setEditing(null)}>
          <EditForm
            tenant={editing}
            onSubmit={async (delta) => {
              await handleUpdate(delta);
            }}
            onCancel={() => setEditing(null)}
            submitting={updateMutation.isPending}
          />
        </Modal>
      )}
    </div>
  );
}

export default TenantsPage;
