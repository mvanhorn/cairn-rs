/**
 * OperatorsPage — RFC-026 PR-A4 admin surface.
 *
 * List every operator profile under the active scope's tenant (admin-
 * gated), expose Create + Edit flows, and let a tenant-admin grant or
 * revoke tenant-role grants per operator. The page assumes it is
 * wrapped in `<AdminGate>` so nav is hidden for non-admins; the
 * server-side `TenantAdminGuard` is the authoritative gate and will
 * respond 403 with `tenant_role_missing` for any call that slips
 * through.
 *
 * Backend contract (all live as of PR-A4):
 *   GET    /v1/admin/tenants/:tenant_id/operator-profiles           list
 *   POST   /v1/admin/tenants/:tenant_id/operator-profiles           create
 *   PATCH  /v1/admin/tenants/:tenant_id/operator-profiles/:id       edit
 *   GET    /v1/admin/tenants/:tenant_id/operators/:id/tenant-roles  grants (PR-A4)
 *   POST   /v1/admin/operators/:id/tenant-roles/:tenant/promote     grant
 *   DELETE /v1/admin/operators/:id/tenant-roles/:tenant             revoke
 *
 * The edit modal edits the operator's WorkspaceRole (the default role
 * carried on the profile). The tenant-scope TenantRole is edited in
 * the expanded grants section — a separate concept from WorkspaceRole.
 */

import { useMemo, useState } from 'react';
import {
  useMutation, useQueries, useQuery, useQueryClient,
} from '@tanstack/react-query';
import {
  Users, Plus, RefreshCw, Pencil, Check, X, ChevronRight, ChevronDown,
  ShieldCheck, Trash2, Mail, UserCog,
} from 'lucide-react';
import { clsx } from 'clsx';

import { defaultApi } from '../lib/api';
import type {
  OperatorProfile, TenantRoleGrant, WorkspaceRole, TenantRole,
} from '../lib/types';
import { errorMessage } from '../lib/errors';
import { useScope } from '../hooks/useScope';
import { EntityExplainer } from '../components/EntityExplainer';
import { ErrorFallback } from '../components/ErrorFallback';

// ── Constants ────────────────────────────────────────────────────────────────

const WORKSPACE_ROLES: WorkspaceRole[] = ['viewer', 'member', 'admin', 'owner'];
const TENANT_ROLES:    TenantRole[]    = ['read_only', 'member', 'admin'];

const TENANT_ID_RE = /^[a-z0-9][a-z0-9_-]{0,62}$/;

// ── Validation ───────────────────────────────────────────────────────────────

function validateDisplayName(name: string): string | null {
  if (!name.trim()) return 'Display name is required.';
  if (name.length > 128) return 'Display name must be 128 characters or fewer.';
  return null;
}

function validateEmail(email: string): string | null {
  const trimmed = email.trim();
  if (!trimmed) return 'Email is required.';
  // Mirror backend's split-and-domain-dot check so typos are rejected
  // client-side rather than making the operator wait for a 422 round-
  // trip.
  const at = trimmed.indexOf('@');
  if (at <= 0 || at === trimmed.length - 1) return 'Email must be of the form <local>@<domain>.';
  const domain = trimmed.slice(at + 1);
  if (!domain.includes('.')) return 'Email must include a dot in the domain.';
  return null;
}

function validateTenantId(id: string): string | null {
  if (!id.trim()) return 'Tenant ID is required.';
  if (!TENANT_ID_RE.test(id)) return 'Lowercase letters, digits, hyphens, underscores (max 63 chars).';
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
  onSubmit: (body: {
    display_name: string;
    email: string;
    role: WorkspaceRole;
  }) => Promise<void>;
  onCancel: () => void;
  submitting: boolean;
}

function CreateForm({ onSubmit, onCancel, submitting }: CreateFormProps) {
  const [displayName, setDisplayName] = useState('');
  const [email, setEmail]             = useState('');
  const [role, setRole]               = useState<WorkspaceRole>('member');
  const [err, setErr]                 = useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const nameErr  = validateDisplayName(displayName.trim());
    if (nameErr)  { setErr(nameErr);  return; }
    const emailErr = validateEmail(email.trim());
    if (emailErr) { setErr(emailErr); return; }
    try {
      await onSubmit({
        display_name: displayName.trim(),
        email:        email.trim(),
        role,
      });
    } catch (e2) {
      setErr(errorMessage(e2, 'Failed to create operator.'));
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4" aria-label="Create operator form">
      <div>
        <label htmlFor="create-op-name" className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1.5">
          Display name <span className="text-red-500">*</span>
        </label>
        <input
          id="create-op-name"
          name="display_name"
          autoFocus
          value={displayName}
          onChange={e => { setDisplayName(e.target.value); setErr(null); }}
          placeholder="Alice Operator"
          className="w-full rounded border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-950 text-[13px] text-gray-800 dark:text-zinc-200 px-3 py-2 focus:outline-none focus:border-indigo-500 transition-colors"
        />
      </div>
      <div>
        <label htmlFor="create-op-email" className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1.5">
          Email <span className="text-red-500">*</span>
        </label>
        <input
          id="create-op-email"
          name="email"
          type="email"
          value={email}
          onChange={e => { setEmail(e.target.value); setErr(null); }}
          placeholder="alice@example.com"
          className="w-full rounded border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-950 text-[13px] text-gray-800 dark:text-zinc-200 px-3 py-2 focus:outline-none focus:border-indigo-500 transition-colors"
        />
      </div>
      <div>
        <label htmlFor="create-op-role" className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1.5">
          Role <span className="text-red-500">*</span>
        </label>
        <select
          id="create-op-role"
          name="role"
          value={role}
          onChange={e => { setRole(e.target.value as WorkspaceRole); setErr(null); }}
          className="w-full rounded border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-950 text-[13px] text-gray-800 dark:text-zinc-200 px-3 py-2 focus:outline-none focus:border-indigo-500 transition-colors"
        >
          {WORKSPACE_ROLES.map(r => (
            <option key={r} value={r}>{r}</option>
          ))}
        </select>
        <p className="text-[10px] text-gray-300 dark:text-zinc-600 mt-1">
          Default workspace role — controls access when the operator joins a new
          workspace. Tenant-admin grants are managed separately below.
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
  operator: OperatorProfile;
  onSubmit: (delta: {
    display_name?: string;
    email?: string;
    role?: WorkspaceRole;
  }) => Promise<void>;
  onCancel: () => void;
  submitting: boolean;
}

function EditForm({ operator, onSubmit, onCancel, submitting }: EditFormProps) {
  const [displayName, setDisplayName] = useState(operator.display_name);
  const [email, setEmail]             = useState(operator.email);
  const [role, setRole]               = useState<WorkspaceRole>(operator.role);
  const [err, setErr]                 = useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();

    const nameTrim  = displayName.trim();
    const emailTrim = email.trim();

    // Per-field validation only if the field has actually changed —
    // unchanged fields aren't sent, so the server doesn't need them to
    // be re-validated against stricter-than-stored rules.
    if (nameTrim !== operator.display_name) {
      const ne = validateDisplayName(nameTrim);
      if (ne) { setErr(ne); return; }
    }
    if (emailTrim !== operator.email) {
      const ee = validateEmail(emailTrim);
      if (ee) { setErr(ee); return; }
    }

    const delta: { display_name?: string; email?: string; role?: WorkspaceRole } = {};
    if (nameTrim  !== operator.display_name) delta.display_name = nameTrim;
    if (emailTrim !== operator.email)        delta.email        = emailTrim;
    if (role      !== operator.role)         delta.role         = role;
    if (Object.keys(delta).length === 0) {
      setErr('No changes to save.');
      return;
    }
    try {
      await onSubmit(delta);
    } catch (e2) {
      setErr(errorMessage(e2, 'Failed to update operator.'));
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4" aria-label="Edit operator form">
      <div>
        <label className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1.5">
          Operator ID
        </label>
        <input
          value={operator.operator_id}
          disabled
          className="w-full rounded border border-gray-200 dark:border-zinc-800 bg-gray-100 dark:bg-zinc-900 font-mono text-[13px] text-gray-500 dark:text-zinc-500 px-3 py-2 cursor-not-allowed"
        />
        <p className="text-[10px] text-gray-300 dark:text-zinc-600 mt-1">Immutable.</p>
      </div>
      <div>
        <label htmlFor="edit-op-name" className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1.5">
          Display name <span className="text-red-500">*</span>
        </label>
        <input
          id="edit-op-name"
          name="display_name"
          autoFocus
          value={displayName}
          onChange={e => { setDisplayName(e.target.value); setErr(null); }}
          className="w-full rounded border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-950 text-[13px] text-gray-800 dark:text-zinc-200 px-3 py-2 focus:outline-none focus:border-indigo-500 transition-colors"
        />
      </div>
      <div>
        <label htmlFor="edit-op-email" className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1.5">
          Email <span className="text-red-500">*</span>
        </label>
        <input
          id="edit-op-email"
          name="email"
          type="email"
          value={email}
          onChange={e => { setEmail(e.target.value); setErr(null); }}
          className="w-full rounded border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-950 text-[13px] text-gray-800 dark:text-zinc-200 px-3 py-2 focus:outline-none focus:border-indigo-500 transition-colors"
        />
      </div>
      <div>
        <label htmlFor="edit-op-role" className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1.5">
          Role <span className="text-red-500">*</span>
        </label>
        <select
          id="edit-op-role"
          name="role"
          value={role}
          onChange={e => { setRole(e.target.value as WorkspaceRole); setErr(null); }}
          className="w-full rounded border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-950 text-[13px] text-gray-800 dark:text-zinc-200 px-3 py-2 focus:outline-none focus:border-indigo-500 transition-colors"
        >
          {WORKSPACE_ROLES.map(r => (
            <option key={r} value={r}>{r}</option>
          ))}
        </select>
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

// ── Tenant-role grants section ───────────────────────────────────────────────

interface GrantsSectionProps {
  operatorId: string;
  onGrant: (tenantId: string, role: TenantRole) => Promise<void>;
  onRevoke: (tenantId: string) => Promise<void>;
  /** Active scope tenant — used to pre-fill the grant form when the
   *  admin wants to promote on the tenant they're already viewing. */
  activeTenantId: string;
}

function GrantsSection({ operatorId, onGrant, onRevoke, activeTenantId }: GrantsSectionProps) {
  const grantsQuery = useQuery({
    queryKey: ['operator-tenant-roles', activeTenantId, operatorId],
    queryFn:  () => defaultApi.listOperatorTenantRoles(activeTenantId, operatorId),
    staleTime: 15_000,
    retry: false,
  });
  const grants = useMemo<TenantRoleGrant[]>(
    () => grantsQuery.data?.items ?? [],
    [grantsQuery.data],
  );

  const [grantTenant, setGrantTenant] = useState(activeTenantId);
  const [grantRole,   setGrantRole]   = useState<TenantRole>('admin');
  const [grantErr,    setGrantErr]    = useState<string | null>(null);
  const [submitting,  setSubmitting]  = useState(false);

  async function handleGrant(e: React.FormEvent) {
    e.preventDefault();
    const tenantErr = validateTenantId(grantTenant);
    if (tenantErr) { setGrantErr(tenantErr); return; }
    setSubmitting(true);
    try {
      await onGrant(grantTenant.trim(), grantRole);
      setGrantErr(null);
    } catch (e2) {
      setGrantErr(errorMessage(e2, 'Failed to grant tenant role.'));
    } finally {
      setSubmitting(false);
    }
  }

  if (grantsQuery.isLoading) {
    return (
      <div className="bg-gray-50 dark:bg-zinc-900/40 border-t border-gray-100 dark:border-zinc-900 px-6 py-4">
        <p className="text-[12px] text-gray-400 dark:text-zinc-500">Loading tenant roles…</p>
      </div>
    );
  }

  if (grantsQuery.isError) {
    return (
      <div className="bg-gray-50 dark:bg-zinc-900/40 border-t border-gray-100 dark:border-zinc-900 px-6 py-4">
        <p className="text-[11px] text-red-400">
          {errorMessage(grantsQuery.error, 'Failed to load tenant roles.')}
        </p>
      </div>
    );
  }

  return (
    <div className="bg-gray-50 dark:bg-zinc-900/40 border-t border-gray-100 dark:border-zinc-900 px-6 py-4 space-y-3">
      <h4 className="text-[11px] font-medium uppercase tracking-wider text-gray-400 dark:text-zinc-500">
        Tenant roles
      </h4>

      {grants.length === 0 ? (
        <p className="text-[12px] text-gray-400 dark:text-zinc-500">
          No tenant-role grants yet. Use the form below to grant this operator
          tenant-admin (or member / read-only) on a tenant.
        </p>
      ) : (
        <table className="w-full border-collapse">
          <thead>
            <tr className="text-left">
              <th className="px-2 py-1.5 text-[10px] font-medium uppercase tracking-wider text-gray-400 dark:text-zinc-500">
                tenant
              </th>
              <th className="px-2 py-1.5 text-[10px] font-medium uppercase tracking-wider text-gray-400 dark:text-zinc-500">
                role
              </th>
              <th className="px-2 py-1.5 text-[10px] font-medium uppercase tracking-wider text-gray-400 dark:text-zinc-500">
                status
              </th>
              <th className="px-2 py-1.5 text-[10px] font-medium uppercase tracking-wider text-gray-400 dark:text-zinc-500">
                granted
              </th>
              <th className="px-2 py-1.5" />
            </tr>
          </thead>
          <tbody>
            {grants.map(g => {
              const revoked = g.revoked_at_ms != null;
              return (
                <tr
                  key={`${g.tenant_id}-${g.granted_at_ms}`}
                  className="border-t border-gray-100 dark:border-zinc-900"
                >
                  <td className="px-2 py-1.5 font-mono text-[12px] text-gray-800 dark:text-zinc-200">
                    {g.tenant_id}
                  </td>
                  <td className="px-2 py-1.5 text-[12px]">
                    <span className={clsx(
                      'inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-[11px]',
                      g.role === 'admin'
                        ? 'bg-indigo-500/10 text-indigo-500'
                        : g.role === 'member'
                          ? 'bg-sky-500/10 text-sky-500'
                          : 'bg-gray-100 dark:bg-zinc-800 text-gray-500 dark:text-zinc-400',
                    )}>
                      <ShieldCheck size={10} /> {g.role}
                    </span>
                  </td>
                  <td className="px-2 py-1.5 text-[12px]">
                    {revoked ? (
                      <span className="text-gray-400 dark:text-zinc-500">
                        revoked · {fmtDate(g.revoked_at_ms ?? 0)}
                      </span>
                    ) : (
                      <span className="text-emerald-500">active</span>
                    )}
                  </td>
                  <td className="px-2 py-1.5 text-[12px] text-gray-500 dark:text-zinc-500 tabular-nums">
                    {fmtDate(g.granted_at_ms)}
                    {g.granted_by && (
                      <span className="ml-2 text-gray-400 dark:text-zinc-600">by {g.granted_by}</span>
                    )}
                  </td>
                  <td className="px-2 py-1.5 text-right">
                    {!revoked && (
                      <button
                        type="button"
                        onClick={() => { void onRevoke(g.tenant_id); }}
                        aria-label={`Revoke tenant role ${g.tenant_id} for ${operatorId}`}
                        className="inline-flex items-center gap-1 px-2 py-1 rounded text-[11px] text-gray-500 dark:text-zinc-500 hover:text-red-500 dark:hover:text-red-400 hover:bg-red-500/10 transition-colors"
                      >
                        <Trash2 size={11} /> Revoke
                      </button>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}

      {/* Grant form */}
      <form onSubmit={handleGrant} className="flex flex-wrap items-end gap-3 pt-2">
        <div className="flex-1 min-w-[160px]">
          <label htmlFor={`grant-tenant-${operatorId}`} className="block text-[10px] text-gray-400 dark:text-zinc-500 mb-1">
            Grant tenant id
          </label>
          <input
            id={`grant-tenant-${operatorId}`}
            value={grantTenant}
            onChange={e => { setGrantTenant(e.target.value); setGrantErr(null); }}
            placeholder="acme"
            className="w-full rounded border border-gray-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 font-mono text-[12px] text-gray-800 dark:text-zinc-200 px-2.5 py-1.5 focus:outline-none focus:border-indigo-500 transition-colors"
          />
        </div>
        <div>
          <label htmlFor={`grant-role-${operatorId}`} className="block text-[10px] text-gray-400 dark:text-zinc-500 mb-1">
            Grant role
          </label>
          <select
            id={`grant-role-${operatorId}`}
            value={grantRole}
            onChange={e => { setGrantRole(e.target.value as TenantRole); setGrantErr(null); }}
            className="rounded border border-gray-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 text-[12px] text-gray-800 dark:text-zinc-200 px-2.5 py-1.5 focus:outline-none focus:border-indigo-500 transition-colors"
          >
            {TENANT_ROLES.map(r => (
              <option key={r} value={r}>{r}</option>
            ))}
          </select>
        </div>
        <button
          type="submit"
          disabled={submitting}
          className={clsx(
            'flex items-center gap-1.5 rounded px-3 py-1.5 text-[12px] font-medium text-white',
            'bg-indigo-600 hover:bg-indigo-500',
            'disabled:bg-gray-100 disabled:text-gray-400 dark:disabled:bg-zinc-800 dark:disabled:text-zinc-600 disabled:cursor-not-allowed transition-colors',
          )}
        >
          <Plus size={11} /> {submitting ? 'Granting…' : 'Grant'}
        </button>
      </form>
      {grantErr && <p className="text-[11px] text-red-400">{grantErr}</p>}
    </div>
  );
}

// ── Row ──────────────────────────────────────────────────────────────────────

interface RowProps {
  operator: OperatorProfile;
  expanded: boolean;
  onToggleExpand: () => void;
  onEdit: () => void;
  onGrant: (tenantId: string, role: TenantRole) => Promise<void>;
  onRevoke: (tenantId: string) => Promise<void>;
  activeTenantId: string;
}

function OperatorRow({
  operator, expanded, onToggleExpand, onEdit, onGrant, onRevoke, activeTenantId,
}: RowProps) {
  return (
    <>
      <tr className="border-t border-gray-100 dark:border-zinc-900 hover:bg-gray-50/60 dark:hover:bg-zinc-900/40 transition-colors">
        <td className="px-3 py-2 w-8">
          <button
            type="button"
            onClick={onToggleExpand}
            aria-label={`Tenant roles for ${operator.operator_id}`}
            aria-expanded={expanded}
            className="p-1 rounded text-gray-400 dark:text-zinc-500 hover:text-gray-700 dark:hover:text-zinc-300 hover:bg-gray-100 dark:hover:bg-zinc-800 transition-colors"
          >
            {expanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
          </button>
        </td>
        <td className="px-3 py-2 font-mono text-[12px] text-gray-800 dark:text-zinc-200">
          {operator.operator_id}
        </td>
        <td className="px-3 py-2 text-[12px] text-gray-700 dark:text-zinc-300">
          {operator.display_name}
        </td>
        <td className="px-3 py-2 text-[12px] text-gray-500 dark:text-zinc-500">
          <span className="inline-flex items-center gap-1">
            <Mail size={11} className="text-gray-400 dark:text-zinc-600" />
            {operator.email}
          </span>
        </td>
        <td className="px-3 py-2 text-[12px]">
          <span className={clsx(
            'inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-[11px]',
            operator.role === 'owner' || operator.role === 'admin'
              ? 'bg-indigo-500/10 text-indigo-500'
              : operator.role === 'member'
                ? 'bg-sky-500/10 text-sky-500'
                : 'bg-gray-100 dark:bg-zinc-800 text-gray-500 dark:text-zinc-400',
          )}>
            <UserCog size={10} /> {operator.role}
          </span>
        </td>
        <td className="px-3 py-2 text-right">
          <button
            type="button"
            onClick={onEdit}
            aria-label={`Edit operator ${operator.operator_id}`}
            className="inline-flex items-center gap-1 px-2 py-1 rounded text-[11px] text-gray-500 dark:text-zinc-500 hover:text-indigo-500 dark:hover:text-indigo-400 hover:bg-indigo-500/10 transition-colors"
          >
            <Pencil size={11} /> Edit
          </button>
        </td>
      </tr>
      {expanded && (
        <tr className="border-t border-gray-100 dark:border-zinc-900">
          <td colSpan={6} className="p-0">
            <GrantsSection
              operatorId={operator.operator_id}
              onGrant={onGrant}
              onRevoke={onRevoke}
              activeTenantId={activeTenantId}
            />
          </td>
        </tr>
      )}
    </>
  );
}

// ── Page ─────────────────────────────────────────────────────────────────────

export function OperatorsPage() {
  const qc = useQueryClient();
  const [scope] = useScope();
  const tenantId = scope.tenant_id;

  const [showCreate, setShowCreate] = useState(false);
  const [editing, setEditing]       = useState<OperatorProfile | null>(null);
  const [filter, setFilter]         = useState('');
  const [expanded, setExpanded]     = useState<Record<string, boolean>>({});

  const operatorsQuery = useQuery({
    queryKey: ['operators', tenantId],
    queryFn:  () => defaultApi.listOperatorProfiles(tenantId, { limit: 200 }),
    staleTime: 10_000,
    enabled: Boolean(tenantId),
  });

  const operators = useMemo<OperatorProfile[]>(
    () => operatorsQuery.data?.items ?? [],
    [operatorsQuery.data],
  );

  // Prefetch tenant-role counts per operator in parallel so the row
  // can surface "N tenants" without an expansion click. Failures are
  // silent — the row just omits the count.
  const grantQueries = useQueries({
    queries: operators.map(op => ({
      queryKey: ['operator-tenant-roles', tenantId, op.operator_id] as const,
      queryFn:  () => defaultApi.listOperatorTenantRoles(tenantId, op.operator_id),
      staleTime: 15_000,
      retry: false,
      enabled: Boolean(tenantId),
    })),
  });
  const grantCountById = useMemo(() => {
    const m = new Map<string, number>();
    grantQueries.forEach((q, idx) => {
      if (q.data) {
        const active = q.data.items.filter(g => g.revoked_at_ms == null).length;
        m.set(operators[idx].operator_id, active);
      }
    });
    return m;
  }, [grantQueries, operators]);

  // ── Mutations ─────────────────────────────────────────────────────────────

  const createMutation = useMutation({
    mutationFn: (body: { display_name: string; email: string; role: WorkspaceRole }) =>
      defaultApi.createOperatorProfile(tenantId, body),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['operators', tenantId] });
    },
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, delta }: {
      id: string;
      delta: { display_name?: string; email?: string; role?: WorkspaceRole };
    }) => defaultApi.updateOperatorProfile(tenantId, id, delta),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['operators', tenantId] });
    },
  });

  const promoteMutation = useMutation({
    mutationFn: ({ operatorId, targetTenant, role }: {
      operatorId: string;
      targetTenant: string;
      role: TenantRole;
    }) => defaultApi.promoteOperatorTenantRole(operatorId, targetTenant, role),
    onSuccess: (_, { operatorId }) => {
      void qc.invalidateQueries({
        queryKey: ['operator-tenant-roles', tenantId, operatorId],
      });
    },
  });

  const revokeMutation = useMutation({
    mutationFn: ({ operatorId, targetTenant }: {
      operatorId: string;
      targetTenant: string;
    }) => defaultApi.revokeOperatorTenantRole(operatorId, targetTenant),
    onSuccess: (_, { operatorId }) => {
      void qc.invalidateQueries({
        queryKey: ['operator-tenant-roles', tenantId, operatorId],
      });
    },
  });

  // ── Handlers ──────────────────────────────────────────────────────────────

  async function handleCreate(body: {
    display_name: string;
    email: string;
    role: WorkspaceRole;
  }) {
    await createMutation.mutateAsync(body);
    setShowCreate(false);
  }

  async function handleUpdate(delta: {
    display_name?: string;
    email?: string;
    role?: WorkspaceRole;
  }) {
    if (!editing) return;
    await updateMutation.mutateAsync({ id: editing.operator_id, delta });
    setEditing(null);
  }

  function makeGrantHandler(operatorId: string) {
    return async (targetTenant: string, role: TenantRole) => {
      await promoteMutation.mutateAsync({ operatorId, targetTenant, role });
    };
  }

  function makeRevokeHandler(operatorId: string) {
    return async (targetTenant: string) => {
      await revokeMutation.mutateAsync({ operatorId, targetTenant });
    };
  }

  // ── Filtering ─────────────────────────────────────────────────────────────

  const filtered = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return operators;
    return operators.filter(
      op =>
        op.operator_id.toLowerCase().includes(q) ||
        op.display_name.toLowerCase().includes(q) ||
        op.email.toLowerCase().includes(q),
    );
  }, [operators, filter]);

  // ── Render ────────────────────────────────────────────────────────────────

  if (operatorsQuery.isError) {
    return (
      <ErrorFallback
        error={operatorsQuery.error}
        resource="operators"
        onRetry={() => void operatorsQuery.refetch()}
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
              <Users size={16} className="text-indigo-400" />
            </div>
            <div>
              <h1 className="text-[15px] font-semibold text-gray-900 dark:text-zinc-100">Operators</h1>
              <p className="text-[11px] text-gray-400 dark:text-zinc-600 mt-0.5">
                Admin-only. Every operator that belongs to tenant{' '}
                <code className="font-mono">{tenantId}</code>.
              </p>
              <EntityExplainer className="mt-1">
                {'Operators are the humans (or service accounts) that act inside a tenant. Each carries a default WorkspaceRole used when they join a new workspace, plus optional TenantRole grants that unlock admin powers on specific tenants. Tenant grants are managed per-row below.'}
              </EntityExplainer>
            </div>
          </div>

          <div className="flex items-center gap-2 shrink-0">
            <button
              type="button"
              onClick={() => void operatorsQuery.refetch()}
              disabled={operatorsQuery.isFetching}
              aria-label="Refresh operators"
              className="p-1.5 rounded text-gray-400 dark:text-zinc-500 hover:text-gray-700 dark:hover:text-zinc-300 hover:bg-gray-100 dark:hover:bg-zinc-800 transition-colors disabled:opacity-40"
              title="Refresh"
            >
              <RefreshCw size={14} className={operatorsQuery.isFetching ? 'animate-spin' : ''} />
            </button>
            <button
              type="button"
              onClick={() => setShowCreate(true)}
              className="flex items-center gap-1.5 rounded px-3 py-1.5 text-[12px] font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors"
            >
              <Plus size={12} /> New operator
            </button>
          </div>
        </div>

        {/* Filter */}
        <div className="relative">
          <input
            value={filter}
            onChange={e => setFilter(e.target.value)}
            placeholder="Filter operators…"
            aria-label="Filter operators"
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
        {operatorsQuery.isLoading ? (
          <div className="space-y-2">
            {[1, 2, 3].map(i => (
              <div key={i} className="h-9 rounded bg-gray-100 dark:bg-zinc-900 animate-pulse" />
            ))}
          </div>
        ) : operators.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-16 gap-3 text-center">
            <Users size={28} className="text-gray-300 dark:text-zinc-700" />
            <p className="text-[13px] text-gray-400 dark:text-zinc-500">No operators yet.</p>
            <p className="text-[11px] text-gray-300 dark:text-zinc-600 max-w-md">
              Click <span className="font-medium">New operator</span> above to create one.
              Each operator gets a default workspace role; grant them tenant-admin
              powers via the per-row grants section after creation.
            </p>
          </div>
        ) : filtered.length === 0 ? (
          <div className="flex items-center justify-center py-10 text-[13px] text-gray-400 dark:text-zinc-500">
            No operators match &ldquo;{filter}&rdquo;.
          </div>
        ) : (
          <div className="rounded-lg border border-gray-200 dark:border-zinc-800 overflow-hidden">
            <table className="w-full border-collapse">
              <thead>
                <tr className="bg-gray-50 dark:bg-zinc-900/60 text-left">
                  <th className="px-3 py-2 w-8" />
                  <th className="px-3 py-2 text-[10px] font-medium uppercase tracking-wider text-gray-400 dark:text-zinc-500">
                    operator_id
                  </th>
                  <th className="px-3 py-2 text-[10px] font-medium uppercase tracking-wider text-gray-400 dark:text-zinc-500">
                    Name
                  </th>
                  <th className="px-3 py-2 text-[10px] font-medium uppercase tracking-wider text-gray-400 dark:text-zinc-500">
                    Email
                  </th>
                  <th className="px-3 py-2 text-[10px] font-medium uppercase tracking-wider text-gray-400 dark:text-zinc-500">
                    Role
                  </th>
                  <th className="px-3 py-2" />
                </tr>
              </thead>
              <tbody>
                {filtered.map(op => (
                  <OperatorRow
                    key={op.operator_id}
                    operator={op}
                    expanded={Boolean(expanded[op.operator_id])}
                    onToggleExpand={() =>
                      setExpanded(prev => ({
                        ...prev,
                        [op.operator_id]: !prev[op.operator_id],
                      }))
                    }
                    onEdit={() => setEditing(op)}
                    onGrant={makeGrantHandler(op.operator_id)}
                    onRevoke={makeRevokeHandler(op.operator_id)}
                    activeTenantId={tenantId}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Footer hint */}
        {operators.length > 0 && (
          <p className="text-[11px] text-gray-400 dark:text-zinc-600 text-center pb-2">
            {operators.length} operator{operators.length === 1 ? '' : 's'} visible
            {filter && ` · ${filtered.length} match "${filter}"`}
            {grantCountById.size > 0 && (
              <span className="ml-2">
                · {Array.from(grantCountById.values()).reduce((a, b) => a + b, 0)} active
                tenant-role grant{Array.from(grantCountById.values()).reduce((a, b) => a + b, 0) === 1 ? '' : 's'}
              </span>
            )}
          </p>
        )}
      </div>

      {/* Create modal */}
      {showCreate && (
        <Modal title="Create operator" onClose={() => setShowCreate(false)}>
          <CreateForm
            onSubmit={handleCreate}
            onCancel={() => setShowCreate(false)}
            submitting={createMutation.isPending}
          />
        </Modal>
      )}

      {/* Edit modal */}
      {editing && (
        <Modal title="Edit operator" onClose={() => setEditing(null)}>
          <EditForm
            operator={editing}
            onSubmit={handleUpdate}
            onCancel={() => setEditing(null)}
            submitting={updateMutation.isPending}
          />
        </Modal>
      )}
    </div>
  );
}

export default OperatorsPage;
