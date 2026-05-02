/**
 * AdminGate — RFC-026 PR-A3 client-side role gate for admin surfaces.
 *
 * WHY a UI-side gate when the server already enforces `TenantAdminGuard`?
 *
 *   1. UX — hide nav links and surface an actionable banner instead of
 *      letting the operator land on a page that will 403 on first fetch.
 *   2. Cache — resolving the role once (60 s staleTime) avoids every
 *      admin page making its own probe call.
 *
 * Security: the server-side `TenantAdminGuard` is the authoritative gate.
 * This component is purely an accelerator + UX layer — we hide the link,
 * but an operator typing the URL directly still hits the server and gets
 * the same 403 + structured `tenant_role_missing` body.
 *
 * Probe endpoint: `GET /v1/admin/tenants/:id`. Any 200 means the
 * principal holds `TenantRole::Admin` on that tenant (or is running with
 * the god-token). A 403 with `error_code: "tenant_role_missing"` means
 * the operator is known but not admin — render the actionable banner
 * (NOT <NotFoundPage> per RFC-026 §Open-Q-1 resolution). Any other
 * non-2xx is a transport / server error — render a generic error branch
 * so a momentary 5xx doesn't look like an access-control problem.
 */

import { useQuery } from '@tanstack/react-query';
import { ShieldAlert, AlertTriangle, Loader2 } from 'lucide-react';

import { defaultApi, ApiError } from '../lib/api';
import { useScope } from '../hooks/useScope';

// ── Cache / probe helpers ─────────────────────────────────────────────────────

/** Short enough that revocations propagate within a minute,
 *  long enough that routing between admin pages is instant. */
const ADMIN_PROBE_STALE_MS = 60_000;

/** Shared query key so the component and the hook hit the same cache entry. */
function adminProbeKey(tenantId: string): readonly unknown[] {
  return ['admin-gate', tenantId] as const;
}

// ── Hook ──────────────────────────────────────────────────────────────────────

export interface TenantAdminState {
  isLoading: boolean;
  isAdmin:   boolean;
  /** Structured error code from the probe (if any). The only value we
   *  care about today is `"tenant_role_missing"`; everything else falls
   *  through to `isAdmin: false` without a specific remediation. */
  errorCode?: string;
  /** Human-readable message from the probe's ApiError (the structured
   *  `hint` field on `tenant_role_missing`, or the generic server
   *  message otherwise). Banners surface this verbatim so the operator
   *  sees the backend's exact remediation guidance. */
  errorMessage?: string;
}

/**
 * Probe whether the current principal is `TenantRole::Admin` on the
 * given tenant. Defaults to the active scope's `tenant_id` — most
 * callers (the <Sidebar> link, <AdminGate> wrapper) want "am I admin of
 * the tenant I'm currently viewing?".
 *
 * Safe to call without a tenantId: returns `{ isLoading: false,
 * isAdmin: false }` until the scope resolves.
 */
// Co-located with <AdminGate> on purpose — the hook and the component
// share the cache key and the tiny `TenantAdminState` shape; splitting
// them across files would require three imports at every use site for
// zero readability benefit. Matches the pattern in Toast.tsx.
// eslint-disable-next-line react-refresh/only-export-components
export function useIsTenantAdmin(tenantId?: string): TenantAdminState {
  const [scope] = useScope();
  const id = tenantId ?? scope.tenant_id;

  const query = useQuery({
    queryKey: adminProbeKey(id),
    queryFn:  () => defaultApi.getTenant(id),
    enabled:  Boolean(id),
    staleTime: ADMIN_PROBE_STALE_MS,
    retry:     false, // 403 is a legitimate answer; don't retry it.
  });

  if (!id) {
    return { isLoading: false, isAdmin: false };
  }

  if (query.isPending) {
    return { isLoading: true, isAdmin: false };
  }

  if (query.isSuccess) {
    return { isLoading: false, isAdmin: true };
  }

  // Failure — attach the structured error code + message when present
  // so callers can distinguish "not admin" from "probe failed" and so
  // the banner can surface the backend's actionable hint verbatim.
  const err = query.error;
  if (err instanceof ApiError) {
    return {
      isLoading: false,
      isAdmin: false,
      errorCode: err.code,
      errorMessage: err.message,
    };
  }
  return { isLoading: false, isAdmin: false };
}

// ── Gate component ────────────────────────────────────────────────────────────

interface AdminGateProps {
  /** The admin surface to render when the gate passes. */
  children: React.ReactNode;
  /** Override the probed tenant. Defaults to the active scope's tenant_id. */
  tenantId?: string;
}

/**
 * Wraps an admin surface and short-circuits rendering when the current
 * operator lacks `TenantRole::Admin`. Three render branches:
 *
 *   - pending  → spinner.
 *   - 200      → children.
 *   - 403 tenant_role_missing → actionable banner.
 *   - other    → generic error banner.
 */
export function AdminGate({ children, tenantId }: AdminGateProps) {
  const [scope] = useScope();
  const id = tenantId ?? scope.tenant_id;
  const state = useIsTenantAdmin(id);

  if (state.isLoading) {
    return (
      <div className="flex h-full w-full items-center justify-center bg-white dark:bg-zinc-950">
        <div
          role="status"
          aria-label="Checking admin access"
          className="flex items-center gap-2 text-gray-400 dark:text-zinc-600 text-[12px]"
        >
          <Loader2 size={14} className="animate-spin" />
          <span>Checking admin access…</span>
        </div>
      </div>
    );
  }

  if (state.isAdmin) {
    return <>{children}</>;
  }

  // Not admin. Distinguish "no role" from "probe failed".
  if (state.errorCode === 'tenant_role_missing') {
    return <RoleRequiredBanner tenantId={id} hint={state.errorMessage} />;
  }

  return <ProbeErrorBanner tenantId={id} message={state.errorMessage} />;
}

// ── Banners ───────────────────────────────────────────────────────────────────

function RoleRequiredBanner({ tenantId, hint }: { tenantId: string; hint?: string }) {
  return (
    <div className="flex h-full w-full items-center justify-center bg-white dark:bg-zinc-950 px-6">
      <div className="max-w-xl w-full rounded-xl border border-amber-500/40 bg-amber-500/5 p-6 space-y-3">
        <div className="flex items-center gap-2.5">
          <ShieldAlert size={18} className="text-amber-500 shrink-0" />
          <h2 className="text-[14px] font-semibold text-gray-900 dark:text-zinc-100">
            Tenant-admin role required
          </h2>
        </div>
        <p className="text-[12px] text-gray-600 dark:text-zinc-400 leading-relaxed">
          You need the{' '}
          <code className="font-mono text-amber-600 dark:text-amber-400">TenantRole::Admin</code>{' '}
          on tenant{' '}
          <code className="font-mono text-gray-700 dark:text-zinc-300">{tenantId}</code>{' '}
          to view or edit this section. Contact your deployment admin to
          have the role granted.
        </p>
        {hint && (
          // Verbatim backend hint. Kept as <pre> so the exact command
          // round-trips for a copy/paste remediation.
          <pre className="text-[11px] font-mono text-gray-700 dark:text-zinc-300 bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded px-3 py-2 overflow-x-auto whitespace-pre-wrap">
            {hint}
          </pre>
        )}
        <p className="text-[11px] text-gray-400 dark:text-zinc-500">
          The admin API is also enforced server-side — this message is
          just a shortcut so you don't have to click through every admin
          page to discover the 403.
        </p>
      </div>
    </div>
  );
}

function ProbeErrorBanner({ tenantId, message }: { tenantId: string; message?: string }) {
  return (
    <div className="flex h-full w-full items-center justify-center bg-white dark:bg-zinc-950 px-6">
      <div className="max-w-xl w-full rounded-xl border border-red-500/40 bg-red-500/5 p-6 space-y-3">
        <div className="flex items-center gap-2.5">
          <AlertTriangle size={18} className="text-red-500 shrink-0" />
          <h2 className="text-[14px] font-semibold text-gray-900 dark:text-zinc-100">
            Couldn't verify admin access
          </h2>
        </div>
        <p className="text-[12px] text-gray-600 dark:text-zinc-400 leading-relaxed">
          The admin-role probe for tenant{' '}
          <code className="font-mono text-gray-700 dark:text-zinc-300">{tenantId}</code>{' '}
          failed. This is usually a transient server error; refresh to
          retry. If the problem persists check server logs for more
          detail.
        </p>
        {message && (
          <p className="text-[11px] text-red-400 font-mono break-all">{message}</p>
        )}
      </div>
    </div>
  );
}
