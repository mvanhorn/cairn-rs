/**
 * QuotasPage — RFC-026 PR-A5 admin surface.
 *
 * Single-tenant view keyed off the active scope (matches the Settings
 * page ergonomics rather than TenantsPage's multi-row table). Admin
 * picks a tenant from the scope switcher, this page shows that
 * tenant's quota policy + current usage against each limit.
 *
 * Scope-first UX rationale:
 *   - Admins typically act on one tenant at a time (investigate spike,
 *     raise a limit). The overview-across-all-tenants table is useful
 *     but secondary — the scope switcher already exists.
 *   - Matches how Operators page works (PR-A4): both surfaces pivot on
 *     `useScope().tenant_id`.
 *   - Cheaper: one quota + one overview query, not N+1 across the
 *     tenant list.
 *
 * Backend contract (all live as of PR-A1):
 *   GET  /v1/admin/tenants/:id/quota          current limits + usage
 *     → 404 when no policy has been set (empty state)
 *   POST /v1/admin/tenants/:id/quota          set / replace full policy
 *   GET  /v1/admin/tenants/:id/overview       per-workspace roll-up
 *
 * SET semantics (not PATCH): SetTenantQuotaRequest requires every
 * field on every submit. The page reads the current quota into the
 * edit form, lets the operator change any subset, then POSTs all
 * three values back. This matches `set_tenant_quota_handler` at
 * `handlers/admin.rs:579`.
 *
 * Future work (out-of-scope for PR-A5):
 *   - Dry-run preview (RFC-026 §deferred → v1.1)
 *   - Multi-tenant overview table
 *   - Hourly usage sparkline (requires a new backend endpoint)
 */

import { useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Gauge, RefreshCw, Pencil, Check, X, Plus } from 'lucide-react';
import { clsx } from 'clsx';

import { ApiError, defaultApi } from '../lib/api';
import type { TenantQuota, SetTenantQuotaRequest } from '../lib/types';
import { errorMessage } from '../lib/errors';
import { useScope } from '../hooks/useScope';
import { EntityExplainer } from '../components/EntityExplainer';
import { ErrorFallback } from '../components/ErrorFallback';

// ── Validation ────────────────────────────────────────────────────────────────

/** Backend requires positive non-zero integers on every limit — zero
 *  would mean "no runs allowed" which is indistinguishable from an
 *  unset policy and would wedge the tenant. Reject it client-side so
 *  operators see the actionable error inside the dialog, not as a
 *  generic 422 after round-trip. */
function validateLimit(label: string, value: number): string | null {
  if (!Number.isFinite(value) || Number.isNaN(value)) return `${label} must be a number.`;
  if (!Number.isInteger(value)) return `${label} must be a whole number.`;
  if (value < 1) return `${label} must be at least 1.`;
  if (value > 1_000_000) return `${label} is unreasonably large (max 1,000,000).`;
  return null;
}

// ── Usage helpers ────────────────────────────────────────────────────────────

/** Clamp to [0, 100] and round to an integer percentage. We clamp
 *  because the live-usage counter can briefly exceed the limit during
 *  a quota edit (operator lowers the cap while N runs are live). */
function pct(current: number, limit: number): number {
  if (!limit || limit <= 0) return 0;
  const raw = (current / limit) * 100;
  if (!Number.isFinite(raw)) return 0;
  return Math.max(0, Math.min(100, Math.round(raw)));
}

/** Colour the bar by utilization — the same green/amber/red thresholds
 *  the dashboard uses for cost widgets, so the visual grammar is
 *  consistent across the admin surface. */
function barColor(percent: number): string {
  if (percent >= 90) return 'bg-red-500';
  if (percent >= 75) return 'bg-amber-500';
  return 'bg-emerald-500';
}

// ── Usage bar ────────────────────────────────────────────────────────────────

interface UsageBarProps {
  label:   string;
  current: number;
  limit:   number;
  unit?:   string;
}

function UsageBar({ label, current, limit, unit }: UsageBarProps) {
  const percent = pct(current, limit);
  return (
    <div>
      <div className="flex items-baseline justify-between mb-1">
        <span className="text-[11px] text-gray-500 dark:text-zinc-400">{label}</span>
        <span className="text-[11px] tabular-nums text-gray-700 dark:text-zinc-300">
          <span className="font-medium">{current.toLocaleString()}</span>
          <span className="text-gray-300 dark:text-zinc-600"> / </span>
          <span>{limit.toLocaleString()}</span>
          {unit && <span className="text-gray-400 dark:text-zinc-500"> {unit}</span>}
          <span className="ml-2 text-gray-400 dark:text-zinc-500">{percent}%</span>
        </span>
      </div>
      <div
        role="progressbar"
        aria-label={`${label} utilization`}
        aria-valuenow={percent}
        aria-valuemin={0}
        aria-valuemax={100}
        className="relative h-1.5 rounded bg-gray-100 dark:bg-zinc-900 overflow-hidden"
      >
        <div
          className={clsx('absolute left-0 top-0 h-full transition-all', barColor(percent))}
          style={{ width: `${percent}%` }}
        />
      </div>
    </div>
  );
}

// ── Quota form (shared by Create + Edit) ─────────────────────────────────────

interface QuotaFormProps {
  initial:    SetTenantQuotaRequest;
  submitLabel: string;
  onSubmit:    (body: SetTenantQuotaRequest) => Promise<void>;
  onCancel:    () => void;
  submitting:  boolean;
}

function QuotaForm({ initial, submitLabel, onSubmit, onCancel, submitting }: QuotaFormProps) {
  // Track as string so the user can clear the field without the input
  // snapping to 0. We parse on submit.
  const [runs,     setRuns]     = useState(String(initial.max_concurrent_runs));
  const [sessions, setSessions] = useState(String(initial.max_sessions_per_hour));
  const [tasks,    setTasks]    = useState(String(initial.max_tasks_per_run));
  const [err,      setErr]      = useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const runsN     = Number(runs);
    const sessionsN = Number(sessions);
    const tasksN    = Number(tasks);

    const runsErr     = validateLimit('Concurrent runs',   runsN);
    const sessionsErr = validateLimit('Sessions per hour', sessionsN);
    const tasksErr    = validateLimit('Tasks per run',     tasksN);
    if (runsErr)     { setErr(runsErr);     return; }
    if (sessionsErr) { setErr(sessionsErr); return; }
    if (tasksErr)    { setErr(tasksErr);    return; }

    try {
      await onSubmit({
        max_concurrent_runs:   runsN,
        max_sessions_per_hour: sessionsN,
        max_tasks_per_run:     tasksN,
      });
    } catch (e2) {
      setErr(errorMessage(e2, 'Failed to save quota.'));
    }
  }

  const fieldCls =
    'w-full rounded border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-950 ' +
    'font-mono text-[13px] text-gray-800 dark:text-zinc-200 px-3 py-2 ' +
    'focus:outline-none focus:border-indigo-500 transition-colors';

  return (
    <form onSubmit={handleSubmit} className="space-y-4" aria-label="Quota form">
      <div>
        <label htmlFor="quota-runs" className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1.5">
          Max concurrent runs <span className="text-red-500">*</span>
        </label>
        <input
          id="quota-runs"
          name="max_concurrent_runs"
          autoFocus
          type="number"
          min={1}
          value={runs}
          onChange={e => { setRuns(e.target.value); setErr(null); }}
          className={fieldCls}
        />
        <p className="text-[10px] text-gray-300 dark:text-zinc-600 mt-1">
          Upper bound on simultaneously-running runs across the tenant.
        </p>
      </div>
      <div>
        <label htmlFor="quota-sessions" className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1.5">
          Max sessions per hour <span className="text-red-500">*</span>
        </label>
        <input
          id="quota-sessions"
          name="max_sessions_per_hour"
          type="number"
          min={1}
          value={sessions}
          onChange={e => { setSessions(e.target.value); setErr(null); }}
          className={fieldCls}
        />
        <p className="text-[10px] text-gray-300 dark:text-zinc-600 mt-1">
          Rolling-window session cap. Resets hourly.
        </p>
      </div>
      <div>
        <label htmlFor="quota-tasks" className="block text-[11px] text-gray-400 dark:text-zinc-500 mb-1.5">
          Max tasks per run <span className="text-red-500">*</span>
        </label>
        <input
          id="quota-tasks"
          name="max_tasks_per_run"
          type="number"
          min={1}
          value={tasks}
          onChange={e => { setTasks(e.target.value); setErr(null); }}
          className={fieldCls}
        />
        <p className="text-[10px] text-gray-300 dark:text-zinc-600 mt-1">
          Per-run task fan-out cap. Guards against runaway planners.
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
          <Check size={11} /> {submitting ? 'Saving…' : submitLabel}
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

// ── Page ─────────────────────────────────────────────────────────────────────

/** Default limits seeded into the Create-quota modal when no policy
 *  exists yet. Chosen conservatively — ops can always raise. Matches
 *  the `Default` impl on `cairn_domain::quotas::TenantQuota` (100/100/50). */
const DEFAULT_LIMITS: SetTenantQuotaRequest = {
  max_concurrent_runs:   10,
  max_sessions_per_hour: 100,
  max_tasks_per_run:     50,
};

export function QuotasPage() {
  const qc = useQueryClient();
  const [scope] = useScope();
  const tenantId = scope.tenant_id;

  const [mode, setMode] = useState<'idle' | 'create' | 'edit'>('idle');

  // Quota query — a 404 means "no policy set", which we render as an
  // empty state rather than an error. Any other failure surfaces the
  // shared ErrorFallback.
  const quotaQuery = useQuery<TenantQuota | null, Error>({
    queryKey: ['tenant-quota', tenantId],
    queryFn: async () => {
      try {
        return await defaultApi.getTenantQuota(tenantId);
      } catch (e) {
        if (e instanceof ApiError && e.status === 404) return null;
        throw e;
      }
    },
    staleTime: 10_000,
    enabled: Boolean(tenantId),
  });

  // Overview is informational only — we show workspace/member/active-run
  // counts alongside the quota so the operator has context. Failures
  // here don't block the page; the header counters fall back to "—".
  const overviewQuery = useQuery({
    queryKey: ['tenant-overview', tenantId],
    queryFn: () => defaultApi.getTenantOverview(tenantId),
    staleTime: 30_000,
    enabled: Boolean(tenantId),
    retry: false,
  });

  const setMutation = useMutation({
    mutationFn: (body: SetTenantQuotaRequest) => defaultApi.setTenantQuota(tenantId, body),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['tenant-quota', tenantId] });
    },
  });

  async function handleSubmit(body: SetTenantQuotaRequest) {
    await setMutation.mutateAsync(body);
    setMode('idle');
  }

  // Memoised initial values for the edit form — re-read whenever the
  // underlying quota changes so the dialog doesn't ghost stale values
  // after a concurrent update.
  const editInitial = useMemo<SetTenantQuotaRequest>(() => {
    const q = quotaQuery.data;
    if (!q) return DEFAULT_LIMITS;
    return {
      max_concurrent_runs:   q.max_concurrent_runs,
      max_sessions_per_hour: q.max_sessions_per_hour,
      max_tasks_per_run:     q.max_tasks_per_run,
    };
  }, [quotaQuery.data]);

  // Render ---------------------------------------------------------------------

  // Non-404 errors → shared error fallback. 404 is handled below as the
  // empty state because the quoteQuery.queryFn normalises it to `null`.
  if (quotaQuery.isError) {
    return (
      <ErrorFallback
        error={quotaQuery.error}
        resource="tenant quota"
        onRetry={() => void quotaQuery.refetch()}
      />
    );
  }

  const quota = quotaQuery.data;
  const overview = overviewQuery.data;

  return (
    <div className="flex flex-col h-full bg-white dark:bg-zinc-950 overflow-y-auto">
      <div className="max-w-3xl mx-auto px-5 py-5 space-y-5 w-full">

        {/* Header */}
        <div className="flex items-start justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-lg bg-indigo-500/10 flex items-center justify-center shrink-0">
              <Gauge size={16} className="text-indigo-400" />
            </div>
            <div>
              <h1 className="text-[15px] font-semibold text-gray-900 dark:text-zinc-100">Quotas</h1>
              <p className="text-[11px] text-gray-400 dark:text-zinc-600 mt-0.5">
                Admin-only. Per-tenant concurrency &amp; rate limits for{' '}
                <code className="font-mono">{tenantId}</code>.
              </p>
              <EntityExplainer className="mt-1">
                Quotas protect the control plane from runaway tenants. Limits apply
                in aggregate across every workspace inside the tenant. Violations
                surface as TenantQuotaViolated events.
              </EntityExplainer>
            </div>
          </div>

          <div className="flex items-center gap-2 shrink-0">
            <button
              type="button"
              onClick={() => {
                void quotaQuery.refetch();
                void overviewQuery.refetch();
              }}
              disabled={quotaQuery.isFetching}
              aria-label="Refresh quota"
              className="p-1.5 rounded text-gray-400 dark:text-zinc-500 hover:text-gray-700 dark:hover:text-zinc-300 hover:bg-gray-100 dark:hover:bg-zinc-800 transition-colors disabled:opacity-40"
              title="Refresh"
            >
              <RefreshCw size={14} className={quotaQuery.isFetching ? 'animate-spin' : ''} />
            </button>
            {quota && (
              <button
                type="button"
                onClick={() => setMode('edit')}
                aria-label="Edit quota"
                className="flex items-center gap-1.5 rounded px-3 py-1.5 text-[12px] font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors"
              >
                <Pencil size={12} /> Edit quota
              </button>
            )}
          </div>
        </div>

        {/* Tenant context strip — lightweight reminder of which tenant
            the admin is editing. Hidden when overview hasn't resolved;
            re-showing after the first render avoids a layout jump. */}
        {overview && (
          <div className="grid grid-cols-3 gap-3">
            <div className="rounded-lg border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-900/40 px-3 py-2">
              <div className="text-[10px] uppercase tracking-wider text-gray-400 dark:text-zinc-500">Workspaces</div>
              <div className="text-[16px] font-semibold text-gray-800 dark:text-zinc-200 tabular-nums">
                {overview.workspace_count}
              </div>
            </div>
            <div className="rounded-lg border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-900/40 px-3 py-2">
              <div className="text-[10px] uppercase tracking-wider text-gray-400 dark:text-zinc-500">Members</div>
              <div className="text-[16px] font-semibold text-gray-800 dark:text-zinc-200 tabular-nums">
                {overview.total_members}
              </div>
            </div>
            <div className="rounded-lg border border-gray-200 dark:border-zinc-800 bg-gray-50 dark:bg-zinc-900/40 px-3 py-2">
              <div className="text-[10px] uppercase tracking-wider text-gray-400 dark:text-zinc-500">Active runs</div>
              <div className="text-[16px] font-semibold text-gray-800 dark:text-zinc-200 tabular-nums">
                {overview.active_runs}
              </div>
            </div>
          </div>
        )}

        {/* Body */}
        {quotaQuery.isLoading ? (
          <div className="space-y-3">
            {[1, 2, 3].map(i => (
              <div key={i} className="h-12 rounded bg-gray-100 dark:bg-zinc-900 animate-pulse" />
            ))}
          </div>
        ) : !quota ? (
          // Empty state — no quota policy exists for this tenant.
          <div className="flex flex-col items-center justify-center py-16 gap-3 text-center rounded-lg border border-dashed border-gray-200 dark:border-zinc-800">
            <Gauge size={28} className="text-gray-300 dark:text-zinc-700" />
            <p className="text-[13px] text-gray-500 dark:text-zinc-400">
              No quota policy for this tenant.
            </p>
            <p className="text-[11px] text-gray-400 dark:text-zinc-600 max-w-sm">
              The tenant runs without enforced limits until a policy is set.
              Create one to cap concurrent runs, sessions, and per-run tasks.
            </p>
            <button
              type="button"
              onClick={() => setMode('create')}
              className="mt-1 flex items-center gap-1.5 rounded px-3 py-1.5 text-[12px] font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors"
            >
              <Plus size={12} /> Create quota
            </button>
          </div>
        ) : (
          // Quota exists — render limits + usage bars.
          <div className="rounded-lg border border-gray-200 dark:border-zinc-800 p-4 space-y-4">
            <UsageBar
              label="Concurrent runs"
              current={quota.current_active_runs}
              limit={quota.max_concurrent_runs}
            />
            <UsageBar
              label="Sessions per hour"
              current={quota.sessions_this_hour}
              limit={quota.max_sessions_per_hour}
            />
            <div>
              <div className="flex items-baseline justify-between mb-1">
                <span className="text-[11px] text-gray-500 dark:text-zinc-400">Tasks per run</span>
                <span className="text-[11px] tabular-nums text-gray-700 dark:text-zinc-300">
                  <span className="font-medium">{quota.max_tasks_per_run.toLocaleString()}</span>
                  <span className="text-gray-400 dark:text-zinc-500"> max</span>
                </span>
              </div>
              <p className="text-[10px] text-gray-300 dark:text-zinc-600">
                Enforced per-run at task-create time. No live-usage counter — each run counts independently.
              </p>
            </div>
          </div>
        )}
      </div>

      {/* Create modal */}
      {mode === 'create' && (
        <Modal title="Create quota" onClose={() => setMode('idle')}>
          <QuotaForm
            initial={DEFAULT_LIMITS}
            submitLabel="Create"
            onSubmit={handleSubmit}
            onCancel={() => setMode('idle')}
            submitting={setMutation.isPending}
          />
        </Modal>
      )}

      {/* Edit modal */}
      {mode === 'edit' && (
        <Modal title="Edit quota" onClose={() => setMode('idle')}>
          <QuotaForm
            initial={editInitial}
            submitLabel="Save"
            onSubmit={handleSubmit}
            onCancel={() => setMode('idle')}
            submitting={setMutation.isPending}
          />
        </Modal>
      )}
    </div>
  );
}

export default QuotasPage;
