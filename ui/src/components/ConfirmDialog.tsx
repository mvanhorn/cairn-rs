/**
 * ConfirmDialog — styled confirm prompt for destructive operator actions.
 *
 * Issue #393: RunDetailPage's Recover / Claim / Cancel Run flows used
 * `window.confirm`, which blocks the main thread, ignores the dark theme,
 * and is inconsistent with adjacent Drawer-based flows (Intervene, Spawn
 * subagent). This component provides the same guardrail (explicit confirm)
 * with the project's design-system look:
 *   - role="dialog", aria-modal="true"
 *   - focus trap + Escape-to-close via the shared `useFocusTrap` hook
 *   - red primary button for destructive variant, indigo for default
 *   - `data-testid="confirm-dialog"` so Playwright specs can assert the
 *     Drawer appears (replacing `page.on('dialog', …)` window.confirm handlers)
 *
 * Usage:
 *   const [open, setOpen] = useState(false);
 *   ...
 *   <ConfirmDialog
 *     open={open}
 *     title="Cancel run?"
 *     message="This terminates the run immediately. This cannot be undone."
 *     confirmLabel="Cancel run"
 *     variant="danger"
 *     onConfirm={() => { setOpen(false); doCancel(); }}
 *     onCancel={() => setOpen(false)}
 *   />
 */

import { AlertTriangle, Loader2 } from 'lucide-react';
import { clsx } from 'clsx';
import { useFocusTrap } from '../hooks/useFocusTrap';
import { ds } from '../lib/design-system';

export interface ConfirmDialogProps {
  /** Whether the dialog is visible. Nothing renders when false. */
  open: boolean;
  /** Short imperative title, e.g. "Cancel run?". */
  title: string;
  /** Body copy — one or two sentences, operator-facing. */
  message: React.ReactNode;
  /** Label on the primary action button (default: "Confirm"). */
  confirmLabel?: string;
  /** Label on the secondary / cancel button (default: "Cancel"). */
  cancelLabel?: string;
  /** Visual variant — `danger` uses a red primary button (destructive),
   *  `default` uses the indigo primary button. */
  variant?: 'danger' | 'default';
  /** Called when the operator confirms. The parent is responsible for
   *  closing the dialog if the underlying action succeeds. */
  onConfirm: () => void;
  /** Called when the operator dismisses (X, backdrop, Escape, Cancel). */
  onCancel: () => void;
  /** Disables the confirm button and shows a spinner — use while the
   *  underlying mutation is in flight. */
  isPending?: boolean;
  /** Optional test id on the dialog container — defaults to
   *  `confirm-dialog` so Playwright specs can select it without needing
   *  a custom id per call site. Parents can override to distinguish
   *  multiple dialogs on one page. */
  testId?: string;
}

export function ConfirmDialog({
  open,
  title,
  message,
  confirmLabel = 'Confirm',
  cancelLabel = 'Cancel',
  variant = 'default',
  onConfirm,
  onCancel,
  isPending = false,
  testId = 'confirm-dialog',
}: ConfirmDialogProps) {
  const trapRef = useFocusTrap({ onClose: onCancel });
  if (!open) return null;

  const confirmClass = variant === 'danger'
    ? 'bg-red-600 hover:bg-red-500'
    : 'bg-indigo-600 hover:bg-indigo-500';
  const iconBg = variant === 'danger'
    ? 'bg-red-500/10 border-red-500/20'
    : 'bg-indigo-500/10 border-indigo-500/20';
  const iconClass = variant === 'danger' ? 'text-red-400' : 'text-indigo-400';

  return (
    <div className={ds.modal.backdrop} onClick={onCancel}>
      <div
        data-testid={testId}
        className={clsx(ds.modal.container, 'w-full max-w-md mx-4 shadow-2xl')}
        ref={trapRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={`${testId}-title`}
        onClick={e => e.stopPropagation()}
      >
        <div className="flex items-start gap-3 p-5">
          <div className={clsx('flex h-8 w-8 shrink-0 items-center justify-center rounded-full border', iconBg)}>
            <AlertTriangle size={14} className={iconClass} />
          </div>
          <div className="flex-1 min-w-0">
            <p id={`${testId}-title`} className="text-[13px] font-semibold text-gray-900 dark:text-zinc-100">
              {title}
            </p>
            <div className="text-[12px] text-gray-500 dark:text-zinc-400 mt-1">
              {message}
            </div>
          </div>
        </div>

        <div className="flex justify-end gap-2 px-5 pb-4">
          <button
            data-testid={`${testId}-cancel-btn`}
            onClick={onCancel}
            disabled={isPending}
            className="px-3 py-1.5 rounded bg-gray-100 dark:bg-zinc-800 text-gray-500 dark:text-zinc-400 text-[12px] hover:bg-gray-200 dark:hover:bg-zinc-700 transition-colors disabled:opacity-50"
          >
            {cancelLabel}
          </button>
          <button
            data-testid={`${testId}-confirm-btn`}
            data-pending={isPending ? 'true' : 'false'}
            onClick={onConfirm}
            disabled={isPending}
            className={clsx(
              'px-3 py-1.5 rounded text-white text-[12px] transition-colors flex items-center gap-1.5 disabled:opacity-50',
              confirmClass,
            )}
          >
            {isPending && <Loader2 size={11} className="animate-spin" />}
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}

export default ConfirmDialog;
