/**
 * RFC 031 PR-D2 §Draft persistence — localStorage-backed editor draft.
 *
 * Key shape (per RFC):
 *   edit of existing role:
 *     `cairn:agent_role_draft:{tenant}:{workspace}:{project}:edit:{role_id}`
 *   new role:
 *     `cairn:agent_role_draft:{tenant}:{workspace}:{project}:new:{tab_uuid}`
 *
 * The `tab_uuid` is generated on mount and held in `sessionStorage`
 * so two concurrent "new role" tabs in the same project get distinct
 * keys; the uuid dies with the tab.
 *
 * Writes are debounced (250 ms). Reads expose a `loadExisting`
 * helper — the editor calls it once on mount to decide whether to
 * surface the "Restore draft from HH:MM" banner.
 */

import { useCallback, useMemo, useRef } from "react";

export interface DraftEnvelope<T> {
  saved_at: number;
  data: T;
}

const PREFIX = "cairn:agent_role_draft";
const SESSION_UUID_KEY = "cairn:agent_role_draft:tab_uuid";

function currentTabUuid(): string {
  try {
    const existing = sessionStorage.getItem(SESSION_UUID_KEY);
    if (existing) return existing;
    const fresh = crypto.randomUUID();
    sessionStorage.setItem(SESSION_UUID_KEY, fresh);
    return fresh;
  } catch {
    // sessionStorage disabled → fallback to a random id that lives
    // only in this hook instance. Cross-tab collision is theoretical
    // without storage, so a ~random suffix is enough.
    return `nolocal_${Math.random().toString(36).slice(2)}`;
  }
}

interface Scope {
  tenant: string;
  workspace: string;
  project: string;
}

interface Opts {
  scope: Scope;
  mode: "new" | "edit";
  roleId?: string;
}

export interface AgentRoleDraftApi<T> {
  key: string;
  write: (value: T) => void;
  loadExisting: () => DraftEnvelope<T> | null;
  clear: () => void;
}

export function useAgentRoleDraft<T>({
  scope,
  mode,
  roleId,
}: Opts): AgentRoleDraftApi<T> {
  // `useMemo` on the tab-uuid so the same hook instance uses the
  // same suffix for the lifetime of the component (a remount gets
  // whatever is in sessionStorage, which is also the same).
  const tabUuid = useMemo(() => {
    if (mode === "new") return currentTabUuid();
    return ""; // unused for edit keys
  }, [mode]);

  const key = useMemo(() => {
    const base = `${PREFIX}:${scope.tenant}:${scope.workspace}:${scope.project}`;
    if (mode === "edit") {
      return `${base}:edit:${roleId ?? ""}`;
    }
    return `${base}:new:${tabUuid}`;
  }, [scope.tenant, scope.workspace, scope.project, mode, roleId, tabUuid]);

  const timer = useRef<number | null>(null);

  const write = useCallback(
    (value: T) => {
      if (timer.current !== null) {
        clearTimeout(timer.current);
      }
      timer.current = window.setTimeout(() => {
        try {
          const envelope: DraftEnvelope<T> = {
            saved_at: Date.now(),
            data: value,
          };
          localStorage.setItem(key, JSON.stringify(envelope));
        } catch {
          // localStorage full / disabled — draft persistence is a
          // best-effort UX, not a correctness contract.
        }
      }, 250);
    },
    [key],
  );

  const loadExisting = useCallback((): DraftEnvelope<T> | null => {
    try {
      const raw = localStorage.getItem(key);
      if (!raw) return null;
      const parsed = JSON.parse(raw) as DraftEnvelope<T>;
      if (typeof parsed?.saved_at !== "number" || !("data" in parsed)) {
        return null;
      }
      return parsed;
    } catch {
      return null;
    }
  }, [key]);

  const clear = useCallback(() => {
    if (timer.current !== null) {
      clearTimeout(timer.current);
      timer.current = null;
    }
    try {
      localStorage.removeItem(key);
    } catch {
      /* no-op */
    }
  }, [key]);

  return { key, write, loadExisting, clear };
}
