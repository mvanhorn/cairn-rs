/**
 * apiError.test — ensure `apiFetch` (and the exported `defaultApi` methods
 * that sit on top of it) surfaces the RFC-026 structured error envelope
 * correctly. Backend errors use TWO shapes:
 *
 *   1. `{ code: "...", message: "..." }`          — unified envelope.
 *   2. `{ error_code: "tenant_role_missing",       — RFC-026 PR-A0
 *        tenant_id, operator_id, hint }`            structured 403.
 *
 * The wrapper must read *either* `code` or `error_code` into
 * `ApiError.code`, and it must preserve the `hint` (when present) as the
 * user-facing `ApiError.message` for the `tenant_role_missing` case so
 * the `<AdminGate>` banner has something actionable to render.
 *
 * Regression test for PR-A3 — the TenantsPage depends on this being
 * correct to distinguish "no admin role" (render gate) from "generic
 * 403" (render ErrorFallback).
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { ApiError, createApiClient } from '../api';

const originalFetch = globalThis.fetch;

function mockJsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

describe('apiFetch error envelope parsing', () => {
  beforeEach(() => {
    globalThis.fetch = vi.fn();
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it('parses the unified { code, message } envelope', async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce(
      mockJsonResponse(400, { code: 'validation_error', message: 'bad id' }),
    );
    const api = createApiClient({ baseUrl: 'http://t', token: 'x' });
    await expect(api.getTenant('bad')).rejects.toSatisfy((e) => {
      if (!(e instanceof ApiError)) return false;
      return e.status === 400 && e.code === 'validation_error' && e.message === 'bad id';
    });
  });

  it('parses the RFC-026 structured { error_code, hint } envelope', async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce(
      mockJsonResponse(403, {
        error_code: 'tenant_role_missing',
        tenant_id: 't1',
        operator_id: 'op-1',
        hint: 'Ask your deployment admin to run `cairn-app admin promote op-1`',
      }),
    );
    const api = createApiClient({ baseUrl: 'http://t', token: 'x' });
    await expect(api.getTenant('t1')).rejects.toSatisfy((e) => {
      if (!(e instanceof ApiError)) return false;
      return (
        e.status === 403 &&
        e.code === 'tenant_role_missing' &&
        // Hint is surfaced as the error message so toasts / banners have
        // something actionable instead of "HTTP 403".
        e.message.includes('promote op-1')
      );
    });
  });

  it('falls back to defaults when the response body is not JSON', async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce(
      new Response('not json', { status: 500 }),
    );
    const api = createApiClient({ baseUrl: 'http://t', token: 'x' });
    await expect(api.getTenant('t1')).rejects.toSatisfy((e) => {
      if (!(e instanceof ApiError)) return false;
      return e.status === 500 && e.code === 'unknown_error' && e.message === 'HTTP 500';
    });
  });
});
