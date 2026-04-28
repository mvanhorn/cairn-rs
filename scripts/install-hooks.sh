#!/usr/bin/env bash
# Install git hooks for cairn-rs development.
# Run once after cloning: ./scripts/install-hooks.sh
#
# Points git at `.githooks/` via `core.hooksPath` (repo-scoped config),
# which activates every hook script in that directory without needing to
# copy files into `.git/hooks/` on every checkout. Unlike the previous
# file-copy flow, hooks now update with `git pull` — nothing to reinstall.
#
# Active hooks:
#   .githooks/pre-commit  — fmt, clippy, and the RFC-025 log_stub guard
#                           (new log_stub() sites in pg/sqlite projections
#                           are rejected; see
#                           docs/design/rfcs/RFC-025-runtime-aggregate-backend-abstraction.md).
#   .githooks/pre-push    — workspace tests + fabric integration gate.

set -euo pipefail

# Guard against partial checkouts / bare repos.
if ! repo_root="$(git rev-parse --show-toplevel 2>/dev/null)"; then
    echo "✗ Not inside a git repo. Run from a cairn-rs checkout." >&2
    exit 1
fi
cd "$repo_root"

if [ ! -d .githooks ]; then
    echo "✗ .githooks/ directory not found in $repo_root." >&2
    echo "  Check that your working tree is complete." >&2
    exit 1
fi

# Verify the hook files we document are actually present and refuse to
# activate hooksPath against a partial tree — a silent success here
# would leave contributors believing the hooks are enabled while the
# commit/push gates are inert.
missing=()
for hook in pre-commit pre-push; do
    if [ ! -f ".githooks/$hook" ]; then
        missing+=(".githooks/$hook")
    fi
done
if [ ${#missing[@]} -gt 0 ]; then
    echo "✗ Missing hook file(s): ${missing[*]}" >&2
    echo "  Run from a complete cairn-rs checkout." >&2
    exit 1
fi

# Mark every hook executable; if `chmod` can't (e.g. filesystem doesn't
# support the mode bit or we lack permissions) surface the failure
# instead of silently activating hooks git will then refuse to run.
if ! chmod +x .githooks/pre-commit .githooks/pre-push; then
    echo "✗ chmod failed on .githooks/pre-commit or .githooks/pre-push" >&2
    echo "  Hooks activated via core.hooksPath must be executable; fix the" >&2
    echo "  filesystem permissions and rerun this script." >&2
    exit 1
fi

git config core.hooksPath .githooks

echo "✓ Set core.hooksPath = .githooks"
echo "  pre-commit: fmt, clippy, RFC-025 log_stub guard"
echo "  pre-push:   cargo tests, fabric integration (docker), UI build + vitest"
echo ""
echo "  Bypass (use sparingly): git commit --no-verify / git push --no-verify"
