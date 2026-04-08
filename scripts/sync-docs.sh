#!/usr/bin/env bash
#
# Local doc sync: wren-engine/docs → doc website via PR
# Mirrors the GitHub Action but runs from your machine using `gh` CLI.
#
# Requires DOCS_REPO to be set as a GitHub repository variable, or
# passed via environment: DOCS_REPO=owner/repo DOCS_REPO_BRANCH=master
#
# Usage:
#   ./scripts/sync-docs.sh            # dry-run (show diff, no PR)
#   ./scripts/sync-docs.sh --apply    # create branch + PR
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENGINE_REPO="$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || echo "Canner/wren-engine")"

# Read from GitHub repo variables, allow env override
TARGET_REPO="${DOCS_REPO:-$(gh variable get DOCS_REPO -R "$ENGINE_REPO" 2>/dev/null || true)}"
TARGET_BRANCH="${DOCS_REPO_BRANCH:-$(gh variable get DOCS_REPO_BRANCH -R "$ENGINE_REPO" 2>/dev/null || echo "master")}"

if [[ -z "$TARGET_REPO" ]]; then
  echo "error: DOCS_REPO not set. Either:" >&2
  echo "  1. Set GitHub repo variable: gh variable set DOCS_REPO -R $ENGINE_REPO --body 'owner/repo'" >&2
  echo "  2. Pass via env: DOCS_REPO=owner/repo $0" >&2
  exit 1
fi

TARGET_DIR="docs/oss/engine"
SYNC_DIRS=(get_started concept guide reference)
SHORT_SHA="$(git -C "$REPO_ROOT" rev-parse --short=8 HEAD)"

# --- preflight ---
if ! command -v gh &>/dev/null; then
  echo "error: gh CLI not found — install from https://cli.github.com" >&2
  exit 1
fi
if ! gh auth status &>/dev/null 2>&1; then
  echo "error: not authenticated — run 'gh auth login' first" >&2
  exit 1
fi

# --- clone target into a temp dir ---
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

echo "Syncing docs → ${TARGET_REPO} (${TARGET_BRANCH})..."
gh repo clone "$TARGET_REPO" "$TMPDIR/docs-site" -- --branch "$TARGET_BRANCH" --single-branch --depth 1 -q

TARGET="$TMPDIR/docs-site/${TARGET_DIR}"

# --- sync ---
for dir in "${SYNC_DIRS[@]}"; do
  rm -rf "${TARGET}/${dir}"
  cp -r "${REPO_ROOT}/docs/${dir}" "${TARGET}/${dir}"
done

# --- diff ---
cd "$TMPDIR/docs-site"
if git diff --quiet; then
  echo "No changes — docs are already in sync."
  exit 0
fi

echo ""
echo "=== Changes ==="
git diff --stat
echo ""

if [[ "${1:-}" != "--apply" ]]; then
  echo "(dry-run) Re-run with --apply to create a PR."
  exit 0
fi

# --- create PR ---
BRANCH="sync/engine-docs-${SHORT_SHA}"
git checkout -b "$BRANCH"
git add -A
git commit -m "docs: sync from wren-engine@${SHORT_SHA}"
git push origin "$BRANCH"

PR_URL=$(gh pr create \
  --title "docs: sync Wren Engine docs from wren-engine" \
  --body "Manual sync from [wren-engine@\`${SHORT_SHA}\`](https://github.com/Canner/wren-engine/commit/${SHORT_SHA})." \
  --base "$TARGET_BRANCH")

echo ""
echo "PR created: ${PR_URL}"
