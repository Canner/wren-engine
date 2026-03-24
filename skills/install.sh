#!/usr/bin/env bash
# Install Wren Engine skills into your local AI agent skills directory.
#
# Usage:
#   ./install.sh                     # install all skills
#   ./install.sh wren-generate-mdl    # install specific skills
#   ./install.sh --force wren-sql    # overwrite without prompt
#   curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash
#   curl -fsSL .../install.sh | bash -s -- wren-generate-mdl

set -euo pipefail

REPO="Canner/wren-engine"
BRANCH="${WREN_SKILLS_BRANCH:-main}"
DEST="${CLAUDE_SKILLS_DIR:-$HOME/.claude/skills}"
ALL_SKILLS=(wren-generate-mdl wren-project wren-sql wren-mcp-setup wren-quickstart wren-connection-info wren-usage wren-http-api)

# Parse --force flag and skill list from arguments
FORCE=false
SELECTED_SKILLS=()
for arg in "$@"; do
  if [ "$arg" = "--force" ]; then
    FORCE=true
  else
    SELECTED_SKILLS+=("$arg")
  fi
done

if [ "${#SELECTED_SKILLS[@]}" -eq 0 ]; then
  SELECTED_SKILLS=("${ALL_SKILLS[@]}")
fi

# Validate requested skills
for skill in "${SELECTED_SKILLS[@]}"; do
  valid=false
  for known in "${ALL_SKILLS[@]}"; do
    if [ "$skill" = "$known" ]; then valid=true; break; fi
  done
  if [ "$valid" = false ]; then
    echo "Unknown skill: $skill" >&2
    echo "Available: ${ALL_SKILLS[*]}" >&2
    exit 1
  fi
done

# Detect whether we are running from a local clone or piped via curl.
# When piped, BASH_SOURCE[0] is empty or "/dev/stdin".
SCRIPT_DIR=""
if [ -n "${BASH_SOURCE[0]:-}" ] && [ "${BASH_SOURCE[0]}" != "/dev/stdin" ]; then
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi

# Locate index.json for dependency resolution (local or remote)
INDEX_JSON=""
if [ -n "$SCRIPT_DIR" ] && [ -f "$SCRIPT_DIR/index.json" ]; then
  INDEX_JSON="$SCRIPT_DIR/index.json"
fi

# Expand SELECTED_SKILLS to include dependencies declared in index.json.
# Only runs when python3 is available and index.json is accessible.
expand_with_deps() {
  local json_file="$1"
  shift
  local -a input=("$@")
  local -a result=()

  skill_in_result() {
    local s="$1"
    for r in "${result[@]:-}"; do [ "$r" = "$s" ] && return 0; done
    return 1
  }

  is_known_skill() {
    local s="$1"
    for known in "${ALL_SKILLS[@]}"; do [ "$s" = "$known" ] && return 0; done
    return 1
  }

  for skill in "${input[@]}"; do
    skill_in_result "$skill" || result+=("$skill")

    if [ -n "$json_file" ] && command -v python3 &>/dev/null; then
      while IFS= read -r dep; do
        [ -z "$dep" ] && continue
        is_known_skill "$dep" || continue
        if ! skill_in_result "$dep"; then
          echo "  + $dep (dependency of $skill)" >&2
          result+=("$dep")
        fi
      done < <(python3 -c "
import json, sys
try:
    d = json.load(open(sys.argv[1]))
    s = next((x for x in d.get('skills', []) if x['name'] == sys.argv[2]), None)
    if s:
        for dep in s.get('dependencies', []):
            print(dep)
except Exception:
    pass
" "$json_file" "$skill" 2>/dev/null)
    fi
  done

  printf '%s\n' "${result[@]}"
}

# Only expand deps when installing specific skills (not the full set)
if [ "${#SELECTED_SKILLS[@]}" -lt "${#ALL_SKILLS[@]}" ] && [ -n "$INDEX_JSON" ]; then
  EXPANDED=()
  while IFS= read -r line; do
    [ -n "$line" ] && EXPANDED+=("$line")
  done < <(expand_with_deps "$INDEX_JSON" "${SELECTED_SKILLS[@]}")
  SELECTED_SKILLS=("${EXPANDED[@]}")
fi

install_from_local() {
  local src="$1" skill="$2" dest_dir="$3"
  if [ "$FORCE" = false ] && [ -d "$dest_dir" ]; then
    echo "  Skipping $skill (already exists). Use --force to overwrite."
    return
  fi
  rm -rf "$dest_dir"
  cp -r "$src/$skill" "$dest_dir"
  echo "  Installed $skill"
}

install_from_archive() {
  local tmpdir="$1" skill="$2" dest_dir="$3"
  if [ "$FORCE" = false ] && [ -d "$dest_dir" ]; then
    echo "  Skipping $skill (already exists). Use --force to overwrite."
    return
  fi
  if [ ! -d "$tmpdir/$skill" ]; then
    echo "  Failed: $skill not found in archive" >&2
    return 1
  fi
  rm -rf "$dest_dir"
  cp -r "$tmpdir/$skill" "$dest_dir"
  echo "  Installed $skill"
}

mkdir -p "$DEST"

if [ -n "$SCRIPT_DIR" ] && [ -d "$SCRIPT_DIR/wren-generate-mdl" ]; then
  # ---- Local mode: copy directly from repo ----
  echo "Installing from local repo: $SCRIPT_DIR"
  echo "Destination: $DEST"
  echo ""
  for skill in "${SELECTED_SKILLS[@]}"; do
    install_from_local "$SCRIPT_DIR" "$skill" "$DEST/$skill"
  done
else
  # ---- Remote mode: download GitHub archive ----
  echo "Downloading skills from GitHub ($REPO @ $BRANCH)..."
  echo "Destination: $DEST"
  echo ""
  tmpdir=$(mktemp -d)
  trap 'rm -rf "$tmpdir"' EXIT

  # Build the list of paths to extract from the tarball
  extract_paths=()
  for skill in "${SELECTED_SKILLS[@]}"; do
    extract_paths+=("wren-engine-${BRANCH}/skills/${skill}")
  done

  curl -fsSL "https://github.com/$REPO/archive/refs/heads/$BRANCH.tar.gz" \
    | tar -xz -C "$tmpdir" --strip-components=2 "${extract_paths[@]}"

  for skill in "${SELECTED_SKILLS[@]}"; do
    install_from_archive "$tmpdir" "$skill" "$DEST/$skill"
  done
fi

echo ""
echo "Done. Invoke skills in your AI client:"
for skill in "${SELECTED_SKILLS[@]}"; do
  echo "  /$skill"
done
echo ""
echo "To update skills later, re-run with --force:"
echo "  curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash -s -- --force"
echo "Or check for updates: each skill notifies you automatically when a newer version is available."
