#!/usr/bin/env bash
# Verify that versions.json and index.json (in this script's directory) both match
# the version in each skill's SKILL.md frontmatter.
# Exits non-zero if any mismatch is found.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSIONS_JSON="$SCRIPT_DIR/versions.json"
INDEX_JSON="$SCRIPT_DIR/index.json"
ERRORS=0

while IFS= read -r skill; do
  skill_name="${skill//\"/}"
  skill_name="${skill_name%%:*}"
  skill_name="${skill_name// /}"

  versions_version=$(python3 -c "import json,sys; d=json.load(open('$VERSIONS_JSON')); print(d.get('$skill_name','MISSING'))")

  skill_file="$SCRIPT_DIR/$skill_name/SKILL.md"
  if [ ! -f "$skill_file" ]; then
    echo "ERROR: $skill_name listed in versions.json but $skill_file not found" >&2
    ERRORS=$((ERRORS + 1))
    continue
  fi

  md_version=$(grep -m1 'version:' "$skill_file" | sed 's/.*version: *"\{0,1\}\([^"]*\)"\{0,1\}/\1/' | tr -d ' "')

  if [ "$versions_version" != "$md_version" ]; then
    echo "MISMATCH: $skill_name — versions.json=$versions_version, SKILL.md=$md_version" >&2
    ERRORS=$((ERRORS + 1))
  else
    echo "OK (versions.json): $skill_name @ $versions_version"
  fi

  index_version=$(python3 -c "
import json, sys
skills = json.load(open('$INDEX_JSON')).get('skills', [])
match = next((s['version'] for s in skills if s['name'] == '$skill_name'), 'MISSING')
print(match)
")

  if [ "$index_version" != "$md_version" ]; then
    echo "MISMATCH: $skill_name — index.json=$index_version, SKILL.md=$md_version" >&2
    ERRORS=$((ERRORS + 1))
  else
    echo "OK (index.json):    $skill_name @ $index_version"
  fi
done < <(python3 -c "
import json
from pathlib import Path

root = Path('$SCRIPT_DIR')
versions = set(json.load(open('$VERSIONS_JSON')).keys())
index = {s['name'] for s in json.load(open('$INDEX_JSON')).get('skills', [])}
skill_dirs = {p.parent.name for p in root.glob('*/SKILL.md')}

for name in sorted(versions | index | skill_dirs):
    print(name)
")

if [ "$ERRORS" -gt 0 ]; then
  echo "" >&2
  echo "Found $ERRORS version mismatch(es). Update versions.json, index.json, or SKILL.md to match." >&2
  exit 1
fi

echo ""
echo "All skill versions match."
