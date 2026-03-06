#!/usr/bin/env bash
# Verify that skills/versions.json matches the version in each skill's SKILL.md frontmatter.
# Exits non-zero if any mismatch is found.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSIONS_JSON="$SCRIPT_DIR/versions.json"
ERRORS=0

while IFS= read -r skill; do
  skill_name="${skill//\"/}"
  skill_name="${skill_name%%:*}"
  skill_name="${skill_name// /}"

  json_version=$(python3 -c "import json,sys; d=json.load(open('$VERSIONS_JSON')); print(d.get('$skill_name','MISSING'))")

  skill_file="$SCRIPT_DIR/$skill_name/SKILL.md"
  if [ ! -f "$skill_file" ]; then
    echo "ERROR: $skill_name listed in versions.json but $skill_file not found" >&2
    ERRORS=$((ERRORS + 1))
    continue
  fi

  md_version=$(grep -m1 'version:' "$skill_file" | sed 's/.*version: *"\{0,1\}\([^"]*\)"\{0,1\}/\1/' | tr -d ' "')

  if [ "$json_version" != "$md_version" ]; then
    echo "MISMATCH: $skill_name — versions.json=$json_version, SKILL.md=$md_version" >&2
    ERRORS=$((ERRORS + 1))
  else
    echo "OK: $skill_name @ $json_version"
  fi
done < <(python3 -c "import json,sys; [print(k) for k in json.load(open('$VERSIONS_JSON')).keys()]")

if [ "$ERRORS" -gt 0 ]; then
  echo "" >&2
  echo "Found $ERRORS version mismatch(es). Update versions.json or SKILL.md to match." >&2
  exit 1
fi

echo ""
echo "All skill versions match."
