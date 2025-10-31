#!/usr/bin/env python3
"""
Update Databricks function descriptions in CSV by querying DESCRIBE FUNCTION EXTENDED.

- Input CSV format: function_type,name,return_type,param_names,param_types,description
- Connection via Databricks SQL Warehouse using environment variables:
    DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_ACCESS_TOKEN (or DATABRICKS_TOKEN)

Example:
    export DATABRICKS_SERVER_HOSTNAME="adb-xxxxxxxx.xx.azuredatabricks.net"
  export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxxxxxxxxxxxx"
  export DATABRICKS_ACCESS_TOKEN="dapixxx" # or use Databricks personal access token
  poetry run python ibis-server/tools/update_databricks_functions.py \
    --csv ibis-server/resources/function_list/databricks.csv --limit 50

Notes:
- System functions generally work with DESCRIBE FUNCTION EXTENDED <name>
- Some names may require backticks; script retries with backticks if needed
- If description cannot be parsed, leaves original unchanged unless --overwrite-empty
- Description extraction prefers Usage section; falls back to Function section if needed
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from typing import Iterable, List, Optional, Tuple, Any
from dotenv import load_dotenv

load_dotenv(override=True)

try:
    # databricks-sql-connector
    from databricks import sql as dbsql
except Exception as e:  # pragma: no cover - import error surfaced in runtime
    dbsql = None  # type: ignore




def connect() -> Any:
    if dbsql is None:
        raise RuntimeError(
            "databricks-sql-connector is not installed. Install it or ensure the ibis databricks extra is present."
        )
    host = os.environ.get("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    token = os.environ.get("DATABRICKS_ACCESS_TOKEN") or os.environ.get(
        "DATABRICKS_TOKEN"
    )
    if not (host and http_path and token):
        missing = [
            name
            for name, val in [
                ("DATABRICKS_SERVER_HOSTNAME", host),
                ("DATABRICKS_HTTP_PATH", http_path),
                ("DATABRICKS_ACCESS_TOKEN", token),
            ]
            if not val
        ]
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}"
        )
    return dbsql.connect(server_hostname=host, http_path=http_path, access_token=token)


def describe_function(cur: Any, func_name: str) -> Optional[List[str]]:
    """Return DESCRIBE FUNCTION EXTENDED lines (single text column) or None on failure."""
    queries = [
        f"DESCRIBE FUNCTION EXTENDED {func_name}",
        f"DESCRIBE FUNCTION EXTENDED `{func_name}`",
    ]
    for q in queries:
        try:
            cur.execute(q)
            rows = cur.fetchall()
            if not rows:
                continue
            # databricks usually returns one text column; be resilient
            lines: List[str] = []
            for row in rows:
                if isinstance(row, (tuple, list)) and len(row) >= 1:
                    lines.append(str(row[0]))
                else:
                    lines.append(str(row))
            return lines
        except Exception:
            # try next form
            continue
    return None


# Known section labels (lowercase, without the trailing colon)
KNOWN_SECTION_NAMES = (
    "function",
    "class",
    "usage",
    "extended usage",
    "arguments",
    "examples",
    "created by",
    "since",
)


def _normalize_lines(block_lines: List[str]) -> List[str]:
    """Split any multi-line string entries into per-line strings."""
    out: List[str] = []
    for raw in block_lines:
        # Ensure str and split on newline to handle multi-line cells
        s = str(raw).replace("\r\n", "\n").replace("\r", "\n")
        out.extend(s.split("\n"))
    return out


def _collect_after_header(lines: List[str], header: str) -> List[str]:
    """Collect content lines after a section header (case-insensitive) until next section or blank divider.

    - Header matching is based on the label before the first colon, e.g. "Usage" or "Function" (colon optional in input).
    - Inline content on the same line after the colon is captured as the first element.
    - Collection stops when another known section label is encountered or a blank line is hit (after capturing at least one line).
    """
    out: List[str] = []
    started = False
    header_key = header.rstrip(":").strip().lower()
    for raw in lines:
        line = raw.rstrip()
        striped = line.strip()

        # Helper to detect if a line starts a section header and optionally return its label and remainder
        def parse_header(s: str) -> Tuple[Optional[str], Optional[str]]:
            if not s:
                return None, None
            idx = s.find(":")
            if idx == -1:
                return None, None
            label = s[: idx].strip().lower()
            remainder = s[idx + 1 :].strip()
            return label, remainder

        if not started:
            lbl, remainder = parse_header(striped)
            if lbl == header_key:
                started = True
                if remainder:
                    out.append(remainder)
                continue
        else:
            # If we reach another header, stop
            lbl, _ = parse_header(striped)
            if lbl and lbl in KNOWN_SECTION_NAMES:
                break

            # Skip obvious separators
            if not striped:
                if out:
                    break
                else:
                    continue

            out.append(striped)
    return out


def parse_description(lines: List[str]) -> Optional[str]:
    """Extract only the Usage section (single-line summary).

    Preference:
    - Usage: <...> -> return only the Usage content, ignoring Extended Usage/Examples/Since
      If Usage spans multiple lines immediately after the header, capture until blank or next header.
    - If there is no Usage:, fall back to content after Function:.
    """
    if not lines:
        return None
    # Normalize into per-line strings so headers can be detected
    norm = _normalize_lines(lines)
    # Collect only the Usage section (inline part plus immediate continuation lines)
    usage_lines = _collect_after_header(norm, "Usage")
    if usage_lines:
        # Flatten and normalize whitespace into a single line
        text = " ".join(usage_lines)
        text = " ".join(text.split())
        return text
    # Fallback: try take a succinct line after Function:
    fn = _collect_after_header(norm, "Function")
    if fn:
        text = " ".join(fn)
        text = " ".join(text.split())
        return text
    return None


def read_csv(path: str) -> Tuple[List[str], List[List[str]]]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        raise ValueError("CSV is empty")
    header = rows[0]
    body = rows[1:]
    return header, body


def write_csv(path: str, header: List[str], rows: List[List[str]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


DEFAULT_PLACEHOLDERS = {
    "Scalar function",
    "Aggregate function",
    "Windowing helper",
}

def _normalize_token(s: str) -> str:
    # Lowercase, collapse spaces, strip quotes/backticks and trailing periods
    t = (s or "").strip().strip("`\"' ")
    t = " ".join(t.split())
    return t.lower().rstrip(".")


def is_placeholder_desc(desc: str, func_name: str) -> bool:
    """Return True if description looks like a placeholder.

    Placeholder conditions:
    - empty string
    - known generic placeholders (Scalar function, Aggregate function, Windowing helper)
    - description equals the function name (case-insensitive), optionally with empty parentheses (e.g., name or name())
    """
    if not desc:
        return True
    d = desc.strip()
    if d in DEFAULT_PLACEHOLDERS:
        return True
    dn = _normalize_token(d)
    fn = _normalize_token(func_name)
    if dn == fn or dn == f"{fn}()":
        return True
    return False


def placeholder_reason(desc: str, func_name: str) -> str:
    if not desc:
        return "empty description"
    d = desc.strip()
    if d in DEFAULT_PLACEHOLDERS:
        return f"generic placeholder: {d}"
    dn = _normalize_token(d)
    fn = _normalize_token(func_name)
    if dn == fn:
        return "equals function name"
    if dn == f"{fn}()":
        return "equals function name with empty parentheses"
    return "non-placeholder"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Update Databricks function descriptions in CSV"
    )
    ap.add_argument(
        "--csv",
        default="resources/function_list/databricks.csv",
        help="Path to databricks.csv",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of functions to update (for dry-runs)",
    )
    ap.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing non-empty descriptions",
    )
    ap.add_argument(
        "--overwrite-empty",
        action="store_true",
        help="Overwrite only if description is empty or placeholder",
    )
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Sleep seconds between DESCRIBE calls (rate limiting)",
    )
    ap.add_argument(
        "--verbose",
        action="store_true",
        help="Print debug information for each function",
    )
    args = ap.parse_args()

    csv_path = args.csv
    if args.verbose:
        print(f"Using CSV: {csv_path}")
    header, rows = read_csv(csv_path)

    # Locate column indices
    try:
        name_idx = header.index("name")
        desc_idx = header.index("description")
    except ValueError as e:
        raise SystemExit(f"CSV missing required columns: {e}")

    to_process = rows
    if args.limit is not None:
        to_process = rows[: args.limit]

    updated = 0
    skipped = 0

    with connect() as conn:
        with conn.cursor() as cur:
            for i, row in enumerate(to_process):
                func = row[name_idx].strip()
                current_desc = row[desc_idx].strip() if len(row) > desc_idx else ""

                should_update = False
                if args.overwrite:
                    should_update = True
                elif args.overwrite_empty:
                    should_update = is_placeholder_desc(current_desc, func)
                else:
                    # default behavior: fill only placeholders/empty
                    should_update = is_placeholder_desc(current_desc, func)

                if not should_update:
                    if args.verbose:
                        print(
                            f"Skip {func}: existing description kept (use --overwrite or --overwrite-empty). "
                            f"Current desc=\"{current_desc}\"; reason={placeholder_reason(current_desc, func)}"
                        )
                    skipped += 1
                    continue

                lines = describe_function(cur, func)
                if args.verbose:
                    print(f"DESCRIBE {func}: {lines}")
                if not lines:
                    if args.verbose:
                        print(f"Skip {func}: DESCRIBE returned no rows or failed in both forms")
                    skipped += 1
                    continue
                desc = parse_description(lines)
                if desc:
                    row[desc_idx] = desc
                    if args.verbose:
                        print(f"Updated {func}: {desc}")
                    updated += 1
                else:
                    if args.verbose:
                        print(f"Skip {func}: failed to parse description from DESCRIBE output")
                    skipped += 1

                if args.sleep:
                    time.sleep(args.sleep)

    # Merge back updated rows (if limit was set)
    if args.limit is not None:
        rows[: args.limit] = to_process
    else:
        rows = to_process

    write_csv(csv_path, header, rows)

    print(
        f"Done. Updated {updated} descriptions, skipped {skipped}. CSV saved to {csv_path}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
