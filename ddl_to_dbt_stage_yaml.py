#!/usr/bin/env python3
# ddl_to_dbt_stage_yaml.py
from __future__ import annotations
import re, sys, argparse
from pathlib import Path
from typing import Dict, List, Tuple, Any

try:
    import yaml  # type: ignore
except Exception:
    yaml = None

# ---------- Regexes ----------
DDL_CREATE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:\[(?P<schema>\w+)\]\.)?\[(?P<table>\w+)\]\s*\((?P<body>.*)\)\s*",
    re.IGNORECASE | re.DOTALL,
)
# Accept PRIMARY KEY with OR without an explicit CONSTRAINT name
PRIMARY_KEY_RE = re.compile(
    r"(?:CONSTRAINT\s+\[\w+\]\s+)?PRIMARY\s+KEY(?:\s+CLUSTERED|\s+NONCLUSTERED)?\s*\((?P<cols>.*?)\)",
    re.IGNORECASE | re.DOTALL,
)
DEFAULT_FOR_RE = re.compile(
    r"ALTER\s+TABLE\s+(?:\[(?P<schema>\w+)\]\.)?\[(?P<table>\w+)\]\s+ADD\s+CONSTRAINT\s+\[\w+\]\s+DEFAULT\s*\((?P<expr>.*?)\)\s+FOR\s+\[(?P<col>\w+)\]",
    re.IGNORECASE | re.DOTALL,
)
#  NOT NULL,  or  Col nvarchar(255) NULL,  or  [decimal](18, 2)
COLUMN_LINE_RE = re.compile(
    r"""^\s*
        \[(?P<name>\w+)\]\s+
        (?P<dtype>
            \[?\w+\]?
            (?:\s*\(\s*(?:MAX|\d+(?:\s*,\s*\d+)?)\s*\))?
        )
        (?P<rest>.*)$
    """,
    re.IGNORECASE | re.VERBOSE,
)
BRACKETED_COL_RE = re.compile(r"\[\s*(\w+)\s*\]")

# ---------- Parsing ----------
def split_columns_block(body: str) -> Tuple[List[str], List[str]]:
    """Depth-aware split of the CREATE TABLE (...) body on top-level commas."""
    items, buf, depth = [], [], 0
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            items.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        items.append(tail)

    col_items, constraint_items = [], []
    for it in items:
        if re.match(r"^\s*\[", it):
            col_items.append(it)
        else:
            constraint_items.append(it)
    return col_items, constraint_items

def parse_default_alters(sql: str) -> Dict[str, str]:
    return {m.group("col"): m.group("expr").strip() for m in DEFAULT_FOR_RE.finditer(sql)}

def parse_primary_key(constraints: List[str]) -> List[str]:
    for c in constraints:
        m = PRIMARY_KEY_RE.search(c)
        if m:
            return BRACKETED_COL_RE.findall(m.group("cols"))
    return []

def parse_columns(col_items: List[str]) -> List[Dict[str, Any]]:
    cols: List[Dict[str, Any]] = []
    for raw in col_items:
        m = COLUMN_LINE_RE.match(raw)
        if not m:
            continue
        name = m.group("name")
        dtype_raw = m.group("dtype").strip()
        dtype_clean = re.sub(r"[\[\]]", "", dtype_raw)  # strip [] around type
        rest = m.group("rest").strip()

        # IDENTITY(seed, increment)
        ident = None
        m_ident = re.search(r"IDENTITY\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", rest, re.IGNORECASE)
        if m_ident:
            ident = {"seed": int(m_ident.group(1)), "increment": int(m_ident.group(2))}

        # Nullability
        nullable = True
        if re.search(r"\bNOT\s+NULL\b", rest, re.IGNORECASE):
            nullable = False
        elif re.search(r"\bNULL\b", rest, re.IGNORECASE):
            nullable = True

        # Inline DEFAULT (supported but uncommon in SSMS CREATE scripts)
        inline_default = None
        m_def = re.search(r"\bDEFAULT\s+(\(?.*?\)?)\b", rest, re.IGNORECASE)
        if m_def:
            inline_default = m_def.group(1).strip()

        cols.append(
            {
                "name": name,
                "data_type": dtype_clean.upper(),
                "nullable": nullable,
                "identity": ident,
                "inline_default": inline_default,
            }
        )
    return cols

# ---------- YAML emission ----------
def emit_stage_yaml(
    table: str,
    columns: List[Dict[str, Any]],
    pk_cols: List[str],
    default_map: Dict[str, str],
    version: float = 2.0,
) -> str:
    doc: Dict[str, Any] = {
        "version": version,
        "models": [
            {
                "name": table,
                "description": "",     # leave blank for now
                "meta": {},
                "columns": [],
            }
        ],
    }

    # Stage files use meta.primary_key (array) instead of surrogate_key
    if pk_cols:
        doc["models"][0]["meta"]["primary_key"] = pk_cols

    for c in columns:
        meta: Dict[str, Any] = {
            "nullable": bool(c["nullable"]),
        }
        if c.get("identity"):
            meta["identity"] = c["identity"]
        default_expr = default_map.get(c["name"]) or c.get("inline_default")
        if default_expr:
            meta["default_expression"] = str(default_expr)

        doc["models"][0]["columns"].append(
            {
                "name": c["name"],
                "description": "",               # blank by request
                "data_type": c["data_type"],
                "meta": meta,                    # no unknown_member for Staging
            }
        )

    if yaml:
        return yaml.safe_dump(doc, sort_keys=False)
    else:
        import json
        return json.dumps(doc, indent=2)

# ---------- CLI ----------
def main() -> int:
    ap = argparse.ArgumentParser(
        description="Convert SQL Server CREATE TABLE DDL to dbt Stage model YAML."
    )
    ap.add_argument("sql_file", help="Path to .sql file containing a CREATE TABLE script")
    ap.add_argument(
        "--pk", "--primary-key",
        dest="primary_key",
        help="Comma-separated column list to force meta.primary_key when not in DDL (e.g. Code,Id,ItemId)",
    )
    ap.add_argument(
        "--version",
        type=float,
        default=2.0,
        help="Schema YAML version number (default: 2.0)",
    )
    args = ap.parse_args()

    sql = Path(args.sql_file).read_text(encoding="utf-8", errors="ignore")
    m = DDL_CREATE_RE.search(sql)
    if not m:
        print("Could not find a CREATE TABLE statement.", file=sys.stderr)
        return 1

    table = m.group("table")
    body = m.group("body")
    col_items, constraint_items = split_columns_block(body)
    columns = parse_columns(col_items)
    pk_cols = parse_primary_key(constraint_items)

    # Allow manual override for stage PKs (common if DDL doesn't declare PK)
    if not pk_cols and args.primary_key:
        pk_cols = [c.strip() for c in args.primary_key.split(",") if c.strip()]

    default_map = parse_default_alters(sql)
    print(emit_stage_yaml(table, columns, pk_cols, default_map, version=args.version))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
