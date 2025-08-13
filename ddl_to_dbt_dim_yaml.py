#!/usr/bin/env python3
# ddl_to_dbt_dim_yaml.py
from __future__ import annotations
import re, sys, argparse
from pathlib import Path
from typing import Dict, List, Tuple, Any

try:
    import yaml  # type: ignore
except Exception:
    yaml = None

# ---------- Single-quoted scalar support (only for unknown_member) ----------
class SingleQuoted(str):
    pass

if yaml:
    def _represent_single_quoted(dumper, data):
        return dumper.represent_scalar('tag:yaml.org,2002:str', str(data), style="'")
    yaml.add_representer(SingleQuoted, _represent_single_quoted)
    yaml.add_representer(SingleQuoted, _represent_single_quoted, Dumper=yaml.SafeDumper)

# Pretty list indentation (so list items under models/columns are indented)
IndentDumper = None
if yaml:
    class _IndentDumper(yaml.SafeDumper):
        def increase_indent(self, flow=False, indentless=False):
            return super().increase_indent(flow, indentless=False)
    IndentDumper = _IndentDumper

# ---------- Helpers ----------
def canonical_base_type(dtype: str) -> str:
    t = re.sub(r"[\[\]]", "", dtype).strip().upper()
    t = re.sub(r"\s*\(.*\)$", "", t)
    return t

NUMERIC_BASES = {
    "TINYINT","SMALLINT","INT","BIGINT","DECIMAL","NUMERIC","FLOAT","REAL","MONEY","SMALLMONEY","BIT"
}

UNKNOWN_BY_TYPE: Dict[str, Any] = {
    # numerics
    "TINYINT": 0, "SMALLINT": -1, "INT": -1, "BIGINT": -1,
    "DECIMAL": -1, "NUMERIC": -1, "FLOAT": -1, "REAL": -1,
    "MONEY": -1, "SMALLMONEY": -1, "BIT": 0,
    # non-numerics (will be single-quoted in YAML)
    "NVARCHAR": "Unknown", "VARCHAR": "Unknown",
    "NCHAR": "U", "CHAR": "U",
    "TEXT": "Unknown", "NTEXT": "Unknown",
    "UNIQUEIDENTIFIER": "00000000-0000-0000-0000-000000000000",
    "DATE": "1900-01-01",
    "TIME": "00:00:00",
    "DATETIME": "1900-01-01T00:00:00",
    "SMALLDATETIME": "1900-01-01T00:00:00",
    "DATETIME2": "1900-01-01T00:00:00",
    "DATETIMEOFFSET": "1900-01-01T00:00:00Z",
    "BINARY": "0x", "VARBINARY": "0x", "XML": "<unknown/>",
}

def unknown_for_dtype(dtype: str) -> Any:
    base = canonical_base_type(dtype)
    val = UNKNOWN_BY_TYPE.get(base, "Unknown")
    if base in NUMERIC_BASES:
        return val
    return SingleQuoted(str(val)) if yaml else str(val)

# ---------- Regexes ----------
DDL_CREATE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:\[(?P<schema>\w+)\]\.)?\[(?P<table>\w+)\]\s*\((?P<body>.*)\)\s*",
    re.IGNORECASE | re.DOTALL,
)
# Table-level PK and UNIQUE (inside CREATE TABLE), with/without CONSTRAINT name
PRIMARY_KEY_RE = re.compile(
    r"(?:CONSTRAINT\s+\[\w+\]\s+)?PRIMARY\s+KEY(?:\s+CLUSTERED|\s+NONCLUSTERED)?\s*\((?P<cols>.*?)\)",
    re.IGNORECASE | re.DOTALL,
)
UNIQUE_CONSTRAINT_RE = re.compile(
    r"(?:CONSTRAINT\s+\[\w+\]\s+)?UNIQUE(?:\s+CLUSTERED|\s+NONCLUSTERED)?\s*\((?P<cols>.*?)\)",
    re.IGNORECASE | re.DOTALL,
)
# CREATE UNIQUE INDEX outside CREATE TABLE
UNIQUE_INDEX_RE = re.compile(
    r"CREATE\s+UNIQUE\s+(?:CLUSTERED|NONCLUSTERED)?\s+INDEX\s+\[\w+\]\s+ON\s+(?:\[(?P<schema>\w+)\]\.)?\[(?P<table>\w+)\]\s*\((?P<cols>.*?)\)",
    re.IGNORECASE | re.DOTALL,
)
DEFAULT_FOR_RE = re.compile(
    r"ALTER\s+TABLE\s+(?:\[(?P<schema>\w+)\]\.)?\[(?P<table>\w+)\]\s+ADD\s+CONSTRAINT\s+\[\w+\]\s+DEFAULT\s*\((?P<expr>.*?)\)\s+FOR\s+\[(?P<col>\w+)\]",
    re.IGNORECASE | re.DOTALL,
)
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
# If the last column line includes a table-level constraint block with no comma
EMBEDDED_TBL_CONSTRAINT_RE = re.compile(r"\bCONSTRAINT\b.*?(?:\bPRIMARY\s+KEY\b|\bUNIQUE\b).*", re.IGNORECASE | re.DOTALL)

# ---------- Parsing ----------
def split_columns_block(body: str) -> Tuple[List[str], List[str]]:
    """Depth-aware split of CREATE TABLE body; also split out any embedded table-level constraints."""
    raw_items, buf, depth = [], [], 0
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            raw_items.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        raw_items.append(tail)

    col_items, constraint_items = [], []
    for it in raw_items:
        if re.match(r"^\s*\[", it):
            m = EMBEDDED_TBL_CONSTRAINT_RE.search(it)
            if m:
                head = it[:m.start()].rstrip().rstrip(",")
                tail = it[m.start():].strip()
                if head:
                    col_items.append(head)
                if tail:
                    constraint_items.append(tail)
            else:
                col_items.append(it.strip())
        else:
            constraint_items.append(it.strip())
    return col_items, constraint_items

def parse_default_alters(sql: str) -> Dict[str, str]:
    return {m.group("col"): m.group("expr").strip() for m in DEFAULT_FOR_RE.finditer(sql)}

def parse_primary_key(constraints: List[str], col_items: List[str]) -> List[str]:
    # table-level PK
    for c in constraints:
        m = PRIMARY_KEY_RE.search(c)
        if m:
            return BRACKETED_COL_RE.findall(m.group("cols"))
    # inline single-column PK
    pks_inline = []
    for raw in col_items:
        if re.search(r"\bPRIMARY\s+KEY\b", raw, re.IGNORECASE):
            m = COLUMN_LINE_RE.match(raw)
            if m:
                pks_inline.append(m.group("name"))
    return pks_inline

def parse_unique_business_key(sql: str, constraints: List[str], schema: str, table: str) -> List[str]:
    # UNIQUE constraint inside CREATE TABLE
    for c in constraints:
        m = UNIQUE_CONSTRAINT_RE.search(c)
        if m:
            cols = BRACKETED_COL_RE.findall(m.group("cols"))
            if cols:
                return cols
    # CREATE UNIQUE INDEX outside (ensure it's for this table)
    for m in UNIQUE_INDEX_RE.finditer(sql):
        sch = m.group("schema") or "dbo"
        tbl = m.group("table")
        if sch.lower() == (schema or "dbo").lower() and tbl.lower() == table.lower():
            cols = BRACKETED_COL_RE.findall(m.group("cols"))
            if cols:
                return cols
    return []

def parse_columns(col_items: List[str]) -> List[Dict[str, Any]]:
    cols: List[Dict[str, Any]] = []
    for raw in col_items:
        m = COLUMN_LINE_RE.match(raw)
        if not m:
            continue
        name = m.group("name")
        dtype_raw = m.group("dtype").strip()
        dtype_clean = re.sub(r"[\[\]]", "", dtype_raw)
        rest = m.group("rest").strip()

        ident = None
        m_ident = re.search(r"IDENTITY\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", rest, re.IGNORECASE)
        if m_ident:
            ident = {"seed": int(m_ident.group(1)), "increment": int(m_ident.group(2))}

        nullable = True
        if re.search(r"\bNOT\s+NULL\b", rest, re.IGNORECASE):
            nullable = False
        elif re.search(r"\bNULL\b", rest, re.IGNORECASE):
            nullable = True

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
def emit_dim_yaml(
    table: str,
    columns: List[Dict[str, Any]],
    surrogate_key: str | None,
    business_key_cols: List[str],
    default_map: Dict[str, str],
) -> str:
    doc: Dict[str, Any] = {
        "models": [
            {
                "name": table,
                "description": "",
                "meta": {
                    "business_key": business_key_cols or [],
                    "surrogate_key": surrogate_key or "",  # always present (blank if unknown)
                },
                "columns": [],
            }
        ]
    }

    for c in columns:
        meta: Dict[str, Any] = {
            "nullable": bool(c["nullable"]),
            "unknown_member": unknown_for_dtype(c["data_type"]),
        }
        if c.get("identity"):
            meta["identity"] = c["identity"]
        default_expr = default_map.get(c["name"]) or c.get("inline_default")
        if default_expr:
            meta["default_expression"] = str(default_expr)

        doc["models"][0]["columns"].append(
            {
                "name": c["name"],
                "description": "",
                "data_type": c["data_type"],
                "meta": meta,
            }
        )

    if yaml:
        return yaml.dump(
            doc,
            Dumper=IndentDumper,
            sort_keys=False,
            default_flow_style=False,
            indent=2,
            allow_unicode=True,
            width=120,
        )
    else:
        import json
        return json.dumps(doc, indent=2)

# ---------- CLI ----------
def main() -> int:
    ap = argparse.ArgumentParser(description="Convert SQL Server CREATE TABLE DDL to dbt DIM model YAML.")
    ap.add_argument("sql_file", help="Path to .sql file containing a CREATE TABLE script")
    ap.add_argument("--business-key", dest="business_key", help="Comma-separated columns for meta.business_key")
    ap.add_argument("--surrogate-key", dest="surrogate_key", help="Override meta.surrogate_key (single column)")
    args = ap.parse_args()

    sql = Path(args.sql_file).read_text(encoding="utf-8", errors="ignore")

    m = DDL_CREATE_RE.search(sql)
    if not m:
        print("Could not find a CREATE TABLE statement.", file=sys.stderr)
        return 1

    schema = m.group("schema") or "dbo"
    table  = m.group("table")
    body   = m.group("body")

    col_items, constraint_items = split_columns_block(body)
    columns = parse_columns(col_items)
    pk_cols = parse_primary_key(constraint_items, col_items)

    # Surrogate key heuristic: single-column PK (typical for IDENTITY key)
    inferred_surrogate = pk_cols[0] if len(pk_cols) == 1 else None

    # Business key: infer from UNIQUE constraint/index; allow CLI override
    inferred_bk = parse_unique_business_key(sql, constraint_items, schema, table)
    if args.business_key:
        business_key_cols = [c.strip() for c in args.business_key.split(",") if c.strip()]
    else:
        business_key_cols = inferred_bk

    # Surrogate key override (if provided)
    surrogate_key = args.surrogate_key.strip() if args.surrogate_key else inferred_surrogate

    default_map = parse_default_alters(sql)
    print(emit_dim_yaml(table, columns, surrogate_key, business_key_cols, default_map))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
