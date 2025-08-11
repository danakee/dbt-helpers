from __future__ import annotations
import re, sys
from pathlib import Path
from typing import Dict, List, Tuple, Any

try:
    import yaml  # type: ignore
except Exception:
    yaml = None

# ---------- Single-quoted scalar support (for unknown_member only) ----------
class SingleQuoted(str):
    """Marker type to force single-quoted style for this string in YAML output."""
    pass

if yaml:
    def _represent_single_quoted(dumper, data):
        return dumper.represent_scalar('tag:yaml.org,2002:str', str(data), style="'")
    # Register for both default dumper and SafeDumper (used by safe_dump)
    yaml.add_representer(SingleQuoted, _represent_single_quoted)
    yaml.add_representer(SingleQuoted, _represent_single_quoted, Dumper=yaml.SafeDumper)

# ---------- Helpers ----------
def canonical_base_type(dtype: str) -> str:
    t = re.sub(r"[\[\]]", "", dtype).strip().upper()
    t = re.sub(r"\s*\(.*\)$", "", t)  # strip (...) precision/scale
    return t

NUMERIC_BASES = {
    "TINYINT","SMALLINT","INT","BIGINT","DECIMAL","NUMERIC","FLOAT","REAL","MONEY","SMALLMONEY","BIT"
}

UNKNOWN_BY_TYPE: Dict[str, Any] = {
    # numerics (left as numbers)
    "TINYINT": 0, "SMALLINT": -1, "INT": -1, "BIGINT": -1,
    "DECIMAL": -1, "NUMERIC": -1, "FLOAT": -1, "REAL": -1,
    "MONEY": -1, "SMALLMONEY": -1, "BIT": 0,
    # strings / non-numerics (will be single-quoted)
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
        return val  # numeric → no quotes
    # non-numeric → force single-quoted style (if yaml available)
    return SingleQuoted(str(val)) if yaml else str(val)

# ---------- Regexes ----------
DDL_CREATE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:\[(?P<schema>\w+)\]\.)?\[(?P<table>\w+)\]\s*\((?P<body>.*)\)\s*",
    re.IGNORECASE | re.DOTALL,
)
# Accept PK with OR without an explicit CONSTRAINT name
PRIMARY_KEY_RE = re.compile(
    r"(?:CONSTRAINT\s+\[\w+\]\s+)?PRIMARY\s+KEY(?:\s+CLUSTERED|\s+NONCLUSTERED)?\s*\((?P<cols>.*?)\)",
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

# ---------- Parsing ----------
def split_columns_block(body: str) -> Tuple[List[str], List[str]]:
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
def emit_dbt_yaml(schema: str, table: str, columns: List[Dict[str, Any]], pk_cols: List[str], default_map: Dict[str, str]) -> str:
    model: Dict[str, Any] = {
        "models": [
            {
                "name": table,
                "description": "",
                "meta": {},
                "columns": [],
            }
        ]
    }

    if len(pk_cols) == 1:
        model["models"][0]["meta"]["surrogate_key"] = pk_cols[0]

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

        model["models"][0]["columns"].append(
            {
                "name": c["name"],
                "description": "",
                "data_type": c["data_type"],
                "meta": meta,
            }
        )

    if yaml:
        return yaml.safe_dump(model, sort_keys=False)
    else:
        import json
        return json.dumps(model, indent=2)

# ---------- Main ----------
def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python ddl_to_dbt_yaml.py <input.sql>", file=sys.stderr)
        return 2
    sql = Path(sys.argv[1]).read_text(encoding="utf-8", errors="ignore")

    m = DDL_CREATE_RE.search(sql)
    if not m:
        print("Could not find a CREATE TABLE statement.", file=sys.stderr)
        return 1

    schema = m.group("schema") or "dbo"
    table = m.group("table")
    body = m.group("body")

    col_items, constraint_items = split_columns_block(body)
    columns = parse_columns(col_items)
    pk_cols = parse_primary_key(constraint_items)
    default_map = parse_default_alters(sql)

    print(emit_dbt_yaml(schema, table, columns, pk_cols, default_map))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())