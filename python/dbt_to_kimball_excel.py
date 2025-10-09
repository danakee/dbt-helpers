
#!/usr/bin/env python3
"""
dbt_to_kimball_excel.py

Generate a Kimball-style Excel workbook from a dbt Core project's manifest.json and catalog.json.

What you get:
- Summary sheet of all materialized table models (and optionally views), with model path, alias, database.schema.name, tags, and materialization.
- One sheet per model (Excel tab) listing columns, data types (from catalog), descriptions (from manifest), tests (unique, not_null, relationships, etc.), and PK/FK heuristics.
- A Relationships sheet showing inferred foreign keys from dbt relationship tests.
- A Star Map sheet that groups facts and their linked dimensions (inferred from relationship tests & naming/tag heuristics).

Usage:
    python dbt_to_kimball_excel.py --manifest path/to/manifest.json --catalog path/to/catalog.json --out workbook.xlsx [--include-views]

Notes:
- "Dimension" vs "Fact" is inferred using tags (contains 'dim' / 'dimension' / 'fact') and/or name prefix heuristics ('dim_'/'fact_').
- Primary key inference: a column is treated as a PK candidate if it has both `unique` and `not_null` tests OR its name matches common patterns (e.g., endswith 'Key' or 'Id') and is unique. You can refine these rules as needed.
- Foreign key inference: derived from dbt relationships tests (core + packages) defined in schema.yml; we parse tests from manifest.
- Excel sheet names are capped at 31 chars; we ensure uniqueness using numeric suffixes if needed.
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

# ----------------------------
# Utility helpers
# ----------------------------
def load_json(path: Path):
    with path.open('r', encoding='utf-8') as f:
        return json.load(f)

def node_is_model_table(node):
    # dbt node is a model and materialized as table
    if node.get('resource_type') != 'model':
        return False
    mat = (node.get('config') or {}).get('materialized')
    return mat in {'table', 'incremental', 'ephemeral', 'view'}  # allow broader; we'll filter later

def classify_model_kind(name: str, tags):
    name_lower = name.lower()
    tags_lower = {t.lower() for t in (tags or [])}
    if any(t in tags_lower for t in ['fact', 'facts']): 
        return 'Fact'
    if any(t in tags_lower for t in ['dim', 'dimension', 'dimensions']): 
        return 'Dimension'
    if name_lower.startswith('fact_'): 
        return 'Fact'
    if name_lower.startswith('dim_'): 
        return 'Dimension'
    # Heuristic: if endswith _fact/_dim
    if name_lower.endswith('_fact'): 
        return 'Fact'
    if name_lower.endswith('_dim'): 
        return 'Dimension'
    return 'Other'

def safe_sheet_name(base: str, used: set):
    # Excel sheet name max 31 chars and cannot contain: : \ / ? * [ ]
    cleaned = re.sub(r'[:\\\/\?\*\[\]]', '_', base)[:31] or 'Sheet'
    candidate = cleaned
    i = 1
    while candidate in used:
        suffix = f'_{i}'
        candidate = (cleaned[:31-len(suffix)] + suffix)
        i += 1
    used.add(candidate)
    return candidate

def extract_tests_for_model(manifest, node_id):
    """Return list of tests attached to the model's columns, keyed by column name."""
    results = defaultdict(list)
    # In manifest, tests appear as nodes with resource_type == 'test' and depend on model via 'child_map' or 'parent_map' or 'compiled_code'
    # Simpler approach: iterate all tests and check refs in 'depends_on' or 'fqn' 
    for test_id, test in manifest.get('nodes', {}).items():
        if test.get('resource_type') != 'test':
            continue
        # Each test usually has 'test_metadata' and dict of kwargs including 'model' and 'column_name' (varies by adapter/packages)
        # Also test['depends_on']['nodes'] includes the model node_id
        depends = (test.get('depends_on') or {}).get('nodes') or []
        if node_id not in depends:
            # some relationship tests depend on both child and parent nodes; keep scanning for others too
            continue
        # Column
        col = None
        test_meta = test.get('test_metadata') or {}
        kwargs = test_meta.get('kwargs') or {}
        col = kwargs.get('column_name') or kwargs.get('field') or kwargs.get('column') or None
        # Fallback to test name for clarity
        test_name = (test_meta.get('name') or test.get('name') or 'test').lower()
        # For relationships test, capture target
        rel = None
        if 'relationship' in test_name or 'relationships' in test_name:
            # dbt core relationships typically provide to: model, field
            rel_model = kwargs.get('to') or kwargs.get('model') or kwargs.get('to_model')
            rel_field = kwargs.get('field') or kwargs.get('to_field') or kwargs.get('column')
            rel = {'to_model': rel_model, 'to_field': rel_field}
        results[col].append({'name': test_name, 'details': rel, 'raw': test})
    return results

def infer_pk_fk(columns_tests):
    """
    Infer PK and FK flags per column from tests.
    Returns dict: {col_name: {'is_pk': bool, 'is_fk': bool}}
    """
    inferred = {}
    for col, tests in columns_tests.items():
        names = [t['name'] for t in tests]
        is_unique = any('unique' in n for n in names)
        is_not_null = any(n in ('not_null', 'not-null', 'not null') or 'not_null' in n for n in names)
        has_rel = any('relationship' in n or 'relationships' in n for n in names)
        # Heuristics for PK
        is_pk = bool(is_unique and is_not_null)
        if not is_pk and col:
            # name heuristic: endswith 'key' or 'id' and unique
            cl = col.lower()
            if is_unique and (cl.endswith('key') or cl.endswith('id')):
                is_pk = True
        inferred[col] = {'is_pk': is_pk, 'is_fk': has_rel}
    return inferred

def model_relation_identifiers(node):
    db = (node.get('database') or node.get('schema') or '').strip('"')
    schema = (node.get('schema') or '').strip('"')
    alias = (node.get('alias') or node.get('name') or '').strip('"')
    return db, schema, alias

def get_catalog_columns_for_model(catalog, node):
    # catalog nodes keyed by "database.schema.name"
    db, schema, alias = model_relation_identifiers(node)
    key = f'{db}.{schema}.{alias}'.lower()
    for k, v in (catalog.get('nodes') or {}).items():
        if k.lower() == key:
            # columns is dict: {colname: {type, index, comment}}
            return v.get('columns') or {}
    return {}

def build_relationship_rows(manifest):
    """
    Returns list of rows for relationships sheet:
    FromModel, FromColumn, ToModel, ToColumn, TestName
    """
    rows = []
    nodes = manifest.get('nodes', {})
    id_to_name = {nid: n.get('alias') or n.get('name') for nid, n in nodes.items() if n.get('resource_type') == 'model'}
    for test_id, test in nodes.items():
        if test.get('resource_type') != 'test':
            continue
        meta = test.get('test_metadata') or {}
        tname = (meta.get('name') or test.get('name') or '').lower()
        if 'relationship' not in tname and 'relationships' not in tname:
            continue
        depends = (test.get('depends_on') or {}).get('nodes') or []
        # Typically first is child (from), second is parent (to), but not guaranteed; we'll use kwargs
        kwargs = meta.get('kwargs') or {}
        to_model = kwargs.get('to') or kwargs.get('model') or kwargs.get('to_model')
        to_field = kwargs.get('field') or kwargs.get('to_field') or kwargs.get('column')
        # From side
        from_model_node = None
        for nid in depends:
            n = nodes.get(nid) or {}
            if n.get('resource_type') == 'model':
                from_model_node = n
                break
        from_model = (from_model_node.get('alias') or from_model_node.get('name')) if from_model_node else None
        # Column name on 'from' side
        from_column = kwargs.get('column_name') or kwargs.get('field') or kwargs.get('column')
        rows.append({
            'FromModel': from_model,
            'FromColumn': from_column,
            'ToModel': to_model,
            'ToColumn': to_field,
            'TestName': tname
        })
    return rows

def guess_fact_groups(rel_rows, model_kinds):
    """
    Build a mapping FactModel -> set(DimensionModel) based on relationships.
    """
    fact_to_dims = defaultdict(set)
    for r in rel_rows:
        frm = (r.get('FromModel') or '').lower()
        to = (r.get('ToModel') or '').lower()
        if not frm or not to: 
            continue
        # Use classification dict
        frm_kind = model_kinds.get(frm, 'Other')
        to_kind = model_kinds.get(to, 'Other')
        if frm_kind == 'Fact' and to_kind in {'Dimension', 'Other'}:
            fact_to_dims[frm].add(to)
        elif to_kind == 'Fact' and frm_kind in {'Dimension', 'Other'}:
            fact_to_dims[to].add(frm)
    return {k: sorted(v) for k, v in fact_to_dims.items()}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest', required=True, type=Path)
    parser.add_argument('--catalog', required=True, type=Path)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--include-views', action='store_true', help='Include view-materialized models, not only table/incremental')
    args = parser.parse_args()

    manifest = load_json(args.manifest)
    catalog = load_json(args.catalog)

    nodes = manifest.get('nodes', {})
    models = [n for n in nodes.values() if n.get('resource_type') == 'model']

    # Filter models by materialization
    filtered = []
    for n in models:
        mat = (n.get('config') or {}).get('materialized')
        if mat in {'table', 'incremental'}:
            filtered.append(n)
        elif args.include_views and mat == 'view':
            filtered.append(n)

    # Precompute model kinds
    model_kinds = {}
    name_to_node = {}
    for n in filtered:
        alias = (n.get('alias') or n.get('name')).lower()
        model_kinds[alias] = classify_model_kind(alias, n.get('tags', []))
        name_to_node[alias] = n

    # Relationships rows
    rel_rows = build_relationship_rows(manifest)
    fact_groups = guess_fact_groups(rel_rows, model_kinds)

    # Build summary sheet
    summary_rows = []
    for n in filtered:
        db, schema, alias = model_relation_identifiers(n)
        mat = (n.get('config') or {}).get('materialized')
        fqn = '.'.join(n.get('fqn') or [])
        tags = ','.join(n.get('tags') or [])
        desc = n.get('description') or ''
        kind = model_kinds.get(alias.lower(), 'Other')
        summary_rows.append({
            'Model': alias,
            'Kind': kind,
            'Materialization': mat,
            'Database': db,
            'Schema': schema,
            'Relation': f'{db}.{schema}.{alias}',
            'Path': n.get('path'),
            'FQN': fqn,
            'Tags': tags,
            'Description': desc
        })
    df_summary = pd.DataFrame(summary_rows).sort_values(['Kind','Model'])

    # Relationships sheet
    df_rel = pd.DataFrame(rel_rows) if rel_rows else pd.DataFrame(columns=['FromModel','FromColumn','ToModel','ToColumn','TestName'])

    # Star Map sheet
    star_rows = []
    for fact, dims in fact_groups.items():
        if not dims:
            star_rows.append({'FactModel': fact, 'DimensionModel': None})
        else:
            for d in dims:
                star_rows.append({'FactModel': fact, 'DimensionModel': d})
    df_star = pd.DataFrame(star_rows) if star_rows else pd.DataFrame(columns=['FactModel','DimensionModel'])

    # Build per-model sheets
    used_sheet_names = set()
    with pd.ExcelWriter(args.out, engine='openpyxl') as writer:
        # Summary first
        df_summary.to_excel(writer, index=False, sheet_name='Summary')
        if not df_rel.empty:
            df_rel.to_excel(writer, index=False, sheet_name='Relationships')
        if not df_star.empty:
            df_star.to_excel(writer, index=False, sheet_name='Star Map')

        # Per model
        for n in filtered:
            db, schema, alias = model_relation_identifiers(n)
            sheet_base = f'{alias}'
            sheet_name = safe_sheet_name(sheet_base, used_sheet_names)

            # Column tests per model
            col_tests = extract_tests_for_model(manifest, n.get('unique_id'))
            pkfk_flags = infer_pk_fk(col_tests)

            # Columns from catalog
            columns = get_catalog_columns_for_model(catalog, n)

            # Assemble rows
            rows = []
            if columns:
                # catalog returns dict; 'index' can be used to order
                # Build an order mapping
                ordered = sorted(columns.items(), key=lambda kv: (kv[1].get('index') or 0))
                for col, meta in ordered:
                    col_lower = col
                    dtype = meta.get('type') or ''
                    comment = meta.get('comment') or ''
                    tests = col_tests.get(col_lower) or []
                    test_list = ', '.join(sorted({t['name'] for t in tests})) if tests else ''
                    is_pk = pkfk_flags.get(col_lower, {}).get('is_pk', False)
                    is_fk = pkfk_flags.get(col_lower, {}).get('is_fk', False)
                    rows.append({
                        'Column': col,
                        'DataType': dtype,
                        'Description': comment,
                        'Tests': test_list,
                        'IsPK?': 'Y' if is_pk else '',
                        'IsFK?': 'Y' if is_fk else ''
                    })
            else:
                # Fallback to manifest columns if catalog missing
                # dbt 1.6+ may store in n['columns']
                manifest_cols = (n.get('columns') or {})
                for col, meta in manifest_cols.items():
                    desc = meta.get('description') or ''
                    tests = col_tests.get(col) or []
                    test_list = ', '.join(sorted({t['name'] for t in tests})) if tests else ''
                    is_pk = pkfk_flags.get(col, {}).get('is_pk', False)
                    is_fk = pkfk_flags.get(col, {}).get('is_fk', False)
                    rows.append({
                        'Column': col,
                        'DataType': '',
                        'Description': desc,
                        'Tests': test_list,
                        'IsPK?': 'Y' if is_pk else '',
                        'IsFK?': 'Y' if is_fk else ''
                    })

            df = pd.DataFrame(rows, columns=['Column','DataType','Description','Tests','IsPK?','IsFK?'])
            # Add a header block with model metadata on top as a separate sheet? We'll put a small "Model Info" table at top-left by reserving rows.
            # Simpler: write df; then append info on a second small table (pandas makes it harder to put two tables). We'll create a one-row info sheet per model first.
            # Instead: We'll add columns to the top as metadata rows (prefixed with '#').
            info_rows = [
                {'Column': '#Model', 'DataType': alias, 'Description': '', 'Tests': '', 'IsPK?': '', 'IsFK?': ''},
                {'Column': '#Kind', 'DataType': classify_model_kind(alias, n.get('tags', [])), 'Description': '', 'Tests': '', 'IsPK?': '', 'IsFK?': ''},
                {'Column': '#Materialization', 'DataType': (n.get('config') or {}).get('materialized'), 'Description': '', 'Tests': '', 'IsPK?': '', 'IsFK?': ''},
                {'Column': '#Relation', 'DataType': f'{db}.{schema}.{alias}', 'Description': '', 'Tests': '', 'IsPK?': '', 'IsFK?': ''},
                {'Column': '#Tags', 'DataType': ','.join(n.get('tags') or []), 'Description': '', 'Tests': '', 'IsPK?': '', 'IsFK?': ''},
                {'Column': '#Description', 'DataType': (n.get('description') or ''), 'Description': '', 'Tests': '', 'IsPK?': '', 'IsFK?': ''},
            ]
            df_out = pd.concat([pd.DataFrame(info_rows), df], ignore_index=True)
            df_out.to_excel(writer, index=False, sheet_name=sheet_name)

    print(f'Wrote: {args.out}')

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'ERROR: {e}', file=sys.stderr)
        sys.exit(1)
