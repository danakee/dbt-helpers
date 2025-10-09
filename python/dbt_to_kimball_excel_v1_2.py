
#!/usr/bin/env python3
"""
dbt_to_kimball_excel.py  (v1.2)

What's new:
- Supports custom materializations.
- Default: include ALL models except ephemeral (so custom mats are included automatically).
- Optional: --materializations table,incremental,my_custom to restrict to a set.
- Keeps --include-views for convenience, but it's redundant unless you restrict.

Usage examples:
    # Include everything except ephemeral
    python dbt_to_kimball_excel.py --manifest target/manifest.json --catalog target/catalog.json --out kimball.xlsx

    # Only include table+incremental+my_custom
    python dbt_to_kimball_excel.py --manifest target/manifest.json --catalog target/catalog.json --out kimball.xlsx --materializations table,incremental,my_custom

    # Only include views too (when restricting)
    python dbt_to_kimball_excel.py --manifest target/manifest.json --catalog target/catalog.json --out kimball.xlsx --materializations table,incremental,view --include-views
"""
import argparse, json, re, sys
from collections import defaultdict
from pathlib import Path
import pandas as pd

def load_json(path: Path):
    with path.open('r', encoding='utf-8') as f:
        return json.load(f)

def node_is_model(node): return node.get('resource_type') == 'model'

def classify_model_kind(name: str, tags):
    name_lower = (name or '').lower()
    tags_lower = {t.lower() for t in (tags or [])}
    if any(t in tags_lower for t in ['fact','facts']): return 'Fact'
    if any(t in tags_lower for t in ['dim','dimension','dimensions']): return 'Dimension'
    if name_lower.startswith('fact_') or name_lower.endswith('_fact'): return 'Fact'
    if name_lower.startswith('dim_') or name_lower.endswith('_dim'): return 'Dimension'
    return 'Other'

def safe_sheet_name(base: str, used: set):
    cleaned = re.sub(r'[:\\\/\?\*\[\]]', '_', base or 'Sheet')[:31] or 'Sheet'
    candidate = cleaned; i = 1
    while candidate in used:
        suffix = f'_{i}'; candidate = (cleaned[:31-len(suffix)] + suffix); i += 1
    used.add(candidate); return candidate

def extract_tests_for_model(manifest, node_id):
    results = defaultdict(list)
    for test_id, test in manifest.get('nodes', {}).items():
        if test.get('resource_type') != 'test': continue
        depends = (test.get('depends_on') or {}).get('nodes') or []
        if node_id not in depends: continue
        meta = test.get('test_metadata') or {}; kwargs = meta.get('kwargs') or {}
        col = kwargs.get('column_name') or kwargs.get('field') or kwargs.get('column') or None
        test_name = (meta.get('name') or test.get('name') or 'test').lower()
        rel = None
        if 'relationship' in test_name or 'relationships' in test_name:
            rel_model = kwargs.get('to') or kwargs.get('model') or kwargs.get('to_model')
            rel_field = kwargs.get('field') or kwargs.get('to_field') or kwargs.get('column')
            rel = {'to_model': rel_model, 'to_field': rel_field}
        results[col].append({'name': test_name, 'details': rel, 'raw': test})
    return results

def infer_pk_fk(columns_tests):
    inferred = {}
    for col, tests in columns_tests.items():
        names = [t['name'] for t in tests]
        is_unique = any('unique' in n for n in names)
        is_not_null = any(n in ('not_null','not-null','not null') or 'not_null' in n for n in names)
        has_rel = any('relationship' in n or 'relationships' in n for n in names)
        is_pk = bool(is_unique and is_not_null)
        if not is_pk and col:
            cl = col.lower()
            if is_unique and (cl.endswith('key') or cl.endswith('id')): is_pk = True
        inferred[col] = {'is_pk': is_pk, 'is_fk': has_rel}
    return inferred

def model_relation_identifiers(node):
    db = (node.get('database') or node.get('schema') or '').strip('"')
    schema = (node.get('schema') or '').strip('"')
    alias = (node.get('alias') or node.get('name') or '').strip('"')
    return db, schema, alias

def get_catalog_columns_for_model(catalog, node):
    db, schema, alias = model_relation_identifiers(node)
    key = f'{db}.{schema}.{alias}'.lower()
    for k, v in (catalog.get('nodes') or {}).items():
        if k.lower() == key: return v.get('columns') or {}
    return {}

def build_relationship_rows(manifest):
    rows = []; nodes = manifest.get('nodes', {})
    for _, test in nodes.items():
        if test.get('resource_type') != 'test': continue
        meta = test.get('test_metadata') or {}; tname = (meta.get('name') or test.get('name') or '').lower()
        if 'relationship' not in tname and 'relationships' not in tname: continue
        depends = (test.get('depends_on') or {}).get('nodes') or []
        kwargs = meta.get('kwargs') or {}
        to_model = kwargs.get('to') or kwargs.get('model') or kwargs.get('to_model')
        to_field = kwargs.get('field') or kwargs.get('to_field') or kwargs.get('column')
        from_model_node = None
        for nid in depends:
            n = nodes.get(nid) or {}
            if n.get('resource_type') == 'model': from_model_node = n; break
        from_model = (from_model_node.get('alias') or from_model_node.get('name')) if from_model_node else None
        from_column = kwargs.get('column_name') or kwargs.get('field') or kwargs.get('column')
        rows.append({'FromModel': from_model,'FromColumn': from_column,'ToModel': to_model,'ToColumn': to_field,'TestName': tname})
    return rows

def guess_fact_groups(rel_rows, model_kinds):
    from collections import defaultdict
    fact_to_dims = defaultdict(set)
    for r in rel_rows:
        frm = (r.get('FromModel') or '').lower(); to = (r.get('ToModel') or '').lower()
        if not frm or not to: continue
        frm_kind = model_kinds.get(frm, 'Other'); to_kind = model_kinds.get(to, 'Other')
        if frm_kind == 'Fact' and to_kind in {'Dimension','Other'}: fact_to_dims[frm].add(to)
        elif to_kind == 'Fact' and frm_kind in {'Dimension','Other'}: fact_to_dims[to].add(frm)
    return {k: sorted(v) for k, v in fact_to_dims.items()}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest', required=True, type=Path)
    parser.add_argument('--catalog', required=True, type=Path)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--include-views', action='store_true', help='Kept for convenience when restricting with --materializations')
    parser.add_argument('--materializations', type=str, default='', help='Comma-separated list. If provided, only include these mats. Default is all except ephemeral.')
    args = parser.parse_args()

    manifest = load_json(args.manifest); catalog = load_json(args.catalog)
    nodes = manifest.get('nodes', {}); models = [n for n in nodes.values() if node_is_model(n)]

    # Build allowed materializations set
    mats_arg = [m.strip().lower() for m in args.materializations.split(',') if m.strip()]

    filtered = []
    for n in models:
        mat = ((n.get('config') or {}).get('materialized') or '').lower()
        if mats_arg:
            # User provided a whitelist
            if mat in mats_arg or (args.include-views and mat == 'view'):
                filtered.append(n)
        else:
            # Default: include all except ephemeral
            if mat != 'ephemeral':
                filtered.append(n)

    model_kinds = {}; name_to_node = {}
    for n in filtered:
        alias = (n.get('alias') or n.get('name') or '').lower()
        model_kinds[alias] = classify_model_kind(alias, n.get('tags', []))
        name_to_node[alias] = n

    rel_rows = build_relationship_rows(manifest); fact_groups = guess_fact_groups(rel_rows, model_kinds)

    summary_rows = []
    for n in filtered:
        db, schema, alias = model_relation_identifiers(n)
        mat = (n.get('config') or {}).get('materialized')
        fqn = '.'.join(n.get('fqn') or [])
        tags = ','.join(n.get('tags') or [])
        desc = n.get('description') or ''
        kind = classify_model_kind(alias, n.get('tags', []))
        summary_rows.append({'Model': alias,'Kind': kind,'Materialization': mat,'Database': db,'Schema': schema,
                             'Relation': f'{db}.{schema}.{alias}','Path': n.get('path'),'FQN': fqn,'Tags': tags,'Description': desc})
    summary_cols = ['Model','Kind','Materialization','Database','Schema','Relation','Path','FQN','Tags','Description']
    df_summary = pd.DataFrame(summary_rows, columns=summary_cols)
    if not df_summary.empty: df_summary = df_summary.sort_values(['Kind','Model'])

    df_rel = pd.DataFrame(rel_rows) if rel_rows else pd.DataFrame(columns=['FromModel','FromColumn','ToModel','ToColumn','TestName'])

    star_rows = []
    for fact, dims in fact_groups.items():
        if not dims: star_rows.append({'FactModel': fact,'DimensionModel': None})
        else:
            for d in dims: star_rows.append({'FactModel': fact,'DimensionModel': d})
    df_star = pd.DataFrame(star_rows) if star_rows else pd.DataFrame(columns=['FactModel','DimensionModel'])

    used_sheet_names = set()
    with pd.ExcelWriter(args.out, engine='openpyxl') as writer:
        df_summary.to_excel(writer, index=False, sheet_name='Summary')
        if not df_rel.empty: df_rel.to_excel(writer, index=False, sheet_name='Relationships')
        if not df_star.empty: df_star.to_excel(writer, index=False, sheet_name='Star Map')
        for n in filtered:
            db, schema, alias = model_relation_identifiers(n)
            sheet_name = safe_sheet_name(alias or 'Model', used_sheet_names)
            col_tests = extract_tests_for_model(manifest, n.get('unique_id')); pkfk_flags = infer_pk_fk(col_tests)
            columns = get_catalog_columns_for_model(catalog, n)

            rows = []
            if columns:
                ordered = sorted(columns.items(), key=lambda kv: (kv[1].get('index') or 0))
                for col, meta in ordered:
                    dtype = meta.get('type') or ''; comment = meta.get('comment') or ''
                    tests = col_tests.get(col) or []; test_list = ', '.join(sorted({t['name'] for t in tests})) if tests else ''
                    is_pk = pkfk_flags.get(col, {}).get('is_pk', False); is_fk = pkfk_flags.get(col, {}).get('is_fk', False)
                    rows.append({'Column': col,'DataType': dtype,'Description': comment,'Tests': test_list,'IsPK?': 'Y' if is_pk else '','IsFK?': 'Y' if is_fk else ''})
            else:
                manifest_cols = (n.get('columns') or {})
                for col, meta in manifest_cols.items():
                    desc = meta.get('description') or ''
                    tests = col_tests.get(col) or []; test_list = ', '.join(sorted({t['name'] for t in tests})) if tests else ''
                    is_pk = pkfk_flags.get(col, {}).get('is_pk', False); is_fk = pkfk_flags.get(col, {}).get('is_fk', False)
                    rows.append({'Column': col,'DataType': '','Description': desc,'Tests': test_list,'IsPK?': 'Y' if is_pk else '','IsFK?': 'Y' if is_fk else ''})

            df = pd.DataFrame(rows, columns=['Column','DataType','Description','Tests','IsPK?','IsFK?'])
            info_rows = [
                {'Column': '#Model','DataType': alias,'Description': '','Tests': '','IsPK?': '','IsFK?': ''},
                {'Column': '#Kind','DataType': classify_model_kind(alias, n.get('tags', [])),'Description': '','Tests': '','IsPK?': '','IsFK?': ''},
                {'Column': '#Materialization','DataType': (n.get('config') or {}).get('materialized'),'Description': '','Tests': '','IsPK?': '','IsFK?': ''},
                {'Column': '#Relation','DataType': f'{db}.{schema}.{alias}','Description': '','Tests': '','IsPK?': '','IsFK?': ''},
                {'Column': '#Tags','DataType': ','.join(n.get('tags') or []),'Description': '','Tests': '','IsPK?': '','IsFK?': ''},
                {'Column': '#Description','DataType': (n.get('description') or ''),'Description': '','Tests': '','IsPK?': '','IsFK?': ''},
            ]
            pd.concat([pd.DataFrame(info_rows), df], ignore_index=True).to_excel(writer, index=False, sheet_name=sheet_name)

    print(f'Wrote: {args.out}')

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'ERROR: {e}', file=sys.stderr); sys.exit(1)
