"""Week 2: profile CSV, JSON, and Parquet sources.

Profiles are computed from the data itself; no row counts, duplicate IDs,
or specific source errors are hard-coded. Source files are never modified.
"""
from pathlib import Path
from collections import Counter
from datetime import datetime
import json, csv

import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / 'data'


# ---------- helpers ----------

def infer_logical_type(values):
    """Infer a logical type from non-empty string values: integer, decimal, date, datetime, or string."""
    non_empty = [v for v in values if v not in ('', None)]
    if not non_empty:
        return 'unknown (all missing)'
    checks = {'integer': True, 'decimal': True, 'date': True, 'datetime': True}
    for v in non_empty:
        if checks['integer']:
            try:
                int(v)
            except ValueError:
                checks['integer'] = False
        if checks['decimal']:
            try:
                float(v)
            except ValueError:
                checks['decimal'] = False
        if checks['date'] or checks['datetime']:
            try:
                parsed = datetime.fromisoformat(v)
                if parsed.hour or parsed.minute or parsed.second or 'T' in v:
                    checks['date'] = False
                else:
                    checks['datetime'] = False
            except ValueError:
                checks['date'] = checks['datetime'] = False
    for logical in ('integer', 'decimal', 'date', 'datetime'):
        if checks[logical]:
            return logical
    return 'string'


def header(title):
    print()
    print('=' * 70)
    print(title)
    print('=' * 70)


# ---------- Task 1.2: customers.csv ----------

def profile_csv(path):
    header(f'PROFILE: {path.name}')
    print(f'File size: {path.stat().st_size:,} bytes')

    with path.open(newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    columns = list(rows[0].keys())
    print(f'Rows: {len(rows)}  Columns: {len(columns)}')

    print('\nColumn -> inferred logical type / missing count:')
    for col in columns:
        values = [r[col] for r in rows]
        missing = sum(1 for v in values if v in ('', None))
        print(f'  {col:18s} {infer_logical_type(values):22s} missing={missing}')

    # exact duplicate rows (all columns identical)
    row_counts = Counter(tuple(r[c] for c in columns) for r in rows)
    dup_groups = {k: n for k, n in row_counts.items() if n > 1}
    extra_copies = sum(n - 1 for n in dup_groups.values())
    print(f'\nExact duplicate rows: {extra_copies} extra copies across {len(dup_groups)} duplicated row(s)')
    for key, n in dup_groups.items():
        print(f'  x{n}: {key}')

    # customer_id uniqueness
    id_counts = Counter(r['customer_id'] for r in rows)
    dup_ids = {k: n for k, n in id_counts.items() if n > 1}
    unique = not dup_ids
    print(f'\ncustomer_id unique: {unique}')
    if dup_ids:
        print(f'  repeated ids: {dict(sorted(dup_ids.items()))}')

    # quick value scans that support validation-rule candidates
    bad_email = sum(1 for r in rows if r['email'] and '@' not in r['email'])
    print(f'email values without "@": {bad_email}')
    print(f'customer_segment values: {sorted({r["customer_segment"] for r in rows if r["customer_segment"]})}')
    print(f'signup_date range: {min((r["signup_date"] for r in rows if r["signup_date"]), default="-")}'
          f' .. {max((r["signup_date"] for r in rows if r["signup_date"]), default="-")}')

    print('\nCandidate validation rules:')
    print('  1. customer_id must be non-null and unique (candidate key).')
    print('  2. email, when present, must contain "@" and a domain.')
    print('  3. signup_date must parse as YYYY-MM-DD and not be in the future.')
    print('  4. customer_segment must belong to the observed controlled vocabulary.')
    print('  5. No exact duplicate rows should be delivered in a single extract.')


# ---------- Task 1.3: orders.json ----------

def profile_json(path):
    header(f'PROFILE: {path.name}')
    print(f'File size: {path.stat().st_size:,} bytes')

    records = json.loads(path.read_text(encoding='utf-8'))
    print(f'Root structure is a list: {isinstance(records, list)}')
    print(f'Record count: {len(records)}')

    # union of top-level keys (records may omit keys)
    all_keys = []
    for r in records:
        for k in r.keys():
            if k not in all_keys:
                all_keys.append(k)
    print(f'Top-level keys: {all_keys}')

    nested = [k for k in all_keys if any(isinstance(r.get(k), dict) for r in records)]
    print(f'Nested object field(s): {nested}')
    for k in nested:
        sub_keys = sorted({sk for r in records if isinstance(r.get(k), dict) for sk in r[k]})
        print(f'  {k} sub-keys: {sub_keys}')

    print('\nKey -> type observed / null or missing count:')
    for k in all_keys:
        types = sorted({type(r[k]).__name__ for r in records if k in r and r[k] is not None})
        nulls = sum(1 for r in records if r.get(k) is None and k in r)
        absent = sum(1 for r in records if k not in r)
        print(f'  {k:18s} types={types}  null={nulls}  missing_key={absent}')

    ts_fields = [k for k in all_keys
                 if any(isinstance(r.get(k), str) and _is_iso_ts(r[k]) for r in records)]
    num_fields = [k for k in all_keys
                  if any(isinstance(r.get(k), (int, float)) and not isinstance(r.get(k), bool) for r in records)]
    print(f'\nTimestamp-like fields: {ts_fields}')
    print(f'Numeric fields: {num_fields}')

    print('\nDownstream options for the nested shipping object:')
    print('  a) Flatten into columns (shipping_region, shipping_method) for relational targets.')
    print('  b) Keep as a JSON/VARIANT column preserving the original structure.')
    print('  c) Normalize into a separate shipping table keyed by order_id.')


def _is_iso_ts(s):
    try:
        datetime.fromisoformat(s)
        return len(s) >= 10
    except (ValueError, TypeError):
        return False


# ---------- Task 1.4: products.parquet ----------

def profile_parquet(path):
    header(f'PROFILE: {path.name}')
    df = pd.read_parquet(path)
    print(f'File size: {path.stat().st_size:,} bytes')
    print(f'Shape: {df.shape[0]} rows x {df.shape[1]} columns')
    print('\nDtypes (declared in the file, not inferred):')
    print(df.dtypes.to_string())
    print('\nNulls per column:')
    print(df.isna().sum().to_string())
    if 'product_id' in df.columns:
        print(f'\nproduct_id unique: {df["product_id"].is_unique}')

    # optional same-data comparison files
    for other in ('products_optional_compare.csv', 'products_optional_compare.json'):
        p = path.parent / other
        if p.exists():
            print(f'{other}: {p.stat().st_size:,} bytes '
                  f'(parquet is {path.stat().st_size / p.stat().st_size:.2f}x its size)')

    csv_path = path.parent / 'products_optional_compare.csv'
    if csv_path.exists():
        df_csv = pd.read_csv(csv_path)
        diff = [(c, str(df[c].dtype), str(df_csv[c].dtype))
                for c in df.columns if c in df_csv.columns and str(df[c].dtype) != str(df_csv[c].dtype)]
        print('\nDtype differences parquet vs CSV re-read (column, parquet, csv):')
        for row in diff:
            print(f'  {row}')

    print('\nSchema behavior: Parquet stores an embedded, typed schema (including')
    print('categorical/dictionary encoding), so dtypes survive a round trip. CSV is')
    print('untyped text that must be re-inferred on every read, and JSON only')
    print('distinguishes string/number/bool/null.')


if __name__ == '__main__':
    profile_csv(DATA_DIR / 'customers.csv')
    profile_json(DATA_DIR / 'orders.json')
    profile_parquet(DATA_DIR / 'products.parquet')
