"""Automated validation checks for raw outputs and operational state.

Each check is an assertion computed from the artifacts themselves; nothing
(counts, ids, hashes) is hard-coded. Exits non-zero if any check fails.
"""
from pathlib import Path
from collections import Counter
from datetime import datetime
import hashlib, json, sys

ROOT = Path(__file__).resolve().parents[1]
RAW_FILES = ROOT / 'raw' / 'files'
MANIFEST = RAW_FILES / 'manifest.jsonl'
EVENTS = ROOT / 'raw' / 'api' / 'events.jsonl'
WATERMARK = ROOT / 'state' / 'api_watermark.json'

EXPECTED_FILES = ['customers.csv', 'orders.json', 'products.parquet']
MANIFEST_FIELDS = {'source_file', 'ingested_at', 'bytes', 'sha256'}

failures = []


def check(name, condition, detail=''):
    status = 'PASS' if condition else 'FAIL'
    print(f'[{status}] {name}' + (f' - {detail}' if detail else ''))
    if not condition:
        failures.append(name)


def sha256_file(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def main():
    # --- raw files + manifest ---
    for name in EXPECTED_FILES:
        check(f'raw file exists: {name}', (RAW_FILES / name).exists())
    check('manifest exists', MANIFEST.exists())

    if MANIFEST.exists():
        entries = [json.loads(l) for l in MANIFEST.read_text(encoding='utf-8').splitlines() if l]
        check('manifest entries carry all required fields',
              all(MANIFEST_FIELDS <= set(e) for e in entries),
              f'{len(entries)} entries')
        check('manifest ingested_at values parse as ISO-8601 UTC',
              all(datetime.fromisoformat(e['ingested_at']).tzinfo is not None for e in entries))
        hash_counts = Counter(e['sha256'] for e in entries)
        check('no duplicate content hash ingested twice',
              all(n == 1 for n in hash_counts.values()))
        for name in EXPECTED_FILES:
            p = RAW_FILES / name
            if p.exists():
                check(f'raw copy of {name} matches a manifest sha256',
                      sha256_file(p) in hash_counts)

    # --- API raw output ---
    check('events.jsonl exists', EVENTS.exists())
    if EVENTS.exists():
        events = [json.loads(l) for l in EVENTS.read_text(encoding='utf-8').splitlines() if l]
        check('events.jsonl is non-empty', len(events) > 0, f'{len(events)} records')
        id_counts = Counter(e['event_id'] for e in events)
        dupes = [k for k, n in id_counts.items() if n > 1]
        check('event_id unique across raw API records', not dupes,
              f'duplicates: {dupes}' if dupes else f'{len(id_counts)} distinct ids')
        check('every record has _ingested_at and _source',
              all('_ingested_at' in e and '_source' in e for e in events))
        parseable = True
        for e in events:
            try:
                datetime.fromisoformat(e['updated_at'])
            except (KeyError, ValueError):
                parseable = False
        check('every updated_at parses as ISO-8601', parseable)

        check('watermark file exists', WATERMARK.exists())
        if WATERMARK.exists() and events and parseable:
            wm = json.loads(WATERMARK.read_text())['updated_at']
            max_updated = max(e['updated_at'] for e in events)
            check('watermark equals max(updated_at) of persisted records',
                  wm == max_updated, f'watermark={wm}, max={max_updated}')

    print()
    if failures:
        print(f'VALIDATION FAILED: {len(failures)} check(s) failed: {failures}')
        sys.exit(1)
    print('VALIDATION PASSED: all checks succeeded')


if __name__ == '__main__':
    main()
