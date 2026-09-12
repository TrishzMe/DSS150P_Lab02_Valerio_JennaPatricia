"""Week 3: rerunnable ingestion to a raw area.

File ingestion (SHA-256 manifest) + paginated REST API ingestion with
event_id deduplication, a persisted updated_at watermark, atomic writes,
and a per-run operational log. Safe to rerun: unchanged inputs produce
no duplicate logical records.
"""
from pathlib import Path
from datetime import datetime, timezone
import csv, json, hashlib, os, shutil, sys, tempfile, uuid

import requests

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data'
RAW = ROOT / 'raw'
STATE = ROOT / 'state'
OUTPUTS = ROOT / 'outputs'
API_URL = 'http://127.0.0.1:8000/api/events'
RUN_LOG = OUTPUTS / 'pipeline_run_log.csv'
RUN_LOG_HEADER = ROOT / 'templates' / 'pipeline_run_log_template.csv'

SOURCE_FILES = ['customers.csv', 'orders.json', 'products.parquet']


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def atomic_write_text(path, text):
    """Write to a temp file in the same directory, then atomically replace the target."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as f:
            f.write(text)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


# ---------- operational state ----------

def load_watermark():
    p = STATE / 'api_watermark.json'
    if not p.exists():
        return None
    return json.loads(p.read_text())['updated_at']


def save_watermark(value):
    atomic_write_text(STATE / 'api_watermark.json', json.dumps({'updated_at': value}, indent=2))


def log_run(row):
    """Append one run-log row, creating the file from the template header if needed."""
    OUTPUTS.mkdir(exist_ok=True)
    header = RUN_LOG_HEADER.read_text(encoding='utf-8').strip().split(',')
    new_file = not RUN_LOG.exists()
    with RUN_LOG.open('a', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=header)
        if new_file:
            w.writeheader()
        w.writerow(row)


# ---------- Task 3.1: file ingestion ----------

def load_manifest(manifest_path):
    if not manifest_path.exists():
        return []
    return [json.loads(line) for line in manifest_path.read_text(encoding='utf-8').splitlines() if line]


def ingest_files(run_id):
    started = utc_now()
    dest_dir = RAW / 'files'
    dest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = dest_dir / 'manifest.jsonl'
    known_hashes = {e['sha256'] for e in load_manifest(manifest_path)}

    read = written = skipped = 0
    for name in SOURCE_FILES:
        src = DATA / name
        read += 1
        digest = sha256_file(src)
        if digest in known_hashes:
            skipped += 1
            print(f'[files] skip {name}: content hash already ingested')
            continue
        shutil.copy2(src, dest_dir / name)
        entry = {'source_file': name, 'ingested_at': utc_now(),
                 'bytes': src.stat().st_size, 'sha256': digest}
        with manifest_path.open('a', encoding='utf-8', newline='\n') as f:
            f.write(json.dumps(entry) + '\n')
        known_hashes.add(digest)
        written += 1
        print(f'[files] ingested {name} ({entry["bytes"]} bytes, sha256={digest[:12]}...)')

    log_run({'run_id': run_id, 'started_at': started, 'finished_at': utc_now(),
             'status': 'SUCCESS', 'source': 'files',
             'records_read': read, 'records_written': written,
             'duplicates_removed': skipped,
             'watermark_before': '', 'watermark_after': '', 'error_message': ''})
    print(f'[files] done: {read} sources checked, {written} copied, {skipped} skipped as duplicates')


# ---------- Tasks 3.2-3.4: paginated API ingestion with dedup + watermark ----------

def fetch_api_page(page, per_page=20, updated_after=None):
    params = {'page': page, 'per_page': per_page}
    if updated_after:
        params['updated_after'] = updated_after
    r = requests.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_all_events(updated_after):
    """Follow pagination until has_more is false; never assume one page is enough."""
    records, page = [], 1
    while True:
        payload = fetch_api_page(page, per_page=20, updated_after=updated_after)
        ingested_at = utc_now()
        for item in payload['items']:
            item = dict(item)
            item['_ingested_at'] = ingested_at
            item['_source'] = API_URL
            records.append(item)
        print(f'[api] page {page}: {len(payload["items"])} records (has_more={payload["has_more"]})')
        if not payload['has_more']:
            return records
        page = payload['next_page']


def dedupe_events(records):
    """One logical record per event_id, keeping the greatest updated_at.

    Deterministic tie-break: on equal updated_at, the later-ingested record wins.
    No duplicate ids are known in advance; everything is derived from the data.
    """
    best = {}
    for rec in records:
        key = rec['event_id']
        rank = (rec['updated_at'], rec.get('_ingested_at', ''))
        if key not in best or rank > (best[key]['updated_at'], best[key].get('_ingested_at', '')):
            best[key] = rec
    return sorted(best.values(), key=lambda r: (r['updated_at'], r['event_id']))


def load_existing_events(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines() if line]


def ingest_api(run_id):
    started = utc_now()
    watermark_before = load_watermark()
    out_path = RAW / 'api' / 'events.jsonl'
    try:
        fetched = fetch_all_events(updated_after=watermark_before)
        existing = load_existing_events(out_path)
        merged = existing + fetched
        deduped = dedupe_events(merged)
        duplicates_removed = len(merged) - len(deduped)

        # durable write first...
        atomic_write_text(out_path, ''.join(json.dumps(r) + '\n' for r in deduped))
        # ...and only then advance the watermark
        watermark_after = max((r['updated_at'] for r in deduped), default=watermark_before)
        if watermark_after:
            save_watermark(watermark_after)

        log_run({'run_id': run_id, 'started_at': started, 'finished_at': utc_now(),
                 'status': 'SUCCESS', 'source': 'api_events',
                 'records_read': len(fetched), 'records_written': len(deduped),
                 'duplicates_removed': duplicates_removed,
                 'watermark_before': watermark_before or '',
                 'watermark_after': watermark_after or '', 'error_message': ''})
        print(f'[api] done: fetched {len(fetched)} new, wrote {len(deduped)} logical records, '
              f'removed {duplicates_removed} duplicates')
        print(f'[api] watermark: {watermark_before!r} -> {watermark_after!r}')
    except requests.exceptions.RequestException as exc:
        msg = f'API ingestion failed, watermark NOT advanced: {exc}'
        log_run({'run_id': run_id, 'started_at': started, 'finished_at': utc_now(),
                 'status': 'FAILED', 'source': 'api_events',
                 'records_read': 0, 'records_written': 0, 'duplicates_removed': 0,
                 'watermark_before': watermark_before or '',
                 'watermark_after': watermark_before or '', 'error_message': str(exc)})
        print(f'[api] ERROR: {msg}', file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    RAW.mkdir(exist_ok=True)
    STATE.mkdir(exist_ok=True)
    run_id = uuid.uuid4().hex[:12]
    print(f'run_id={run_id}')
    ingest_files(run_id)
    ingest_api(run_id)
