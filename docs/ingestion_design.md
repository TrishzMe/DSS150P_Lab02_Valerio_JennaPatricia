# Ingestion Design (Task 2.4) and Watermark Semantics (Task 2.5)

## Task 2.4 - Ingestion design (defined before coding)

| Source | Method | Raw destination | Duplicate key | Incremental state |
|---|---|---|---|---|
| customers.csv, orders.json, products.parquet | File copy + manifest | `raw/files/` | File SHA-256 | N/A (full snapshot each delivery) |
| REST API `/api/events` | Paginated GET (`page`, `per_page`, follow `has_more`/`next_page`), `updated_after` when a watermark exists | `raw/api/events.jsonl` | `event_id`, keeping greatest `updated_at` | `max(updated_at)` persisted in `state/api_watermark.json` |
| PostgreSQL `support_tickets` | Inspection only in this lab (bounded queries) | N/A | `ticket_id` | Future: incremental on a last-updated timestamp column, or CDC (logical replication) so extraction never rescans the OLTP table |

Design decisions:

- **File ingestion is content-addressed.** The manifest (`raw/files/manifest.jsonl`) records
  source file name, UTC ingestion timestamp, byte size, and SHA-256. A file is copied only
  when its hash is not already in the manifest, so unchanged reruns write nothing and a
  changed source file is ingested as new evidence.
- **API ingestion merges before writing.** Existing records in `raw/api/events.jsonl` are
  loaded, combined with newly fetched records, deduplicated by `event_id` keeping the
  greatest `updated_at` (ties broken deterministically by `_ingested_at`), and the whole
  file is rewritten atomically (temp file + `os.replace`). This keeps one logical record
  per event across any number of runs.
- **Run log.** Every execution appends one row per source to `outputs/pipeline_run_log.csv`
  using the header in `templates/pipeline_run_log_template.csv`, including failures.
- **Credentials** stay in the environment / compose file, never in source code or Git.

## Task 2.5 - Watermark semantics

The watermark is the greatest `updated_at` that was **successfully persisted** to the raw
area. On the next run it is sent as `updated_after`, so the source only returns newer
records. It is operational state about the pipeline's progress — not source data — which
is why it lives in `state/`, outside the raw area, and is not committed to Git.

**What could go wrong if the watermark is saved before the raw file is written?**
If the process crashes between saving the watermark and writing the raw output, the
records of that run are lost permanently: the next run asks only for records newer than a
watermark that was never actually persisted, so the source never re-sends the lost
records. Data loss becomes silent and unrecoverable from state alone. Advancing the
watermark only after a durable write makes the failure mode harmless: the rerun re-fetches
the same records and deduplication discards the repeats.

**What could go wrong when multiple records share exactly the same timestamp?**
With a strict `updated_after > watermark` filter, suppose records A and B both have
`updated_at = T`, but only A had been delivered when our run executed. The watermark
advances to T; on the next run the source excludes everything `<= T`, so B is skipped
forever. The same risk appears when a page boundary or crash splits a group of same-
timestamp records across runs.

**One limitation and one production-grade mitigation.**
Limitation: a timestamp is not a unique, monotonic position in the stream — it cannot
distinguish two records written in the same instant, and it trusts the source clock.
Mitigation: use a composite cursor such as `(updated_at, event_id)` keyset pagination, or
a monotonically increasing change sequence (log sequence number / CDC offset), and/or
re-read with an overlap window (`updated_after = watermark - epsilon`) relying on
key-based deduplication to absorb the overlap.
