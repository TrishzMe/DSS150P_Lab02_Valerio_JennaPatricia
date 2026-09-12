# Engineering Reflection — Laboratory Activity #2

Valerio, Jenna Patricia — DSS150P

**1. Why should source profiling occur before implementation of ingestion?**
Profiling replaces assumptions with evidence. Before writing any code I already knew that
`customer_id` is not unique (C0090 is shared by two different people), that the API
re-delivers updated events under the same `event_id`, and that pagination spans 13 pages.
Each of those facts changed the design — the file pipeline cannot key on `customer_id`,
the API pipeline needs deterministic deduplication, and the fetch loop must follow
`has_more`. Discovering these after implementation would have meant silent data loss or a
rewrite.

**2. What is the difference between source event time (`updated_at`) and ingestion time?**
`updated_at` is when the event happened or was last revised *inside the source system*,
on the source's clock. `_ingested_at` is when *my pipeline* persisted the record, in UTC.
They serve different purposes: `updated_at` drives incremental filtering and duplicate
resolution; `_ingested_at` provides lineage and lets me reconstruct what a run saw. They
can differ by minutes or by months (a late-arriving update still gets a fresh ingestion
time).

**3. Why is `event_id` alone insufficient to decide which duplicate record to keep?**
`event_id` only tells me two records describe the same logical event; it says nothing
about which version is current. The API deliberately re-delivers E0020 and E0055 with
newer `updated_at` values. Keeping "whichever arrived first" or an arbitrary
`drop_duplicates()` row could preserve the stale version. The rule must be
key + version: per `event_id`, keep the greatest `updated_at`.

**4. Why must watermark state advance only after successful persistence?**
The watermark is a promise: "everything up to T is safely on disk." If it advances before
the write and the write fails, the next run asks the source only for records newer than T
and the lost records are never re-sent — silent, unrecoverable data loss. Advancing after
the write makes failure harmless: the rerun re-fetches the same window and deduplication
absorbs the repeats. My failure experiment shows exactly this — the crashed run left the
watermark untouched and the recovery run completed cleanly.

**5. What limitation does `updated_after > watermark` have with identical timestamps?**
If several records share `updated_at = T` and only some were delivered before the
watermark advanced to T, the strict `>` filter excludes the rest forever. A timestamp is
not a unique position in the change stream. Mitigations: a composite cursor
`(updated_at, event_id)`, a monotonic change sequence (CDC/LSN), or re-reading with an
overlap window and relying on key-based dedup.

**6. How is duplicate prevention related to idempotency?**
Idempotency means a rerun with unchanged inputs leaves the same logical result — and
duplicate prevention is the mechanism that makes it true. My pipeline is built from two
dedup layers: file content hashes stop the same bytes from being copied twice, and the
`event_id`/max-`updated_at` rule stops re-fetched events from becoming extra rows.
Without them, every rerun would append and the raw area would grow with copies.

**7. Why should the raw area preserve source values instead of applying business transformations?**
The raw area is the durable evidence of what the source actually said. If I "fixed" the
C0090 collision at ingestion time, the downstream would never know the source has a key
integrity problem, the fix could not be revisited when the business rule changes, and a
bug in the cleaning logic would corrupt the only copy. Preserving source values lets
cleaning be reapplied, audited, and improved later; transformations belong to subsequent
lifecycle stages.

**8. How could querying a production OLTP source for profiling or extraction degrade the application?**
Full-table scans and analytical aggregates compete with transaction processing for CPU,
I/O, buffer cache, and locks. Long-running reads can bloat undo/VACUUM work, hold back
cleanup, and evict the hot working set, so checkout-style queries suddenly become slow.
That is why this lab restricted PostgreSQL work to bounded queries (`LIMIT`, targeted
counts) and why production extraction should use replicas, snapshots, or CDC.

**9. What would I change if the API had a rate limit of 60 requests per minute?**
Raise `per_page` to the maximum (50) to cut the number of requests, add client-side
throttling to stay under one request per second, honor `429`/`Retry-After` with
exponential backoff and jitter, and persist per-page progress so a run interrupted by the
limit resumes instead of restarting. The run log would also record request counts to
watch the budget.

**10. How would I extend this pipeline to load PostgreSQL while preserving rerun safety?**
Load the deduplicated records into a staging table, then apply an upsert keyed on the
logical id (`INSERT ... ON CONFLICT (event_id) DO UPDATE` guarded by
`EXCLUDED.updated_at > current.updated_at`), all inside one transaction that also updates
a watermark table. Commit makes both the data and the state advance atomically; a rerun
re-upserts the same rows and changes nothing — the database equivalent of the
atomic-write-then-watermark pattern used here.
