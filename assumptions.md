# Project assumptions (useful for other coders)

## API & network

1. **Base API (`/api/users`) is paginated** — responses include `page` and `total_pages`.
   _Why:_ pipeline fetches page 1 first to learn `total_pages`, then iterates pages.

2. **API returns JSON** for successful (200) responses. Invalid JSON is treated as error.
   _Why:_ processor expects JSON objects; invalid responses raise `APIClientError`.

3. **API key is optional** — the Reqres demo API does not require auth; client accepts an `api_key` for real services.
   _Why:_ keeps client reusable for both public demo and real APIs.

4. **Network failures and server errors are transient** (retryable). Client retries on `requests.RequestException` and 5xx responses.
   _Why:_ mirrors real-world unreliable third-party APIs.

5. **Rate limit (429) respects `Retry-After` header** if present; otherwise use exponential backoff.
   _Why:_ polite behavior and prevents immediate retries that cause more 429s.

6. **Timeouts are applied to HTTP requests** (10s default). Long-hanging requests are treated as failures and retried.

---

## Retry / backoff behavior

7. **Retry strategy is fixed**: max 3 attempts with backoff intervals 1s, 2s, 4s. Tests patch `time.sleep` to avoid slowing test runs.
   _Why:_ simple, predictable retry policy that’s easy to change centrally.

8. **Tests mock network & sleep** — unit/integration tests do not hit the real `reqres.in` and do not actually wait for backoff.
   _Why:_ deterministic, fast tests suitable for CI.

---

## Data model & transformation

9. **Client returns raw records (no deduplication)** — the client only fetches and returns all raw items. Deduplication happens in the processor.
   _Why:_ the processor computes `data_quality_score` and should decide which duplicate is better.

10. **Processor is the single source of truth for dedupe** — duplicates are resolved by keeping the record with the **highest `data_quality_score`**. If tie, first-seen wins.
    _Why:_ meets business requirement to preserve best-quality data.

11. **`data_quality_score` calculation is simple**: start at 100 and deduct 10 points per missing key field (name/email).
    _Why:_ deterministic and easy to reason about; can be replaced with a richer scoring algorithm later.

12. **Random enrichment fields** (`engagement_level`, `activity_status`, `acquisition_channel`, `market_segment`, `customer_tier`) are generated randomly for demo purposes and **tests patch random.choice** to make behavior deterministic.
    _Why:_ avoids coupling to unavailable attributes while allowing realistic-looking data.

13. **Malformed emails** result in `email_domain: "unknown"` and a quality deduction. Email extraction uses a conservative regex that requires a TLD.
    _Why:_ avoids false extraction on bad input.

14. **Missing `id` behavior:** processor must handle records missing `id` (either drop or assign sentinel). If you keep such records, make a clear policy and adjust `ProcessedCustomer` validation accordingly.
    _Why:_ Pydantic model expects `customer_id: int`; undefined `id` would break validation otherwise.

---

## Export / output

15. **Output is JSON with `metadata` and `customers`**. Metadata includes `total_customers`, `export_timestamp` (UTC), and `data_quality_summary` with buckets `high_quality`, `medium_quality`, `low_quality`.
    _Why:_ simple, stable structure that analytics jobs can parse easily.

16. **Customers are sorted by `full_name` (case-insensitive)** before export. Missing names should sort to the top/bottom predictably (we use empty-string fallback).
    _Why:_ consistent exports help diffs and deterministic downstream processing.

17. **Timestamp format** is ISO-8601 UTC. If you require a trailing `Z` (instead of `+00:00`), normalize `datetime(...).isoformat().replace("+00:00", "Z")`.

---

## Logging, errors & monitoring

18. **Errors carry context**: `APIClientError` includes `url`, `status_code`, and `retries` where possible to improve observability.
    _Why:_ easier debugging in logs and tests.

19. **Pipeline continues fail-fast at orchestration layer**: `run_pipeline()` logs errors and exits cleanly on fatal failures (API or export errors).
    _Why:_ avoid partial or inconsistent exports.

20. **All classes are single-responsibility**:

- `CustomerAPIClient` → fetching + retry
- `CustomerDataProcessor` → transform + dedupe + enrichment
- `DataExporter` → sorting + metadata + writing

---

## Testing assumptions

21. **No real external network calls in tests** — `requests.Session.get` is patched in tests.
    _Why:_ CI-safe and deterministic.

22. **Randomness is patched in tests** — `random.choice` is replaced with a deterministic lambda so enrichment is predictable.
    _Why:_ enables assertions on enrichment fields if needed.

23. **Tests expect client to return raw duplicates** — update any test that previously asserted client-side dedupe.

---

## Operational / deploy assumptions

24. **Python 3.9+** (typing features, pydantic compatibility). Pin exact versions in `requirements.txt` when releasing.
25. **Small dataset expectation** — pipeline currently processes whole dataset in memory (not chunked). For large datasets, implement streaming/chunking and connection pooling.

---

## Suggestions for future teams (extension notes)

- Convert to **async** (`aiohttp` + `asyncio`) for faster concurrent page fetches (bonus).
- Add optional **caching** (Redis or file) to avoid repeat fetches in dev/CI.
- Replace random enrichment with **real metrics** (last_active timestamps, pageviews) and compute `engagement_level` deterministically.
- Add **observability**: metrics (requests count, success/fail), tracing, and alerting for persistent 5xx/429 patterns.
- Add schema validation step (Pydantic) in the processor to ensure exported data matches `ProcessedCustomer`.
