# E-commerce Analytics — Customer Data Pipeline

## Overview

This repository contains a small, production-oriented data synchronization pipeline that fetches customer records from a paginated third-party API (example: `https://reqres.in/api/users`), enriches and standardizes them for analytics, and exports the cleaned dataset to JSON. The system demonstrates robust error handling (retries, backoff, rate-limit handling), data quality scoring, deduplication by highest quality, and clear logging.

## Project structure

```

customer_data_pipeline/
├── src/
│ ├── api_client.py # CustomerAPIClient
│ ├── data_processor.py # CustomerDataProcessor
│ ├── exporter.py # DataExporter
│ ├── models.py # Pydantic models
│ └── main.py # run_pipeline orchestration
├── tests/
│ ├── test_api_client.py
│ ├── test_data_processor.py
│ └── test_integration.py
├── requirements.txt
├── README.md
└── sample_output.json

```

## Requirements

- Python 3.9+ (3.10+ recommended)
- Install dependencies:

```bash
python -m pip install -r requirements.txt
```

## Usage

Run the pipeline (fetch → process → export):

```bash
python -m src.main
```

By default `main.py` writes `sample_output.json` at the repository root (see `main.py` for exact path). You can also programmatically call `run_pipeline(base_url, output_file)`.

Example (programmatic):

```python
from src.main import run_pipeline
run_pipeline(base_url="https://reqres.in/api", output_file="out.json")
```

## Tests

Run the unit & integration tests with pytest:

```bash
pytest -q
```

Notes for tests:

- Network calls in tests are mocked (no external HTTP traffic).
- Tests patch `time.sleep` so retry/backoff tests run quickly.

## Design decisions & assumptions

- **Separation of concerns**: `CustomerAPIClient` is responsible for fetching raw pages with retries and rate-limit handling. It **does not** deduplicate so the processor can choose the highest-quality entry.
- **Final deduplication** is performed in `CustomerDataProcessor` using `data_quality_score` (keeps highest score).
- **Data quality scoring**: start at 100, deduct 10 points per missing key field (name/email). This is simple and easy to extend.
- **Random enrichment**: fields like `engagement_level`, `activity_status`, `acquisition_channel`, `market_segment`, and `customer_tier` are generated randomly for the purposes of this challenge. Tests patch randomness to be deterministic.
- **Export format**: JSON with `metadata` (total, timestamp, quality summary) and `customers` (sorted by `full_name` ascending).

## Troubleshooting

- If tests fail due to timing in retry tests, ensure `time.sleep` is being patched in tests (tests included already mock it).
- If running behind a corporate proxy or with limited network, tests are safe (they mock the HTTP layer).

## Extending

- Add async fetching with `aiohttp` for performance (bonus).
- Add Redis or an on-disk cache to avoid repeated fetching during development.
- Use real business rules (engagement scoring based on activity timestamps) instead of random enrichment.

Notes (for the output.json):

- `Dana Doe` has `email_domain: "unknown"` and a low score to illustrate quality buckets.
- The customer list is sorted by `full_name`.
- Adapt the timestamp to the exact export time if you run the pipeline.
