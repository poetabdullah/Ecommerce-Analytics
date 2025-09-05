import json
from unittest.mock import MagicMock, patch

import pytest
import requests
import random

from src.data_processor import CustomerDataProcessor
from src.exporter import DataExporter
from src.api_client import CustomerAPIClient
from src.main import run_pipeline

BASE_URL = "https://reqres.in/api"


def make_response(status_code=200, json_data=None, headers=None, text=""):
    # Create a mock requests.Response-like object (MagicMock).
    mock_resp = MagicMock(spec=requests.Response)
    mock_resp.status_code = status_code
    mock_resp.headers = headers or {}
    mock_resp.text = text
    if json_data is not None:
        mock_resp.json.return_value = json_data
    else:
        mock_resp.json.side_effect = ValueError("Invalid JSON")
    return mock_resp


@patch("requests.Session.get")
def test_integration_api_to_processor(mock_get, monkeypatch):
    """
    Integration: CustomerAPIClient (fetch all pages) -> CustomerDataProcessor (transform + dedupe by quality).
    The client returns ALL raw records (no dedupe); processor should keep the highest-quality duplicate.
    """
    # Make random deterministic
    monkeypatch.setattr(random, "choice", lambda choices: choices[0])

    # Page 1 contains id=1 and id=2 (id=2 missing email -> lower quality)
    page1 = {
        "page": 1,
        "total_pages": 2,
        "data": [
            {"id": 1, "first_name": "Alice", "last_name": "A", "email": "alice@aa.com"},
            {"id": 2, "first_name": "Bob", "last_name": "B", "email": ""},
        ],
    }
    # Page 2 contains a better duplicate of id=2 and id=3
    page2 = {
        "page": 2,
        "total_pages": 2,
        "data": [
            {"id": 2, "first_name": "Bob", "last_name": "B", "email": "bob@bb.com"},
            {"id": 3, "first_name": "", "last_name": "", "email": "charlie@cc.com"},
        ],
    }

    mock_get.side_effect = [make_response(200, page1), make_response(200, page2)]

    client = CustomerAPIClient(BASE_URL)
    raw = client.fetch_all_customers()

    # Because client no longer dedupes, raw should include all 4 items
    assert len(raw) == 4

    processor = CustomerDataProcessor()
    processed = processor.process_customers(raw)

    # Processor should dedupe by quality and keep ids 1,2,3
    processed_ids = {c["customer_id"] for c in processed}
    assert processed_ids == {1, 2, 3}

    # For id=2 ensure the higher-quality record (with email) was kept
    rec2 = next(c for c in processed if c["customer_id"] == 2)
    assert rec2["email_domain"] == "bb.com"
    assert rec2["data_quality_score"] == 100

    # id=3 missing names -> quality reduced
    rec3 = next(c for c in processed if c["customer_id"] == 3)
    assert rec3["full_name"] == "Unknown"
    assert rec3["data_quality_score"] <= 90


def test_integration_processor_to_exporter(tmp_path):
    """
    Integration: Processor -> Exporter.
    Validate JSON structure, sorting, and quality buckets.
    """
    processed_customers = [
        {
            "customer_id": 3,
            "full_name": "Zara Zee",
            "email_domain": "z.com",
            "engagement_level": "high",
            "activity_status": "active",
            "acquisition_channel": "website",
            "market_segment": "US-West",
            "customer_tier": "premium",
            "data_quality_score": 95,
        },
        {
            "customer_id": 1,
            "full_name": "Alice Alpha",
            "email_domain": "a.com",
            "engagement_level": "medium",
            "activity_status": "inactive",
            "acquisition_channel": "mobile_app",
            "market_segment": "EU-Central",
            "customer_tier": "basic",
            "data_quality_score": 85,
        },
        {
            "customer_id": 2,
            "full_name": "Bob Beta",
            "email_domain": "b.com",
            "engagement_level": "low",
            "activity_status": "active",
            "acquisition_channel": "email_campaign",
            "market_segment": "APAC",
            "customer_tier": "enterprise",
            "data_quality_score": 65,
        },
    ]

    out_file = tmp_path / "out.json"
    exporter = DataExporter()
    exporter.export_customers(processed_customers, str(out_file))

    payload = json.loads(out_file.read_text(encoding="utf-8"))
    assert "metadata" in payload and "customers" in payload

    # Sorted by name
    names = [c["full_name"] for c in payload["customers"]]
    assert names == sorted(names, key=lambda s: s.lower())

    # Metadata checks
    assert payload["metadata"]["total_customers"] == 3
    counts = payload["metadata"]["data_quality_summary"]
    assert sum(counts.values()) == 3
    assert counts["high_quality"] == 1
    assert counts["medium_quality"] == 1
    assert counts["low_quality"] == 1


@patch("requests.Session.get")
def test_integration_full_pipeline(mock_get, tmp_path, monkeypatch):
    """
    End-to-end test invoking run_pipeline() with mocked API responses.
    Ensures file creation and valid structure.
    """
    monkeypatch.setattr(random, "choice", lambda choices: choices[0])

    page1 = {
        "page": 1,
        "total_pages": 2,
        "data": [
            {"id": 10, "first_name": "Ann", "last_name": "A", "email": "ann@a.com"},
            {"id": 11, "first_name": "Ben", "last_name": "B", "email": ""},
        ],
    }
    page2 = {
        "page": 2,
        "total_pages": 2,
        "data": [
            {"id": 11, "first_name": "Ben", "last_name": "B", "email": "ben@b.com"},
            {"id": 12, "first_name": "Carl", "last_name": "C", "email": "carl@c.com"},
        ],
    }

    mock_get.side_effect = [make_response(200, page1), make_response(200, page2)]

    out_file = tmp_path / "pipeline_output.json"
    run_pipeline(base_url=BASE_URL, output_file=str(out_file))

    assert out_file.exists()
    payload = json.loads(out_file.read_text(encoding="utf-8"))

    assert "metadata" in payload and "customers" in payload

    # After processor dedupe: ids 10,11,12 present
    customer_ids = {c["customer_id"] for c in payload["customers"]}
    assert customer_ids == {10, 11, 12}

    # Sorted names
    names = [c["full_name"] for c in payload["customers"]]
    assert names == sorted(names, key=lambda s: s.lower())

    assert payload["metadata"]["total_customers"] == 3
    assert sum(payload["metadata"]["data_quality_summary"].values()) == 3
