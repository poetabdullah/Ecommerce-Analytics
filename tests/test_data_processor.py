import pytest
import random
from src.data_processor import CustomerDataProcessor


@pytest.fixture
def processor():
    return CustomerDataProcessor()


def test_data_processor_transformation(processor, monkeypatch):
    # Test transformation of a valid customer into analytics-ready format.

    # Patch random.choice to make it deterministic
    monkeypatch.setattr(random, "choice", lambda choices: choices[0])

    raw = {
        "id": 1,
        "first_name": "George",
        "last_name": "Bluth",
        "email": "george.bluth@reqres.in",
    }

    result = processor._transform_customer(raw)

    assert result["customer_id"] == 1
    assert result["full_name"] == "George Bluth"
    assert result["email_domain"] == "reqres.in"
    assert result["engagement_level"] in processor.ENGAGEMENT_LEVELS
    assert result["data_quality_score"] == 100


def test_duplicate_handling(processor, monkeypatch):
    # Test deduplication keeps record with highest quality score.

    monkeypatch.setattr(random, "choice", lambda choices: choices[0])

    raw_customers = [
        {"id": 1, "first_name": "John", "last_name": "Doe", "email": ""},  # low quality
        {
            "id": 1,
            "first_name": "John",
            "last_name": "Doe",
            "email": "john@doe.com",
        },  # higher quality
    ]

    processed = processor.process_customers(raw_customers)

    assert len(processed) == 1
    assert processed[0]["email_domain"] == "doe.com"
    assert processed[0]["data_quality_score"] == 100


def test_data_quality_scoring(processor, monkeypatch):
    # Test quality score deductions for missing fields.

    monkeypatch.setattr(random, "choice", lambda choices: choices[0])

    raw = {
        "id": 2,
        "first_name": "",
        "last_name": "",
        "email": "invalidemail",
    }

    result = processor._transform_customer(raw)

    # Deduct -10 for unknown full_name, -10 for invalid email
    assert result["data_quality_score"] == 80
    assert result["full_name"] == "Unknown"
    assert result["email_domain"] == "unknown"


# Testing some edge cases
@pytest.mark.parametrize(
    "email,expected_domain",
    [
        ("", "unknown"),
        (None, "unknown"),
        ("notanemail", "unknown"),
        ("abc@valid.com", "valid.com"),
    ],
)
def test_edge_cases(processor, email, expected_domain, monkeypatch):
    # Test malformed data handling for emails.

    monkeypatch.setattr(random, "choice", lambda choices: choices[0])

    raw = {"id": 3, "email": email}

    result = processor._transform_customer(raw)

    assert result["email_domain"] == expected_domain


def test_random_or_unknown(processor, monkeypatch):
    """Test _random_or_unknown deterministic behavior."""

    # Force random.choice to return the first element
    monkeypatch.setattr(random, "choice", lambda choices: choices[0])

    assert processor._random_or_unknown(["a", "b", "c"]) == "a"
    assert processor._random_or_unknown([]) == "unknown"
