import pytest
import requests
from unittest.mock import MagicMock, patch
from src.api_client import CustomerAPIClient, APIClientError
from unittest.mock import patch as umpatch


BASE_URL = "https://reqres.in/api"


def make_response(status_code=200, json_data=None, headers=None, text=""):
    """Helper to create a fake requests.Response object."""
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
def test_fetch_all_customers_success(mock_get):
    # Arrange: two pages of data
    page1 = {"page": 1, "total_pages": 2, "data": [{"id": 1, "email": "abc@x.com"}]}
    page2 = {"page": 2, "total_pages": 2, "data": [{"id": 2, "email": "xyz@x.com"}]}
    mock_get.side_effect = [
        make_response(200, page1),
        make_response(200, page2),
    ]

    client = CustomerAPIClient(BASE_URL)

    # Act
    customers = client.fetch_all_customers()

    # Assert
    assert len(customers) == 2
    assert {c["id"] for c in customers} == {1, 2}


@patch("requests.Session.get")
def test_client_returns_all_raw_records(mock_get):
    # Client should return all raw records (no dedupe) so processor can resolve duplicates by quality.
    page1 = {
        "page": 1,
        "total_pages": 1,
        "data": [
            {"id": 1, "email": "first@x.com"},
            {"id": 1, "email": "second@x.com"},  # duplicate preserved
        ],
    }
    mock_get.return_value = make_response(200, page1)

    client = CustomerAPIClient(BASE_URL)
    customers = client.fetch_all_customers()

    # Expect both entries preserved so processor can later pick by quality
    assert len(customers) == 2
    assert customers[0]["email"] == "first@x.com"
    assert customers[1]["email"] == "second@x.com"


@umpatch("time.sleep", lambda s: None)
@patch("requests.Session.get")
def test_retry_logic_on_500_then_success(mock_get):

    # First two fail with 500, third succeeds
    fail_resp = make_response(500)
    success_page = {"page": 1, "total_pages": 1, "data": [{"id": 1}]}
    success_resp = make_response(200, success_page)

    mock_get.side_effect = [fail_resp, fail_resp, success_resp]

    client = CustomerAPIClient(BASE_URL, max_retries=3)
    customers = client.fetch_all_customers()

    assert len(customers) == 1
    assert customers[0]["id"] == 1


@patch("requests.Session.get")
def test_respects_retry_after_on_429(mock_get, monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)  # no-op sleep
    page = {"page": 1, "total_pages": 1, "data": [{"id": 1}]}
    resp429 = make_response(429, headers={"Retry-After": "1"})
    resp200 = make_response(200, page)
    mock_get.side_effect = [resp429, resp200]

    client = CustomerAPIClient(BASE_URL)
    customers = client.fetch_all_customers()

    assert len(customers) == 1


@patch("requests.Session.get")
def test_invalid_json_raises_error(mock_get):
    mock_get.return_value = make_response(200, json_data=None)  # invalid JSON

    client = CustomerAPIClient(BASE_URL)

    with pytest.raises(APIClientError):
        client.fetch_all_customers()


@patch("requests.Session.get")
def test_client_error_raises_error(mock_get):
    mock_get.return_value = make_response(404, text="Not found")

    client = CustomerAPIClient(BASE_URL)

    with pytest.raises(APIClientError) as excinfo:
        client.fetch_all_customers()

    assert "Client error 404" in str(excinfo.value)


# Add a tiny unit test that when ClientError is raised, the APIClientError contains url and status_code
@patch("requests.Session.get")
def test_api_client_error_contains_context(mock_get):
    resp = make_response(404, text="Not found")
    mock_get.return_value = resp
    client = CustomerAPIClient(BASE_URL)
    with pytest.raises(APIClientError) as excinfo:
        client.fetch_all_customers()
    err = excinfo.value
    assert hasattr(err, "url") and err.url is not None
    assert err.status_code == 404
    assert err.retries >= 1
