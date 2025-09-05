"""
- fetch paginated users from an API https://reqres.in/api/users
- retries with exponential backoff (1s, 2s, 4s)
- special handling for 429 (uses Retry-After when provided)
- deduplicates by `id` (keeps first-seen)
"""

from typing import Dict, List, Optional
import logging
import time

import requests


class APIClientError(Exception):
    # Custom exception for API client errors.

    def __init__(
        self,
        message: str,
        url: Optional[str] = None,
        status_code: Optional[int] = None,
        retries: int = 0,
    ):
        super().__init__(message)
        self.url = url
        self.status_code = status_code
        self.retries = retries

    def __str__(self) -> str:
        base = f"APIClientError: {self.args[0]}"
        if self.url:
            base += f" | URL: {self.url}"
        if self.status_code is not None:
            base += f" | Status: {self.status_code}"
        if self.retries:
            base += f" | Retries: {self.retries}"
        return base


class CustomerAPIClient:
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_BACKOFF = (1, 2, 4)  # seconds

    def __init__(
        self,
        base_url: str,
        api_key: Optional[
            str
        ] = None,  # Does not really requires an API key for this API
        session: Optional[requests.Session] = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff: Optional[tuple] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.session = session or requests.Session()
        self.max_retries = max_retries
        self.backoff = backoff or self.DEFAULT_BACKOFF
        self.logger = logger or self._build_default_logger()

    def _build_default_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.__class__.__name__)
        if not logger.handlers:
            h = logging.StreamHandler()
            fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
            h.setFormatter(logging.Formatter(fmt))
            logger.addHandler(h)
            logger.setLevel(logging.INFO)
        return logger

    def _get_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _request(self, url: str, params: Optional[Dict] = None) -> Dict:
        # Perform GET with retry/backoff and special 429 handling.
        params = params or {}
        headers = self._get_headers()

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, headers=headers, timeout=10)
            except requests.RequestException as exc:
                self.logger.warning(
                    "Network error on attempt %d for %s: %s", attempt, url, exc
                )
                self._sleep_for_attempt(attempt)
                continue

            # Success
            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError:
                    raise APIClientError(f"Invalid JSON from {url}")

            # Rate limit -> respect Retry-After if possible
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                self.logger.warning(
                    "429 received for %s on attempt %d. Retry-After=%s",
                    url,
                    attempt,
                    retry_after,
                )
                if retry_after:
                    # Try to parse and wait server-specified seconds
                    try:
                        wait = int(retry_after)
                    except ValueError:
                        wait = self.backoff[min(attempt - 1, len(self.backoff) - 1)]
                    time.sleep(wait)
                else:
                    self._sleep_for_attempt(attempt)
                continue

            # Server errors -> retry
            if 500 <= resp.status_code < 600:
                self.logger.warning(
                    "Server error %d on attempt %d for %s",
                    resp.status_code,
                    attempt,
                    url,
                )
                self._sleep_for_attempt(attempt)
                continue

            # Client error (other than 429) -> don't retry
            if 400 <= resp.status_code < 500:
                raise APIClientError(
                    f"Client error {resp.status_code} for {url}: {resp.text}"
                )

            # Unexpected -> retry
            self.logger.warning(
                "Unexpected status %d on attempt %d for %s",
                resp.status_code,
                attempt,
                url,
            )
            self._sleep_for_attempt(attempt)

        # exhausted retries
        raise APIClientError(f"Failed to fetch {url} after {self.max_retries} attempts")

    def _sleep_for_attempt(self, attempt: int) -> None:
        idx = min(attempt - 1, len(self.backoff) - 1)
        wait = self.backoff[idx]
        self.logger.debug("Sleeping %ds before next attempt", wait)
        time.sleep(wait)

    def _fetch_page(self, page: int) -> Dict:
        url = f"{self.base_url}/users"
        self.logger.debug("Fetching page %d from %s", page, url)
        return self._request(url, params={"page": page})

    def fetch_all_customers(self) -> List[Dict]:
        """Fetch all pages from /users, dedupe by `id`, return list of user dicts."""
        # Fetch first page to learn pagination
        first = self._fetch_page(1)
        data = list(first.get("data") or [])
        total_pages = int(first.get("total_pages") or 1)
        self.logger.info(
            "Discovered total_pages=%s, first_page_count=%d", total_pages, len(data)
        )

        # Fetch remaining pages
        for p in range(2, total_pages + 1):
            page_json = self._fetch_page(p)
            page_data = list(page_json.get("data") or [])
            self.logger.info("Fetched page %d with %d records", p, len(page_data))
            data.extend(page_data)

        # Deduplicate by id (keep first seen)
        seen = {}
        for item in data:
            item_id = item.get("id")
            if item_id is None:
                # generate a unique fallback key so malformed but present records are not lost
                key = f"_noid_{id(item)}"
            else:
                key = f"id_{item_id}"
            if key not in seen:
                seen[key] = item
            else:
                self.logger.debug(
                    "Duplicate encountered for key=%s; keeping first seen", key
                )

        results = list(seen.values())
        self.logger.info("Returning %d unique raw customer records", len(results))
        return results
