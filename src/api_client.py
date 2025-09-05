from typing import Dict, List, Optional
import logging
import time

import requests


class APIClientError(Exception):
    """Custom exception for API client errors."""

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
        api_key: Optional[str] = None,
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
        """Perform GET with retry/backoff and special 429 handling."""
        params = params or {}
        headers = self._get_headers()
        last_exc: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, headers=headers, timeout=10)
            except requests.RequestException as exc:
                last_exc = exc
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
                    raise APIClientError(
                        f"Invalid JSON from {url}",
                        url=url,
                        status_code=resp.status_code,
                        retries=attempt,
                    )

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
                    try:
                        wait = int(retry_after)
                    except ValueError:
                        wait = self.backoff[min(attempt - 1, len(self.backoff) - 1)]
                    time.sleep(wait)
                else:
                    self._sleep_for_attempt(attempt)
                last_exc = APIClientError(
                    "429 Too Many Requests", url=url, status_code=429, retries=attempt
                )
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
                last_exc = APIClientError(
                    f"{resp.status_code} server error",
                    url=url,
                    status_code=resp.status_code,
                    retries=attempt,
                )
                continue

            # Client error (other than 429) -> don't retry
            if 400 <= resp.status_code < 500:
                raise APIClientError(
                    f"Client error {resp.status_code} for {url}: {resp.text}",
                    url=url,
                    status_code=resp.status_code,
                    retries=attempt,
                )

            # Unexpected -> retry
            self.logger.warning(
                "Unexpected status %d on attempt %d for %s",
                resp.status_code,
                attempt,
                url,
            )
            self._sleep_for_attempt(attempt)
            last_exc = APIClientError(
                f"Unexpected status {resp.status_code}",
                url=url,
                status_code=resp.status_code,
                retries=attempt,
            )

        # exhausted retries
        if isinstance(last_exc, APIClientError):
            raise APIClientError(
                str(last_exc),
                url=last_exc.url,
                status_code=last_exc.status_code,
                retries=self.max_retries,
            )
        if last_exc:
            raise APIClientError(
                f"Failed to fetch {url} after {self.max_retries} attempts: {last_exc}",
                url=url,
                retries=self.max_retries,
            )
        raise APIClientError(
            f"Failed to fetch {url} after {self.max_retries} attempts",
            url=url,
            retries=self.max_retries,
        )

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
        """
        Fetch all pages from /users and return combined raw records.

        NOTE: Deduplication is intentionally NOT performed in the client so that
        the processor (which computes data_quality_score) can decide which duplicate
        to keep (highest-quality). This follows the project spec.
        """
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

        self.logger.info("Returning %d raw customer records (no dedupe)", len(data))
        return data
