import random
import re
from typing import Dict, List, Optional
import logging


class CustomerDataProcessor:
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or self._default_logger()

    def _default_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.__class__.__name__)
        if not logger.handlers:
            h = logging.StreamHandler()
            h.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
            )
            logger.addHandler(h)
            logger.setLevel(logging.INFO)
        return logger

    # then inside process_customers / _transform_customer we can optionally log:
    # self.logger.debug("Transforming customer id=%s", customer_id)

    # Process and standardize raw customer data from the API.

    ENGAGEMENT_LEVELS = ["high", "medium", "low"]
    ACTIVITY_STATUSES = ["active", "inactive"]
    ACQUISITION_CHANNELS = ["website", "mobile_app", "email_campaign"]
    MARKET_SEGMENTS = ["US-West", "US-East", "EU-Central", "APAC"]
    CUSTOMER_TIERS = ["basic", "premium", "enterprise"]

    def process_customers(self, raw_customers: List[Dict]) -> List[Dict]:
        """
        Transform raw API data into analytics-ready format.
        Deduplicates customers by keeping the record with highest quality score.
        """
        processed = []
        seen = {}

        for raw in raw_customers:
            customer = self._transform_customer(raw)
            cid = customer["customer_id"]

            # Deduplicate: keep the one with higher quality score
            if (
                cid not in seen
                or customer["data_quality_score"] > seen[cid]["data_quality_score"]
            ):
                seen[cid] = customer

        processed = list(seen.values())
        return processed

    def _transform_customer(self, raw: Dict) -> Dict:
        """
        Transform one raw record into standardized format.
        Handles missing fields and assigns data quality score.
        """
        customer_id = raw.get("id")
        first_name = raw.get("first_name", "").strip()
        last_name = raw.get("last_name", "").strip()
        full_name = (
            f"{first_name} {last_name}".strip()
            if (first_name or last_name)
            else "Unknown"
        )

        email = raw.get("email", "")
        email_domain = self._extract_domain(email)

        # Start quality score at 100, deduct for missing/invalid fields
        quality_score = 100
        if not email_domain or email_domain == "unknown":
            quality_score -= 10
        if full_name == "Unknown":
            quality_score -= 10

        transformed = {
            "customer_id": customer_id,
            "full_name": full_name,
            "email_domain": email_domain,
            "engagement_level": self._random_or_unknown(self.ENGAGEMENT_LEVELS),
            "activity_status": self._random_or_unknown(self.ACTIVITY_STATUSES),
            "acquisition_channel": self._random_or_unknown(self.ACQUISITION_CHANNELS),
            "market_segment": self._random_or_unknown(self.MARKET_SEGMENTS),
            "customer_tier": self._random_or_unknown(self.CUSTOMER_TIERS),
            "data_quality_score": quality_score,
        }

        return transformed

    def _extract_domain(self, email: str) -> str:
        # Extract domain from email safely, return 'unknown' if invalid.
        if not email or "@" not in email:
            return "unknown"
        match = re.match(r".+@([A-Za-z0-9.-]+\.[A-Za-z]{2,})$", email)
        return match.group(1) if match else "unknown"

    def _random_or_unknown(self, choices: List[str]) -> str:
        # Pick a random value from choices, return 'unknown' if empty.
        return random.choice(choices) if choices else "unknown"
