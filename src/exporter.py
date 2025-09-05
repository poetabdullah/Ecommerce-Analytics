"""
- Sort processed customers by full_name (ascending)
- Produce metadata: total count, export timestamp (UTC ISO), data quality summary
- Export full payload to JSON file (structured per spec)
- Provide a programmatic summary report (dict) for tests/CLI
"""

from typing import Dict, List, Any
import json
import logging
from datetime import datetime, timezone
from pathlib import Path


class ExportError(Exception):
    # Raised when export fails (IO or validation).
    pass


class DataExporter:
    def __init__(self, logger: logging.Logger = None):
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

    def _quality_bucket(self, score: int) -> str:
        """Map numeric score to quality bucket.
        - high: score >= 90
        - medium: 70 <= score < 90
        - low: score < 70
        """
        if score >= 90:
            return "high_quality"
        if score >= 70:
            return "medium_quality"
        return "low_quality"

    def generate_summary_report(
        self, customers: List[Dict[str, Any]]
    ) -> Dict[str, Any]:

        # Return summary metadata/statistics for the provided customers list.
        if customers is None:
            raise ExportError("customers list is None")

        total = len(customers)
        counts = {"high_quality": 0, "medium_quality": 0, "low_quality": 0}
        for c in customers:
            score = c.get("data_quality_score", 0)
            bucket = self._quality_bucket(score)
            counts[bucket] += 1

        report = {
            "total_customers": total,
            "export_timestamp": datetime.now(timezone.utc).isoformat(),
            "data_quality_summary": counts,
        }
        return report

    def export_customers(
        self, customers: List[Dict[str, Any]], output_file: str
    ) -> None:
        """
        Export customers to JSON file with metadata and sorted customer list.

        :param customers: list of processed customer dicts (must contain 'full_name' and 'data_quality_score')
        :param output_file: path to write JSON (will be overwritten if exists)
        """
        if customers is None:
            raise ExportError("customers list is None")

        # Sort by full_name (case-insensitive); if missing, put at end
        def sort_key(c: Dict[str, Any]) -> str:
            name = c.get("full_name") or ""
            return name.lower()

        sorted_customers = sorted(customers, key=sort_key)

        # Build metadata
        metadata = self.generate_summary_report(sorted_customers)

        payload = {"metadata": metadata, "customers": sorted_customers}

        # Ensure directory exists
        out_path = Path(output_file)
        if out_path.parent and not out_path.parent.exists():
            out_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with out_path.open("w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, ensure_ascii=False)
            self.logger.info(
                "Exported %d customers to %s", len(sorted_customers), output_file
            )
        except Exception as exc:
            self.logger.exception(
                "Failed to export customers to %s: %s", output_file, exc
            )
            raise ExportError(f"Failed to write output file {output_file}: {exc}")
