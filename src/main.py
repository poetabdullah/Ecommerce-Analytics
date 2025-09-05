"""
Coordinates the pipeline:
 1. Fetch raw customers from API
 2. Process & enrich them into standardized format
 3. Export results to JSON
 4. Print summary report
"""

import sys
import logging
from pathlib import Path

from src.api_client import CustomerAPIClient, APIClientError
from src.data_processor import CustomerDataProcessor
from src.exporter import DataExporter, ExportError


def run_pipeline(base_url: str, output_file: str, api_key: str = None) -> None:
    logger = logging.getLogger("Pipeline")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    try:
        # 1. Fetch customers
        client = CustomerAPIClient(base_url=base_url, api_key=api_key, logger=logger)
        raw_customers = client.fetch_all_customers()
        logger.info("Fetched %d raw customers", len(raw_customers))

        # 2. Process & enrich
        processor = CustomerDataProcessor(logger=logger)
        processed_customers = processor.process_customers(raw_customers)
        logger.info("Processed %d customers", len(processed_customers))

        # 3. Export
        exporter = DataExporter(logger=logger)
        exporter.export_customers(processed_customers, output_file)

        # 4. Summary
        report = exporter.generate_summary_report(processed_customers)
        logger.info("Summary Report: %s", report)

    except APIClientError as e:
        logger.error("API client failed: %s", e)
    except ExportError as e:
        logger.error("Export failed: %s", e)
    except Exception as e:
        logger.exception("Unexpected pipeline error: %s", e)


if __name__ == "__main__":
    BASE_URL = "https://reqres.in/api"
    OUTPUT_FILE = str(Path(__file__).parent.parent / "sample_output.json")

    run_pipeline(base_url=BASE_URL, output_file=OUTPUT_FILE)
