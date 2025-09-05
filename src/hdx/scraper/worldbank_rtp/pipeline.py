#!/usr/bin/python
"""Worldbank_rtp scraper"""

import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        tempdir: str,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir

    def fetch_data(self, model: str, max_records: Optional[int] = None) -> List[Dict]:
        limit = 1000  # API max is 1000
        offset = 0
        total = max_records  # max_records used for testing
        all_records = []

        while True:
            data_url = f"{self._configuration['base_url']}{self._configuration[model]}?limit={limit}&offset={offset}"
            response = self._retriever.download_json(data_url)

            if total is None:
                total = response.get("total", 0)

            batch = response.get("data", [])
            if not batch:
                break

            all_records.extend(batch)
            offset += limit

            if offset >= total:
                break

        return all_records

    def aggregate_and_generate_datasets(
        self, models: List, max_records: Optional[int] = None
    ) -> Tuple:
        """
        Split data by country across all models
        Return a nested dict: {country: {model: [records]}}
        """
        country_data = defaultdict(lambda: defaultdict(list))
        global_model_data = defaultdict(list)

        for model in models:
            records = self.fetch_data(model, max_records)
            for record in records:
                country_code = record.get("ISO3", "Unknown")
                record["DATES"] = parse_date(record.get("DATES"))

                country_data[country_code][model].append(record)
                global_model_data[model].append(record)

                # Check if data for a country/model > 50000
            #     if len(country_data[country_code][model]) == 50000:
            #         logger.warning(
            #             f"{country_code} has over 50,000 records for model {model}"
            #         )
            #
            # if len(global_model_data[model]) > 250000:
            #     logger.warning(f"Model {model} has more than 250,000 global records")

        # Generate country datasets
        datasets = []
        for country_code, model_data in country_data.items():
            dataset = self.generate_dataset(country_code, model_data)
            if dataset:
                datasets.append(dataset)

        # Generate global dataset
        global_dataset = self.generate_dataset("global", global_model_data)

        return datasets, global_dataset

    def generate_dataset(
        self, country_code: str, country_model_data: Dict
    ) -> Optional[Dataset]:
        if country_code == "global":
            country_name = "Global"
        else:
            country_name = Country.get_country_name_from_iso3(country_code)
            if not country_name:
                logger.warning(f"Unknown ISO3: {country_code}")
                return None

        dataset_title = f"{country_name} - {self._configuration['title']}"
        dataset_name = slugify(dataset_title)

        # Get min/max date across all models
        all_records = [
            r for model_records in country_model_data.values() for r in model_records
        ]
        min_date, max_date = self.get_date_range(all_records)

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )
        dataset.set_time_period(startdate=min_date, enddate=max_date)
        dataset.add_tags(self._configuration["tags"])

        if country_code == "global":
            dataset.set_subnational(False)
            try:
                dataset.add_other_location("World", exact=False)
            except HDXError:
                logger.warning("Can't add 'World', skipping")
        else:
            dataset.set_subnational(True)
            try:
                dataset.add_country_location(country_code)
            except HDXError:
                logger.warning(f"Can't find country {country_name}, skipping")
                return None

        # Add a resource per model
        for model, records in country_model_data.items():
            if not records:
                logger.warning(f"Skipping empty resource for {country_code} - {model}")
                continue

            if country_code == "global":
                resource_name = f"Global Real Time {model.capitalize()} Prices"
            else:
                resource_name = (
                    f"Real Time {model.capitalize()} Prices for {country_name}"
                )
            resource_description = f"description_{model}"
            resource_data = {
                "name": resource_name,
                "description": self._configuration.get(resource_description, ""),
            }

            dataset.generate_resource_from_iterable(
                headers=list(records[0].keys()),
                iterable=records,
                hxltags={},
                folder=self._tempdir,
                filename=f"{slugify(resource_name)}.csv",
                resourcedata=resource_data,
                quickcharts=None,
            )

        return dataset

    def format_date(self, date_str: str, date_fmt: str = None) -> str:
        if not date_str:
            return ""
        try:
            if date_fmt:
                dt = datetime.strptime(date_str, date_fmt)
            else:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt.date().isoformat()  # Return 'YYYY-MM-DD' format
        except Exception:
            return date_str  # Return original value if parsing fails

    def get_date_range(self, records: List) -> Tuple:
        dates = []
        for rec in records:
            date = rec.get("DATES")
            if not date:
                continue
            dates.append(date)

        if not dates:
            return None, None

        return min(dates), max(dates)
