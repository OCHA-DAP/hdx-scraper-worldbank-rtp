#!/usr/bin/python
"""Worldbank_rtp scraper"""

import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, Iterator, List, Optional, Tuple

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

    def fetch_data(self, model: str, max_records: Optional[int] = None):
        limit = 1000
        offset = 0
        total = max_records

        while True:
            data_url = f"{self._configuration['base_url']}{self._configuration[model]}?limit={limit}&offset={offset}"
            response = self._retriever.download_json(data_url)

            if total is None:
                total = response.get("total", 0)

            batch = response.get("data", [])
            if not batch:
                break

            for record in batch:
                yield record

            offset += limit
            if offset >= total:
                break

    def aggregate_by_country(
        self, models: List, max_records: Optional[int] = None
    ) -> Iterator[Tuple]:
        """
        Aggregate data by country across all models
        Return a nested dict: {country: {model: [records]}}
        """
        country_data = defaultdict(lambda: defaultdict(list))

        for model in models:
            for record in self.fetch_data(model, max_records):
                country_code = record.get("ISO3", "Unknown")
                record["DATES"] = parse_date(record.get("DATES"))
                country_data[country_code][model].append(record)

                # If one country gets big, yield it and reset
                total_records = sum(
                    len(records) for records in country_data[country_code].values()
                )
                if total_records >= 10000:
                    yield country_code, country_data[country_code]
                    del country_data[country_code]  # Free up memory

        # Yield remaining countries
        for country_code, model_data in country_data.items():
            if any(model_data.values()):
                yield country_code, model_data

    def generate_dataset(
        self, country_code: str, country_model_data: Dict
    ) -> Optional[Dataset]:
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

        dataset_tags = self._configuration["tags"]

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period(startdate=min_date, enddate=max_date)
        dataset.add_tags(dataset_tags)
        dataset.set_subnational(True)

        try:
            dataset.add_country_location(country_code)
        except HDXError:
            logger.error(f"Couldn't find country {country_name}, skipping")
            return None

        # Add a resource per model
        for model, records in country_model_data.items():
            resource_name = f"Real Time {model.capitalize()} Prices for {country_name}"
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

    def get_date_range(
        self, records: List
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        dates = []
        for rec in records:
            date = rec.get("DATES")
            if not date:
                continue
            dates.append(date)

        if not dates:
            return None, None

        return min(dates), max(dates)
