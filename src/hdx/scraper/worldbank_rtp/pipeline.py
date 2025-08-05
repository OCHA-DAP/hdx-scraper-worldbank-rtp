#!/usr/bin/python
"""Worldbank_rtp scraper"""

import logging
from collections import defaultdict
from datetime import datetime
from typing import List, Optional, Tuple

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
        self._model = "energy"

    def fetch_data(self, max_records: Optional[int] = None):
        limit = 1000
        offset = 0
        total = max_records

        while True:
            model = self._model.lower()
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

    def aggregate_by_country(self, max_records: Optional[int] = None):
        country_data = defaultdict(list)

        for record in self.fetch_data(max_records=max_records):
            country_code = record.get("ISO3", "Unknown")

            # Format date fields
            record["DATES"] = parse_date(record.get("DATES"))
            # record["start_dense_data"] = self.format_date(record.get("start_dense_data", ""), "%b %Y")
            # record["last_survey_point"] = self.format_date(record.get("last_survey_point", ""), "%b %Y")

            country_data[country_code].append(record)

            if len(country_data[country_code]) >= 10000:
                yield country_code, country_data[country_code]
                country_data[country_code] = []

        # Yield remaining data
        for country_code, records in country_data.items():
            if records:
                yield country_code, records

    def generate_dataset(self, country_data: List[dict]) -> Optional[Dataset]:
        country_code = country_data[0].get("ISO3", "Unknown")
        country_name = Country.get_country_name_from_iso3(country_code)
        if not country_name:
            logger.warning(f"Unknown ISO3: {country_code}")
            return None

        dataset_title = f"{country_name} - {self._configuration['title']}"
        dataset_name = slugify(dataset_title)
        min_date, max_date = self.get_date_range(country_data)

        dataset_tags = self._configuration["tags"]

        # Dataset info
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
            return

        # Add resources here
        resource_name = (
            f"Real Time {self._model.capitalize()} Prices for {country_name}"
        )
        resource_description = f"description_{self._model}"
        resource_data = {
            "name": resource_name,
            "description": self._configuration[resource_description],
        }

        dataset.generate_resource_from_iterable(
            headers=list(country_data[0].keys()),
            iterable=country_data,
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
