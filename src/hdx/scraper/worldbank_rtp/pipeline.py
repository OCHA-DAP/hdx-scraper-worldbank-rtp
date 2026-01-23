#!/usr/bin/python
"""Worldbank_rtp scraper"""

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
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
        self,
        models: List,
        max_records: Optional[int] = None,
        max_countries: Optional[int] = None,
    ) -> Iterator[Tuple]:
        """
        Split data by country across all models
        Return a nested dict: {country: {model: [records]}}
        """
        country_data = defaultdict(lambda: defaultdict(list))

        for model in models:
            for record in self.fetch_data(model, max_records):
                country_code = record.get("ISO3", "Unknown")
                record["DATES"] = parse_date(record.get("DATES"))
                country_data[country_code][model].append(record)

        total_countries = len(country_data)
        logger.info(f"Yielding {total_countries} countries")

        countries_yielded = 0
        for country_code, model_data in country_data.items():
            if any(model_data.values()):
                yield country_code, model_data
                countries_yielded += 1

                # Stop after max_countries if specified
                if max_countries and countries_yielded >= max_countries:
                    logger.info(
                        f"Reached max_countries limit ({max_countries}), stopping"
                    )
                    break

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
        if not all_records:
            logger.warning(f"No records for {country_name}, skipping")
            return None

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
            if not records:
                logger.warning(f"No records for {model} in {country_name}")
                continue

            resource_name = f"Real Time {model.capitalize()} Prices for {country_name}"
            resource_description = f"description_{model}"
            resource_data = {
                "name": resource_name,
                "description": self._configuration.get(resource_description, ""),
            }

            dataset.generate_resource(
                folder=self._tempdir,
                filename=f"{slugify(resource_name)}.csv",
                rows=records,
                resourcedata=resource_data,
            )

        return dataset

    def create_global_datasets_by_year(
        self,
        models: List,
        years: Optional[int] = None,
        max_records: Optional[int] = None,
    ) -> List[Dataset]:
        """
        Create separate global datasets for each model, with one resource per year.

        Args:
            models: List of model names to process
            years: Number of years to include. If None, includes all available years. Defaults to None.
            max_records: Maximum records to fetch per model (for testing)

        Returns:
            List of datasets (one per model), each with yearly resources
        """
        current_year = datetime.now().year
        datasets = []

        if years:
            logger.info(f"Creating global datasets with last {years} years of data...")
        else:
            logger.info("Creating global datasets with all available years...")

        for model in models:
            logger.info(f"Processing global dataset for {model}...")

            # Create dataset for this model
            dataset_title = f"Global - Real Time {model.capitalize()} Prices"
            dataset_name = slugify(dataset_title)

            dataset = Dataset(
                {
                    "name": dataset_name,
                    "title": dataset_title,
                }
            )

            model_dates = []

            # Collect all records for this model, organized by year
            records_by_year = defaultdict(list)

            for record in self.fetch_data(model, max_records=max_records):
                record["DATES"] = parse_date(record.get("DATES"))

                if record.get("DATES"):
                    year = record["DATES"].year

                    if years is None or (current_year - year < years):
                        records_by_year[year].append(record)
                        model_dates.append(record["DATES"])

            if not records_by_year:
                logger.warning(f"No records found for model {model}")
                continue

            # Create one resource per year for this model
            for year in sorted(records_by_year.keys(), reverse=True):
                records = records_by_year[year]

                if not records:
                    continue

                logger.info(
                    f"Creating resource for {model} {year} ({len(records)} records)"
                )

                resource_name = f"Global Real Time {model.capitalize()} Prices {year}"
                resource_data = {
                    "name": resource_name,
                    "description": f"{self._configuration.get(f'description_{model}', '')} - data for {year}",
                }

                # Generate resource from the collected records
                dataset.generate_resource(
                    folder=self._tempdir,
                    filename=f"{slugify(resource_name)}.csv",
                    rows=records,
                    resourcedata=resource_data,
                )
                logger.info(
                    f"Added resource {model} {year} with {len(records)} records"
                )

            # Set time period for this model's dataset
            if model_dates:
                dataset.set_time_period(
                    startdate=min(model_dates), enddate=max(model_dates)
                )

            # Add tags
            dataset.add_tags(self._configuration["tags"])
            dataset.set_subnational(False)
            dataset.add_other_location("world")

            datasets.append(dataset)
            logger.info(f"Completed global dataset for {model}")

        return datasets

    def generate_global_dataset(
        self, models: List, max_records: Optional[int] = None
    ) -> Optional[Dataset]:
        """
        Create global datasets for each model containing current data (last 2 years)
        """
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=730)  # 2 years ago

        logger.info("Creating global dataset...")

        dataset_title = f"Global - {self._configuration['title']}"
        dataset_name = slugify(dataset_title)

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        overall_dates = []
        for model in models:
            record_count = 0
            first_record = None

            # Collect records for this model (last 2 years only)
            records = []
            for record in self.fetch_data(model, max_records=max_records):
                record["DATES"] = parse_date(record.get("DATES"))

                # Only include records from last 2 years
                if record.get("DATES") and record["DATES"] >= cutoff_date:
                    if first_record is None:
                        first_record = record

                    records.append(record)
                    overall_dates.append(record["DATES"])
                    record_count += 1

            if not records:
                logger.warning(f"No current records for model {model}")
                continue

            logger.info(f"Filtered {record_count} current records for {model}")

            resource_name = f"Global Real Time {model.capitalize()} Prices"
            resource_data = {
                "name": resource_name,
                "description": f"{self._configuration.get(f'description_{model}', '')} - current data from the last 2 years",
            }

            # Generate resource from the collected records
            dataset.generate_resource(
                folder=self._tempdir,
                filename=f"{slugify(resource_name)}.csv",
                rows=records,
                resourcedata=resource_data,
            )

            logger.info(f"Added {model} resource with {record_count} current records")

        # Set time period from all resources
        if overall_dates:
            dataset.set_time_period(
                startdate=min(overall_dates), enddate=max(overall_dates)
            )

        # Add tags
        tags = self._configuration["tags"]
        dataset.add_tags(tags)
        dataset.set_subnational(False)
        dataset.add_other_location("world")

        return dataset

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
