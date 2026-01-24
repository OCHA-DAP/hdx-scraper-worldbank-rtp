#!/usr/bin/python
"""Worldbank_rtp scraper"""

import logging
from collections import defaultdict
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

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.add_tags(self._configuration["tags"])
        dataset.set_subnational(True)

        try:
            dataset.add_country_location(country_code)
        except HDXError:
            logger.error(f"Couldn't find country {country_name}, skipping")
            return None

        min_date = None
        max_date = None

        # Add a resource per model
        for model, records in country_model_data.items():
            if not records:
                logger.warning(f"No records for {model} in {country_name}")
                continue

            # Update date range for this model's records
            for record in records:
                date = record.get("DATES")
                if date:
                    if min_date is None or date < min_date:
                        min_date = date
                    if max_date is None or date > max_date:
                        max_date = date

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

        # Set time period after collecting all dates
        if min_date is None or max_date is None:
            logger.warning(f"No valid dates found for {country_name}")
            return None

        dataset.set_time_period(startdate=min_date, enddate=max_date)

        return dataset

    def create_global_datasets_from_csv_files(
        self, global_csv_files: Dict
    ) -> List[Dataset]:
        """
        Create global datasets from CSV files organized by model and year

        Args:
            global_csv_files: Dictionary of {(model, year): {'filepath': path, 'file': obj, 'writer': writer, 'headers': list}}

        Returns:
            List of datasets (one per model) with resources by year
        """
        datasets = []

        # Organize files by model
        files_by_model = defaultdict(list)
        for (model, year), file_info in global_csv_files.items():
            files_by_model[model].append((year, file_info["filepath"]))

        # Create one dataset per model
        for model in sorted(files_by_model.keys()):
            logger.info(f"Creating global dataset for {model}")

            dataset_title = f"Global - Real Time {model.capitalize()} Prices"
            dataset_name = slugify(dataset_title)

            dataset = Dataset(
                {
                    "name": dataset_name,
                    "title": dataset_title,
                }
            )

            # Sort years in reverse order (most recent first)
            year_files = sorted(files_by_model[model], key=lambda x: x[0], reverse=True)

            all_dates = []

            # Add one resource per year
            for year, filepath in year_files:
                logger.info(f"Adding global resource for {model} {year}")

                # Count records and collect dates for this year
                record_count = 0
                import csv as csv_module

                with open(filepath, "r", encoding="utf-8") as f:
                    reader = csv_module.DictReader(f)
                    for row in reader:
                        record_count += 1
                        date_str = row.get("DATES")
                        if date_str:
                            date = parse_date(date_str)
                            if date:
                                all_dates.append(date)

                logger.info(
                    f"Global resource {model} {year} has {record_count} records"
                )

                resource_name = f"Real Time {model.capitalize()} Prices {year}"
                resource_data = {
                    "name": resource_name,
                    "description": f"{self._configuration.get(f'description_{model}', '')} - data for {year}",
                    "format": "csv",
                }

                # Add the CSV file as a resource
                resource = dataset.add_update_resource(resource_data)
                resource.set_file_to_upload(filepath)

                # Set time period for this model's dataset
            if all_dates:
                dataset.set_time_period(
                    startdate=min(all_dates), enddate=max(all_dates)
                )

            # Add tags
            dataset.add_tags(self._configuration["tags"])
            dataset.set_subnational(False)
            dataset.add_other_location("world")

            datasets.append(dataset)
            logger.info(
                f"Completed global dataset for {model} with {len(year_files)} yearly resources"
            )

        return datasets
