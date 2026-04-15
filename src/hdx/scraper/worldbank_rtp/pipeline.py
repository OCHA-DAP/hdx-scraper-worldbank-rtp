#!/usr/bin/python
"""Worldbank_rtp scraper"""

import csv as csv_module
import logging
import os
from collections import defaultdict
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
        self._global_writers: Dict[Tuple, dict] = {}
        self._global_date_ranges: Dict[Tuple, dict] = {}

    # Safety cap: stop pagination if offset exceeds this value and log a warning
    # Prevents runaway loops if the API never returns an empty or partial page
    MAX_FETCH_OFFSET = 200_000

    def fetch_data(
        self,
        model: str,
        max_records: Optional[int] = None,
        iso3: Optional[str] = None,
    ):
        limit = 1000
        offset = 0
        total = max_records

        while True:
            if offset >= self.MAX_FETCH_OFFSET:
                logger.warning(
                    f"fetch_data reached safety cap of {self.MAX_FETCH_OFFSET} records "
                    f"for model={model}, iso3={iso3}; stopping pagination"
                )
                break

            data_url = f"{self._configuration['base_url']}{self._configuration[model]}?limit={limit}&offset={offset}"
            if iso3:
                data_url += f"&ISO3={iso3}"

            response = self._retriever.download_json(data_url)

            # Use `found` (per-query count) when available; fall back to `total`
            if total is None:
                total = response.get("found") or response.get("total", 0)

            batch = response.get("data", [])
            if not batch:
                break

            for record in batch:
                yield record

            offset += limit

            # Primary termination: a page shorter than the limit means end of data
            if len(batch) < limit:
                break

            # Secondary termination: offset has reached the reported record count
            if total and offset >= total:
                break

    def fetch_country_data(
        self,
        country_code: str,
        models: List[str],
        max_records: Optional[int] = None,
    ) -> Dict[str, List]:
        """Fetch data by country

        Returns:
            {model: [records]}
        """
        model_data = {}
        for model in models:
            records = list(self.fetch_data(model, max_records, iso3=country_code))
            model_data[model] = records
        return model_data

    def write_global_record(
        self,
        model: str,
        record: dict,
        current_year: int,
        global_years: Optional[int] = None,
    ) -> None:
        """Write record to appropriate global CSV file"""
        date_str = record.get("DATES")
        if not date_str:
            return

        year = date_str[:4]

        if global_years and (current_year - int(year) >= global_years):
            return

        key = (model, year)

        if key not in self._global_writers:
            filepath = os.path.join(self._tempdir, f"global_{model}_{year}.csv")
            headers = list(record.keys())
            f = open(filepath, "w", newline="", encoding="utf-8")
            writer = csv_module.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            self._global_writers[key] = {
                "file": f,
                "writer": writer,
                "filepath": filepath,
            }
            self._global_date_ranges[key] = {
                "min_date": date_str,
                "max_date": date_str,
            }

        self._global_writers[key]["writer"].writerow(record)

        dr = self._global_date_ranges[key]
        if date_str < dr["min_date"]:
            dr["min_date"] = date_str
        if date_str > dr["max_date"]:
            dr["max_date"] = date_str

    def close_global_files(self) -> None:
        """Close all open global CSV file handles"""
        for info in self._global_writers.values():
            f = info["file"]
            if f and not f.closed:
                f.close()

    def get_global_file_info(self) -> Dict:
        """Return file info for global datasets

        Returns:
            {(model, year): {"filepath": str, "min_date": datetime, "max_date": datetime}}
        """
        file_info = {}
        for key, writer_info in self._global_writers.items():
            dr = self._global_date_ranges[key]
            file_info[key] = {
                "filepath": writer_info["filepath"],
                "min_date": parse_date(dr["min_date"]),
                "max_date": parse_date(dr["max_date"]),
            }
        return file_info

    def generate_dataset(
        self, country_code: str, country_model_data: Dict
    ) -> Optional[Tuple[Dataset, List[str]]]:
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
        filepaths = []

        # Add a resource per model
        for model, records in country_model_data.items():
            if not records:
                logger.warning(f"No records for {model} in {country_name}")
                continue

            # Update date range
            for record in records:
                date_val = record.get("DATES")
                if date_val:
                    if not hasattr(date_val, "year"):
                        date_val = parse_date(date_val)
                    if date_val:
                        if min_date is None or date_val < min_date:
                            min_date = date_val
                        if max_date is None or date_val > max_date:
                            max_date = date_val

            resource_name = f"Real Time {model.capitalize()} Prices for {country_name}"
            resource_description = f"description_{model}"
            resource_data = {
                "name": resource_name,
                "description": self._configuration.get(resource_description, ""),
            }

            filename = f"{slugify(resource_name)}.csv"
            dataset.generate_resource(
                folder=self._tempdir,
                filename=filename,
                rows=records,
                resourcedata=resource_data,
            )
            filepaths.append(os.path.join(self._tempdir, filename))

        # Set time period after collecting all dates
        if min_date is None or max_date is None:
            logger.warning(f"No valid dates found for {country_name}")
            return None

        dataset.set_time_period(startdate=min_date, enddate=max_date)

        return dataset, filepaths

    def create_global_datasets(self, global_file_info: Dict) -> List[Dataset]:
        """Create global datasets from CSV files organized by model and year

        Args:
            global_file_info: {(model, year): {"filepath": path, "min_date": datetime, "max_date": datetime}}

        Returns:
            List of datasets (one per model) with resources by year
        """
        datasets = []

        # Organize files by model
        files_by_model = defaultdict(list)
        for (model, year), file_info in global_file_info.items():
            files_by_model[model].append((year, file_info))

        # Create one dataset per model
        for model in sorted(files_by_model.keys()):
            dataset_title = f"Global - Real Time {model.capitalize()} Prices"
            dataset_name = slugify(dataset_title)

            dataset = Dataset(
                {
                    "name": dataset_name,
                    "title": dataset_title,
                }
            )

            # Sort years in reverse order
            year_files = sorted(files_by_model[model], key=lambda x: x[0], reverse=True)

            model_min_date = None
            model_max_date = None

            # Add one resource per year
            for year, file_info in year_files:
                filepath = file_info["filepath"]

                # Set date range
                year_min = file_info["min_date"]
                year_max = file_info["max_date"]
                if year_min:
                    if model_min_date is None or year_min < model_min_date:
                        model_min_date = year_min
                if year_max:
                    if model_max_date is None or year_max > model_max_date:
                        model_max_date = year_max

                resource_name = f"Real Time {model.capitalize()} Prices {year}"
                resource_data = {
                    "name": resource_name,
                    "description": f"{self._configuration.get(f'description_{model}', '')} - data for {year}",
                    "format": "csv",
                }

                resource = dataset.add_update_resource(resource_data)
                resource.set_file_to_upload(filepath)

            # Set time period
            if model_min_date and model_max_date:
                dataset.set_time_period(
                    startdate=model_min_date, enddate=model_max_date
                )

            # Add tags
            dataset.add_tags(self._configuration["tags"])
            dataset.set_subnational(False)
            dataset.add_other_location("world")

            datasets.append(dataset)

        return datasets
