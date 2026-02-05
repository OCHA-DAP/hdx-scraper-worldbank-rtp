#!/usr/bin/python
"""Worldbank_rtp scraper"""

import csv as csv_module
import logging
import os
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
        self,
        models: List,
        max_records: Optional[int] = None,
        max_countries: Optional[int] = None,
    ) -> Iterator[Tuple]:
        """
        Two-phase disk-based aggregation to avoid holding all records in memory

        Phase 1: Stream records from API and write to per-country CSV files
        Phase 2: Process one country at a time, parse dates, and yield
        """
        # Track which (country, model) combos have data and their headers
        country_models = defaultdict(dict)  # {country: {model: headers_list}}
        total_records = 0

        # Phase 1: Fetch & write to temp CSV files
        for model in models:
            open_writers = {}  # {country_code: (file_obj, csv_writer)}

            try:
                for record in self.fetch_data(model, max_records):
                    country_code = record.get("ISO3", "Unknown")

                    if country_code not in open_writers:
                        headers = list(record.keys())
                        country_models[country_code][model] = headers
                        filepath = os.path.join(
                            self._tempdir,
                            f"temp_{country_code}_{model}.csv",
                        )
                        f = open(filepath, "w", newline="", encoding="utf-8")
                        writer = csv_module.DictWriter(f, fieldnames=headers)
                        writer.writeheader()
                        open_writers[country_code] = (f, writer)

                    open_writers[country_code][1].writerow(record)
                    total_records += 1

            except Exception as e:
                logger.error(f"Error fetching {model}: {e}")
                for f, _ in open_writers.values():
                    f.close()
                raise
            finally:
                for f, _ in open_writers.values():
                    f.close()

        all_countries = list(country_models.keys())
        logger.info(
            f"Fetched {total_records} records across {len(all_countries)} countries"
        )

        # Phase 2: Process one country at a time and yield
        countries_yielded = 0
        try:
            for country_code in all_countries:
                model_data = {}
                for model, headers in country_models[country_code].items():
                    filepath = os.path.join(
                        self._tempdir,
                        f"temp_{country_code}_{model}.csv",
                    )
                    records = []
                    with open(filepath, "r", encoding="utf-8") as f:
                        reader = csv_module.DictReader(f)
                        for row in reader:
                            # Parse DATES string to datetime
                            try:
                                row["DATES"] = parse_date(row.get("DATES"))
                            except Exception as e:
                                logger.warning(f"Failed to parse date for record: {e}")
                                row["DATES"] = None
                            records.append(row)
                    model_data[model] = records

                    # Remove temp file
                    os.remove(filepath)

                if any(model_data.values()):
                    yield country_code, model_data
                    countries_yielded += 1

                if max_countries and countries_yielded >= max_countries:
                    break
        finally:
            # Clean up any remaining temp files
            for cc, models_dict in country_models.items():
                for mdl in models_dict:
                    fp = os.path.join(
                        self._tempdir,
                        f"temp_{cc}_{mdl}.csv",
                    )
                    if os.path.exists(fp):
                        os.remove(fp)

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

    def write_global_record(
        self,
        model: str,
        record: Dict,
        current_year: int,
        global_years: Optional[int] = None,
    ) -> None:
        """Write record to the appropriate global CSV file

        Opens per-(model, year) CSV writers and tracks min/max dates
        incrementally.
        """
        date = record.get("DATES")
        if not date:
            return

        if isinstance(date, datetime):
            year = date.year
        else:
            return

        if global_years and (current_year - year >= global_years):
            return

        key = (model, year)

        if not hasattr(self, "_global_writers"):
            self._global_writers = {}
            self._global_date_ranges = {}

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
                "min_date": date,
                "max_date": date,
            }

        self._global_writers[key]["writer"].writerow(record)

        dr = self._global_date_ranges[key]
        if date < dr["min_date"]:
            dr["min_date"] = date
        if date > dr["max_date"]:
            dr["max_date"] = date

    def close_global_files(self) -> None:
        """Close all open global CSV file handles"""
        if not hasattr(self, "_global_writers"):
            return
        for info in self._global_writers.values():
            f = info["file"]
            if f and not f.closed:
                f.close()

    def get_global_file_info(self) -> Dict:
        """Return pre-computed file info for global datasets

        Returns:
            Dict of {(model, year): {"filepath": ..., "min_date": ..., "max_date": ...}}
        """
        if not hasattr(self, "_global_writers"):
            return {}
        result = {}
        for key, writer_info in self._global_writers.items():
            result[key] = {
                "filepath": writer_info["filepath"],
                "min_date": self._global_date_ranges[key]["min_date"],
                "max_date": self._global_date_ranges[key]["max_date"],
            }
        return result

    def create_global_datasets_from_csv_files(
        self, global_file_info: Dict
    ) -> List[Dataset]:
        """
        Create global datasets from CSV files organized by model and year

        Args:
            global_file_info: Dictionary of {(model, year): {"filepath": path, "min_date": datetime, "max_date": datetime}}

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

            # Sort years in reverse order (most recent first)
            year_files = sorted(files_by_model[model], key=lambda x: x[0], reverse=True)

            model_min_date = None
            model_max_date = None

            # Add one resource per year
            for year, file_info in year_files:
                filepath = file_info["filepath"]

                # Update overall date range from pre-computed values
                yr_min = file_info["min_date"]
                yr_max = file_info["max_date"]
                if yr_min:
                    if model_min_date is None or yr_min < model_min_date:
                        model_min_date = yr_min
                if yr_max:
                    if model_max_date is None or yr_max > model_max_date:
                        model_max_date = yr_max

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
