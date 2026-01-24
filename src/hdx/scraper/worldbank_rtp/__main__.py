#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import csv
import logging
import time
from datetime import datetime
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.worldbank_rtp._version import __version__
from hdx.scraper.worldbank_rtp.pipeline import Pipeline

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-worldbank-rtp"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: Worldbank_rtp"


def main(
    save: bool = False,
    use_saved: bool = False,
    max_countries: int | None = None,
    max_records: int | None = None,
    global_years: int | None = None,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    start_time = time.time()

    logger.info(f"##### {_LOOKUP} version {__version__} ####")

    if max_countries:
        logger.info(f"TEST MODE: Processing only {max_countries} countries")
    if max_records:
        logger.info(f"TEST MODE: Processing only {max_records} records per model")
    if global_years:
        logger.info(f"Global datasets: Including last {global_years} years")
    else:
        logger.info("Global datasets: Including all available years")

    configuration = Configuration.read()
    User.check_current_user_write_access("hdx")

    with wheretostart_tempdir_batch(folder=_LOOKUP) as info:
        tempdir = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=tempdir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=tempdir,
                save=save,
                use_saved=use_saved,
            )

            models = ["food", "energy", "currency"]
            pipeline = Pipeline(configuration, retriever, tempdir)

            current_year = datetime.now().year
            global_csv_files = {}

            try:
                # Create country datasets, collect global data
                countries_created = 0
                countries_failed = 0

                for country_code, model_data in pipeline.aggregate_by_country(
                    models, max_records=max_records, max_countries=max_countries
                ):
                    try:
                        # Create country dataset
                        dataset = pipeline.generate_dataset(country_code, model_data)
                        if dataset:
                            dataset.update_from_yaml(
                                script_dir_plus_file(
                                    join("config", "hdx_dataset_static.yaml"), main
                                )
                            )
                            dataset.create_in_hdx(
                                remove_additional_resources=False,
                                match_resource_order=False,
                                hxl_update=False,
                                updated_by_script=_UPDATED_BY_SCRIPT,
                                batch=info["batch"],
                            )
                            countries_created += 1
                        else:
                            countries_failed += 1

                        # Write current country data to global CSV files by year
                        for model, records in model_data.items():
                            for record in records:
                                if record.get("DATES"):
                                    year = record["DATES"].year

                                    # For testing, only pull specific number of years
                                    if global_years and (
                                        current_year - year >= global_years
                                    ):
                                        continue

                                    # Create CSV file for this model-year if it doesn't exist
                                    key = (model, year)
                                    if key not in global_csv_files:
                                        filepath = join(
                                            tempdir, f"global_{model}_{year}.csv"
                                        )
                                        file_obj = open(
                                            filepath, "w", newline="", encoding="utf-8"
                                        )
                                        headers = list(record.keys())
                                        writer = csv.DictWriter(
                                            file_obj, fieldnames=headers
                                        )
                                        writer.writeheader()

                                        global_csv_files[key] = {
                                            "filepath": filepath,
                                            "file": file_obj,
                                            "writer": writer,
                                            "headers": headers,
                                        }

                                    # Write record to CSV
                                    global_csv_files[key]["writer"].writerow(record)

                    except Exception as e:
                        logger.error(
                            f"Failed to create dataset for {country_code}: {e}"
                        )
                        countries_failed += 1

                elapsed = (time.time() - start_time) / 60
                logger.info(
                    f"##### Country datasets complete in {elapsed:.1f} minutes #####"
                )
                logger.info(
                    f"Countries created: {countries_created}, failed: {countries_failed}"
                )

                # Close all CSV files
                for file_info in global_csv_files.values():
                    file_info["file"].close()

                global_created = 0
                global_failed = 0

                try:
                    global_datasets = pipeline.create_global_datasets_from_csv_files(
                        global_csv_files
                    )

                    for dataset in global_datasets:
                        try:
                            if dataset:
                                dataset.update_from_yaml(
                                    script_dir_plus_file(
                                        join("config", "hdx_dataset_static.yaml"), main
                                    )
                                )
                                dataset.create_in_hdx(
                                    remove_additional_resources=True,
                                    match_resource_order=False,
                                    hxl_update=False,
                                    updated_by_script=_UPDATED_BY_SCRIPT,
                                    batch=info["batch"],
                                )
                                global_created += 1
                                logger.info(
                                    f"Created global dataset: {dataset['title']}"
                                )
                        except Exception as e:
                            logger.error(f"Failed to create global dataset: {e}")
                            global_failed += 1

                except Exception as e:
                    logger.error(f"Failed to create global datasets: {e}")
                    global_failed += 1

                total_elapsed = (time.time() - start_time) / 60
                logger.info(f"##### COMPLETE in {total_elapsed:.1f} minutes #####")
                logger.info(
                    f"Country datasets: {countries_created} created, {countries_failed} failed"
                )
                logger.info(
                    f"Global datasets: {global_created} created, {global_failed} failed"
                )

            finally:
                # Make sure all files are closed
                for file_info in global_csv_files.values():
                    if file_info["file"] and not file_info["file"].closed:
                        file_info["file"].close()


if __name__ == "__main__":
    facade(
        main,
        # hdx_site="stage",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
