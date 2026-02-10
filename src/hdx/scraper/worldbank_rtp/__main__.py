#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
import os
from datetime import datetime
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.location.country import Country
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
    logger.info(f"##### {_LOOKUP} version {__version__} ####")

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

            country_codes = sorted(Country.countriesdata()["countries"].keys())

            countries_created = 0
            countries_failed = 0

            try:
                for country_code in country_codes:
                    try:
                        model_data = pipeline.fetch_country_data(
                            country_code, models, max_records
                        )

                        if not any(model_data.values()):
                            continue

                        # Write to global CSVs
                        for model, records in model_data.items():
                            for record in records:
                                pipeline.write_global_record(
                                    model, record, current_year, global_years
                                )

                        # Create country dataset
                        result = pipeline.generate_dataset(country_code, model_data)
                        if result:
                            dataset, filepaths = result
                            dataset.update_from_yaml(
                                script_dir_plus_file(
                                    join("config", "hdx_dataset_static.yaml"),
                                    main,
                                )
                            )
                            dataset.create_in_hdx(
                                remove_additional_resources=False,
                                match_resource_order=False,
                                hxl_update=False,
                                updated_by_script=_UPDATED_BY_SCRIPT,
                                batch=info["batch"],
                            )
                            for filepath in filepaths:
                                os.remove(filepath)
                            countries_created += 1
                        else:
                            countries_failed += 1

                    except Exception as e:
                        logger.error(
                            f"Failed to create dataset for {country_code}: {e}"
                        )
                        countries_failed += 1

                    if max_countries and countries_created >= max_countries:
                        break
            finally:
                pipeline.close_global_files()

            # Create global datasets from accumulated CSVs
            global_created = 0
            global_failed = 0

            try:
                global_file_info = pipeline.get_global_file_info()
                global_datasets = pipeline.create_global_datasets(global_file_info)

                for dataset in global_datasets:
                    try:
                        if dataset:
                            dataset.update_from_yaml(
                                script_dir_plus_file(
                                    join("config", "hdx_dataset_static.yaml"),
                                    main,
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
                    except Exception as e:
                        logger.error(f"Failed to create global dataset: {e}")
                        global_failed += 1

            except Exception as e:
                logger.error(f"Failed to create global datasets: {e}")
                global_failed += 1

            logger.info(
                f"{countries_created} country, {global_created} global datasets"
            )


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
