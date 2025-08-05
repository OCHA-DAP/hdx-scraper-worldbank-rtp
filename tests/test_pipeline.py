from os.path import join

from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.worldbank_rtp.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestWorldbank_rtp",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                models = ["food", "energy", "currency"]
                pipeline = Pipeline(configuration, retriever, tempdir)
                for country_code, model_data in pipeline.aggregate_by_country(
                    models, max_records=10
                ):
                    if country_code != "AFG":
                        continue  # Only test Afghanistan

                    dataset = pipeline.generate_dataset(country_code, model_data)

                    if dataset:
                        dataset.update_from_yaml(
                            path=join(config_dir, "hdx_dataset_static.yaml")
                        )

                        assert dataset == {
                            "name": "afghanistan-real-time-prices",
                            "title": "Afghanistan - Real Time Prices",
                            "dataset_date": "[2007-01-01T00:00:00 TO 2025-07-01T23:59:59]",
                            "tags": [
                                {
                                    "name": "energy",
                                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                                },
                                {
                                    "name": "food security",
                                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                                },
                            ],
                            "license_id": "cc-by",
                            "methodology": "Registry",
                            "dataset_source": "World Bank",
                            "groups": [{"name": "afg"}],
                            "package_creator": "HDX Data Systems Team",
                            "private": False,
                            "maintainer": "fdbb8e79-f020-4039-ab3a-9adb482273b8",
                            "owner_org": "905a9a49-5325-4a31-a9d7-147a60a8387c",
                            "data_update_frequency": 7,
                            "subnational": "1",
                            "caveats": None,
                            "notes": "Real Time Prices (RTP) is a live dataset compiled and updated "
                            "weekly by the World Bank Development Economics Data Group (DECDG) "
                            "using a combination of direct price measurement and Machine "
                            "Learning estimation of missing price data. The historical and "
                            "current estimates are based on price information gathered from the "
                            "World Food Program (WFP), UN-Food and Agricultural Organization "
                            "(FAO), select National Statistical Offices, and are continually "
                            "updated and revised as more price information becomes available. "
                            "Real-time exchange rate data used in this process are from official "
                            "and public sources.",
                        }

                        resources = dataset.get_resources()
                        assert resources == [
                            {
                                "description": "Modeled monthly energy price estimates by product and market "
                                "(RTEP dataset)",
                                "format": "csv",
                                "name": "Real Time Energy Prices for Afghanistan",
                                "resource_type": "file.upload",
                                "url_type": "upload",
                            },
                            {
                                "description": "Modeled monthly currency exchange rate estimates by market "
                                "(RTFX dataset)",
                                "format": "csv",
                                "name": "Real Time Currency Prices for Afghanistan",
                                "resource_type": "file.upload",
                                "url_type": "upload",
                            },
                        ]

                    break
