from ydata_profiling import ProfileReport

from models.ETLStep import ETLStep
import os
import pandas as pd


class Aggregate(ETLStep):

    def __init__(self, macro_name):
        super().__init__(None)
        self.maro_name = macro_name
        self.INPUT_FOLDER = "enrich"
        self.OUTPUT_FOLDER = "aggregate"
        self.REPORTS_FOLDER = "reports"

    def create_folders(self):
        os.makedirs(f"data/{self.OUTPUT_FOLDER}", exist_ok=True)

    def read_data_sources(self):
        self.data_sources = {source.replace(".zip", ""): source for source in os.listdir(f"data/{self.INPUT_FOLDER}") if
                             source.endswith(".zip")}

    def _create_base_table(self):
        self.data = pd.read_csv(f"data/{self.INPUT_FOLDER}/epidemiology.zip")

    def _create_macro_table(self):
        demographics = pd.read_csv(f"data/{self.INPUT_FOLDER}/demographics.zip")
        self.data = self.data.merge(demographics, on=["location_key"], how="left")

        health = pd.read_csv(f"data/{self.INPUT_FOLDER}/health.zip")
        self.data = self.data.merge(health, on=["location_key", "country_name"], how="left")

        hospitalizations = pd.read_csv(f"data/{self.INPUT_FOLDER}/hospitalizations.zip")
        self.data = self.data.merge(hospitalizations, on=["location_key", "country_name", "date"], how="left")

        vaccinations = pd.read_csv(f"data/{self.INPUT_FOLDER}/vaccinations.zip")
        self.data = self.data.merge(vaccinations, on=["location_key", "country_name", "date"], how="left")

    def _missing_values_action(self):
        self.data = self.data.fillna(
            self.data.select_dtypes(include=['float64', 'int64']).mean()
        )

    def _create_report(self):
        ProfileReport(self.data, title="Profiling Report").to_file(f"{self.REPORTS_FOLDER}/aggregation.html")

    def _save(self):
        self.tables_data.append((self.maro_name, self.data))

    def action(self):
        self._create_base_table()
        self._create_macro_table()
        self._missing_values_action()
        self._create_report()
        self._save()
