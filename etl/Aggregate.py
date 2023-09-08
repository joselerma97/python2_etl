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
        self.data = self.data.groupby(["week", "country_name"])[
            ["new_confirmed", "new_deceased", "new_deceased_confirmed_ratio"]].median()

    def _create_macro_table(self):
        self.data = self.data.reset_index()

        health = pd.read_csv(f"data/{self.INPUT_FOLDER}/health.zip").groupby("country_name")[["life_expectancy"]].median()
        self.data = self.data.merge(health, on="country_name", how="left")

        hospitalizations = pd.read_csv(f"data/{self.INPUT_FOLDER}/hospitalizations.zip")
        hospitalizations["week"] = pd.DatetimeIndex(hospitalizations.date).to_period("W")
        hospitalizations["week"] = hospitalizations.week.astype(str)
        hospitalizations = hospitalizations.groupby(["week", "country_name"])[
            ["new_hospitalized_patients", "cumulative_hospitalized_patients", "current_hospitalized_patients",
             "current_intensive_care_patients"]].median()
        self.data = self.data.merge(hospitalizations, on=["week", "country_name"], how="left")

        vaccinations = pd.read_csv(f"data/{self.INPUT_FOLDER}/vaccinations.zip")
        vaccinations["week"] = pd.DatetimeIndex(vaccinations.date).to_period("W")
        vaccinations["week"] = vaccinations.week.astype(str)
        vaccinations = vaccinations.groupby(["week", "country_name"])[
            ["new_persons_fully_vaccinated", "cumulative_persons_fully_vaccinated"]].median()
        self.data = self.data.merge(vaccinations, on=["week", "country_name"], how="left")

        self.data.set_index(["week", "country_name"], inplace=True)

    def _missing_values_action(self):
        self.data = self.data.fillna(
            self.data.select_dtypes(include=['float64', 'int64']).median()
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
