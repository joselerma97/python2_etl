from models.ETLStep import ETLStep
import os
import pandas as pd
import re

class Enrich(ETLStep):

    def __init__(self, countries, successor=None):
        super().__init__(successor)
        self.INPUT_FOLDER = "preproc"
        self.OUTPUT_FOLDER = "enrich"
        self.countries = countries
        self.countries = [country.strip() for country in countries]

    def create_folders(self):
        os.makedirs(f"data/{self.OUTPUT_FOLDER}", exist_ok=True)

    def read_data_sources(self):
        self.data_sources = {source.replace(".zip", ""): source for source in os.listdir(f"data/{self.INPUT_FOLDER}") if
                             source.endswith(".zip")}

    def _read_index(self):
        self.index = pd.read_csv(f"data/{self.INPUT_FOLDER}/index.zip")

    def _join_tables(self):
        self.tables_data = []
        for key, value in self.data_sources.items():
            data = pd.read_csv(f"data/{self.INPUT_FOLDER}/{key}.zip")
            if key not in ["index", "demographics"]:
                data = data.merge(self.index, on="location_key")
                print(f"Table {key} processed!")
            if key == "epidemiology" and len(self.countries) > 0:
                pattern = rf'({"|".join(self.countries)})'
                print(pattern)
                data = data[data.country_name.str.contains(pattern, regex=True)]
            self.tables_data.append((key, data))

    def action(self):
        self._read_index()
        self._join_tables()


