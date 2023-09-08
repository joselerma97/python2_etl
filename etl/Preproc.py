from models.ETLStep import ETLStep
import os
import pandas as pd


class Preproc(ETLStep):

    def __init__(self, start, end, successor=None):
        super().__init__(successor)
        self.INPUT_FOLDER = "raw"
        self.OUTPUT_FOLDER = "preproc"
        self.start = start
        self.end = end

    def create_folders(self):
        os.makedirs(f"data/{self.OUTPUT_FOLDER}", exist_ok=True)

    def read_data_sources(self):
        self.data_sources = {source.replace(".zip", ""): source for source in os.listdir(f"data/{self.INPUT_FOLDER}") if
                             source.endswith(".zip")}

    def _epidemiology_load(self):
        self.epidemiology_key = "epidemiology"
        self.data = pd.read_csv(f"data/{self.INPUT_FOLDER}/{self.epidemiology_key}.zip", parse_dates=["date"])
        self.data = self.data[(self.data.date >= self.start) & (self.data.date <= self.end)]

    def _epidemiology_impute_missing_values(self):
        self.data = self.data.fillna({
            "new_confirmed": 0,
            "new_deceased": 0
        })

    def _epidemiology_new_derived_variables(self):
        self.data["week"] = pd.DatetimeIndex(self.data.date).to_period("W")
        self.data["new_deceased_confirmed_ratio"] = self.data["new_deceased"] / self.data["new_confirmed"]

    def _epidemiology_pick_desired_columns(self):
        self.data = self.data[["date", "week", "location_key", "new_confirmed", "new_deceased",
                               "new_deceased_confirmed_ratio"]]

    def _epidemiology_save(self):
        self.tables_data.append((self.epidemiology_key, self.data))

    def _get_null_columns_info(self, data):
        null_counts = data.isnull().sum()
        notnull_counts = data.notnull().sum()
        result_df = pd.DataFrame({
            'null_values': null_counts,
            'notnull_values': notnull_counts
        })
        return result_df

    def _clean_preproc_columns(self, data):
        columns_info = self._get_null_columns_info(data)
        data_null_columns = columns_info[columns_info.notnull_values == 0]
        result = data.drop(columns=data_null_columns.index.tolist())
        result = result.fillna(0)
        return result

    def _proc_tables_remaining(self):
        tables = ["demographics", "health", "hospitalizations", "index", "vaccinations"]
        self.tables_data = []
        for table in tables:
            data = pd.read_csv(f"data/{self.INPUT_FOLDER}/{table}.zip")
            data = self._clean_preproc_columns(data)
            if table == "index":
                data = data[["location_key", "country_name"]]
            self.tables_data.append((table, data))

    def action(self):
        self._epidemiology_load()
        self._epidemiology_impute_missing_values()
        self._epidemiology_new_derived_variables()
        self._epidemiology_pick_desired_columns()
        self._epidemiology_save()

        self._proc_tables_remaining()
