from models.ETLStep import ETLStep
import os
import pandas as pd
from ydata_profiling import ProfileReport


class Raw(ETLStep):

    def __init__(self, successor=None):
        super().__init__(successor)
        self.INPUT_FOLDER = "in"
        self.OUTPUT_FOLDER = "raw"
        self.REPORTS_FOLDER = "reports"

    def create_folders(self):
        os.makedirs(f"data/{self.OUTPUT_FOLDER}", exist_ok=True)
        os.makedirs(f"{self.REPORTS_FOLDER}", exist_ok=True)

    def read_data_sources(self):
        self.data_sources = {source.replace(".zip", ""): source for source in os.listdir(f"data/{self.INPUT_FOLDER}")
                             if source.endswith(".zip")}

    def _load_data_sources(self):
        self.datasets = {}
        for key, value in self.data_sources.items():
            self.datasets[key] = pd.read_csv(f"data/{self.INPUT_FOLDER}/{value}")

    def _execute_data_profiling(self):
        for df_name, df in self.datasets.items():
            ProfileReport(df, title=f"Profiling Report: {df_name}").to_file(f"{self.REPORTS_FOLDER}/{df_name}.html")

    def action(self):
        self._load_data_sources()
        self._execute_data_profiling()
        self._save()

    def _save(self):
        self.tables_data = list(self.datasets.items())
