from abc import ABC, abstractmethod


class ETLStep(ABC):

    def __init__(self, successor=None):
        self.successor = successor
        self.data_sources = None
        self.tables_data = []
        self.OUTPUT_FOLDER = ""

    @abstractmethod
    def create_folders(self):
        pass

    @abstractmethod
    def read_data_sources(self):
        pass

    @abstractmethod
    def action(self):
        pass

    def _save_tables_data(self):
        for table, data in self.tables_data:
            data.to_csv(f"data/{self.OUTPUT_FOLDER}/{table}.zip", index=False)

    def handle_action(self):
        self.create_folders()
        self.read_data_sources()
        self.action()
        self._save_tables_data()
        if self.successor:
            self.successor.handle_action()
