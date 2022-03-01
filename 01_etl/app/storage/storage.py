import abc
import json
from typing import Optional


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path or './storage.json'

    def retrieve_state(self) -> dict:
        with open(self.file_path) as storage_file:
            raw_data = storage_file.read()
            return json.loads(raw_data) if raw_data else {}

    def save_state(self, state: dict) -> None:
        try:
            old_state = self.retrieve_state()
        except FileNotFoundError:
            old_state = {}

        with open(self.file_path, 'w') as storage_file:
            new_state = {**old_state, **state}
            json.dump(new_state, storage_file, indent=2)
