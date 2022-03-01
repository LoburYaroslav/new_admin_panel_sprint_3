from storage.storage import BaseStorage


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value) -> None:
        """Установить состояние для определённого ключа"""
        self.storage.save_state({
            key: value
        })

    def get_state(self, key: str):
        """Получить состояние по определённому ключу"""
        current_state = self.storage.retrieve_state()
        return current_state.get(key)
