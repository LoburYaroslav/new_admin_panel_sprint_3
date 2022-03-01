"""
Здесь описан главный класс EtlProcess, реализующий весь ETL процесс.
"""
from postgres_components.constants import MoviesDatabaseTableEnum


class EtlProcess:
    """Класс реализующий ETL процесс"""

    def __init__(self):
        ...

    def postgres_producer(self, table_name: MoviesDatabaseTableEnum):
        """Ходит в отдельные таблицы бд movies_database и вытаскивает изменившуюся информацию"""

    def postgres_enricher(self):
        """При необходимости обогащает данными таблицы genre и person"""

    def postgres_merger(self):
        """Вмерживает данные для последующей трансформации и отправки в Elastic"""

    def transform(self):
        """Преобразует входящие из postgres данные в вид подходящий для запроса в Elastic"""

    def elasticsearch_loader(self):
        """Отправляет запрос в Elastic"""
