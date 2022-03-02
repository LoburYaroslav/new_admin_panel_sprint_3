"""
Здесь описан главный класс EtlProcess, реализующий весь ETL процесс.
"""
from datetime import datetime
from typing import Tuple

from psycopg2.extensions import connection

from lib.logger import logger
from postgres_components.table_spec import AbstractPostgresTableSpec


class EtlProcess:
    """Класс реализующий ETL процесс"""

    @staticmethod
    def postgres_producer(
            pg_conn: connection,
            table_spec: AbstractPostgresTableSpec,
            last_modified_dt: datetime,
            batch_limit: int,
            batch_offset: int,
    ):
        """Возвращает идентификаторы кинопроизведений связанных с изменившимися сущностями"""
        logger.info(f'RUN postgres_producer for {table_spec.table_name}')

        return table_spec.get_modified_row_ids(
            pg_conn,
            last_modified_dt=last_modified_dt,
            limit=batch_limit,
            offset=batch_offset
        )

    @staticmethod
    def postgres_enricher(
            pg_conn: connection,
            table_spec: AbstractPostgresTableSpec,
            modified_row_ids: Tuple[str],
            batch_limit: int,
            batch_offset: int,
    ):
        """Возвращает film_work.id для измененных записей из таблиц genre и person"""
        logger.info(f'RUN postgres_enricher for {table_spec.table_name}')

        return table_spec.get_film_work_ids_by_modified_row_ids(
            pg_conn,
            modified_row_ids=modified_row_ids,
            limit=batch_limit,
            offset=batch_offset
        )

    def postgres_merger(self):
        """Вмерживает данные для последующей трансформации и отправки в Elastic"""

    def transform(self):
        """Преобразует входящие из postgres данные в вид подходящий для запроса в Elastic"""

    def elasticsearch_loader(self):
        """Отправляет запрос в Elastic"""
