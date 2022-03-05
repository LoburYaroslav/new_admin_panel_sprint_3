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

    @staticmethod
    def postgres_merger(pg_conn: connection, film_work_ids: Tuple[str]):
        """Собирает данные для последующей трансформации и отправки в Elastic"""
        logger.info('RUN postgres_merger')
        with pg_conn.cursor() as cur:
            query = cur.mogrify(
                """
                SELECT
                    fw.id as fw_id,
                    fw.title,
                    fw.description,
                    fw.rating,
                    fw.type,
                    fw.created,
                    fw.modified,
                    pfw.role,
                    p.id,
                    p.full_name,
                    g.name
                FROM film_work fw
                LEFT JOIN person_film_work pfw ON pfw.film_work_id = fw.id
                LEFT JOIN person p ON p.id = pfw.person_id
                LEFT JOIN genre_film_work gfw ON gfw.film_work_id = fw.id
                LEFT JOIN genre g ON g.id = gfw.genre_id
                WHERE fw.id IN %(film_work_ids)s;
                """,
                {'film_work_ids': film_work_ids, }
            )
            cur.execute(query)

    def transform(self):
        """Преобразует входящие из postgres данные в вид подходящий для запроса в Elastic"""

    def elasticsearch_loader(self):
        """Отправляет запрос в Elastic"""
