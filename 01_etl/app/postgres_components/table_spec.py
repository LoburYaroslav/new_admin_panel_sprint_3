"""
Здесь описаны спецификации для получения изменившихся записей из таблиц.
А также спецификация для формирования структуры подходящей для отправки в Elastic.
"""
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Tuple

from psycopg2.extensions import connection


class PostgresTableSpec(ABC):

    @property
    @abstractmethod
    def table_name(self):
        """Имя таблицы в postgres"""

    @classmethod
    @abstractmethod
    def get_film_work_ids_by_modified_rows(
            cls,
            pg_conn: connection,
            last_modified_dt: datetime,
            limit: int,
            offset: int
    ) -> Tuple[str]:
        """
        Находит изменившиеся записи и возвращает связанные с ними идентификаторы сущностей из таблицы film_work
        """


class FilmWorkSpec(PostgresTableSpec):
    table_name = 'film_work'

    @classmethod
    def get_film_work_ids_by_modified_rows(
            cls,
            pg_conn: connection,
            last_modified_dt: datetime,
            limit: int,
            offset: int
    ) -> Tuple[str]:
        with pg_conn.cursor() as cur:
            query = cur.mogrify(
                f"""
                SELECT id
                FROM content.{cls.table_name}
                WHERE modified >= %(modified)s
                ORDER BY modified, id
                OFFSET %(offset)s
                LIMIT %(limit)s;
                """,
                {'modified': last_modified_dt, 'limit': limit, 'offset': offset}
            )
            cur.execute(query)

            return tuple(i[0] for i in cur.fetchall())
