"""
Здесь описаны спецификации для получения изменившихся записей из таблиц.
А также спецификация для формирования структуры подходящей для отправки в Elastic.
"""
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Tuple

from psycopg2.extensions import connection


class AbstractPostgresTableSpec(ABC):

    @property
    @abstractmethod
    def table_name(self):
        """Имя таблицы в postgres"""

    @classmethod
    @abstractmethod
    def get_modified_row_ids(
            cls,
            pg_conn: connection,
            last_modified_dt: datetime,
            limit: int,
            offset: int
    ) -> Tuple[str]:
        """
        Находит изменившиеся записи и возвращает их идентификаторы
        :param pg_conn: коннект к бд
        :param last_modified_dt: дата с которой надо начать смотреть обновления
        :param limit: ограничение по числу записей
        :param offset: смещение
        :return: Tuple с идентификаторами изменившихся строк
        """

    @classmethod
    @abstractmethod
    def get_film_work_ids_by_modified_row_ids(
            cls,
            pg_conn: connection,
            modified_row_ids: Tuple[str],
            limit: int,
            offset: int
    ) -> Tuple[str]:
        """
        По изменившимся идентификаторам находит идентификаторы кинопроизведений
        :param pg_conn: коннект к бд
        :param modified_row_ids: список идентификаторов изменившихся строк
        :param limit: ограничение по числу записей
        :param offset: смещение
        :return: Tuple с идентификаторами кинопроизведений, связанных с modified_row_ids
        """


class PostgresTableSpec(AbstractPostgresTableSpec):
    table_name: str = None
    join_clause: str = None
    film_work_id_field: str = 'film_work_id'

    @classmethod
    def get_modified_row_ids(
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
                FROM {cls.table_name}
                WHERE modified >= %(modified)s
                ORDER BY modified, id
                OFFSET %(offset)s
                LIMIT %(limit)s;
                """,
                {'modified': last_modified_dt, 'limit': limit, 'offset': offset}
            )
            cur.execute(query)

            return tuple(i[0] for i in cur.fetchall())

    @classmethod
    def get_film_work_ids_by_modified_row_ids(
            cls,
            pg_conn: connection,
            modified_row_ids: Tuple[str],
            limit: int,
            offset: int
    ) -> Tuple[str]:
        with pg_conn.cursor() as cur:
            query = cur.mogrify(
                f"""
                SELECT {cls.film_work_id_field}
                FROM {cls.table_name}
                """
                + cls.join_clause * bool(cls.join_clause) +
                f"""
                WHERE {cls.table_name}.id in %(modified)s
                ORDER BY {cls.table_name}.modified, {cls.table_name}.id
                OFFSET %(offset)s
                LIMIT %(limit)s;
                    """,
                {'modified': modified_row_ids, 'limit': limit, 'offset': offset}
            )
            cur.execute(query)

            return tuple(i[0] for i in cur.fetchall())


class FilmWorkSpec(PostgresTableSpec):
    table_name = 'film_work'

    @classmethod
    def get_film_work_ids_by_modified_row_ids(
            cls,
            pg_conn: connection,
            modified_row_ids: Tuple[str],
            limit: int,
            offset: int
    ) -> Tuple[str]:
        return modified_row_ids


class PersonFilmWorkSpec(AbstractPostgresTableSpec):
    table_name = 'person_film_work'
    film_work_id_field: str = 'film_work_id'

    # переопределил чтобы сразу возвращать id кинопроизведений и не ходить лишний раз в бд
    @classmethod
    def get_modified_row_ids(
            cls,
            pg_conn: connection,
            last_modified_dt: datetime,
            limit: int,
            offset: int
    ) -> Tuple[str]:
        with pg_conn.cursor() as cur:
            query = cur.mogrify(
                f"""
                SELECT {cls.film_work_id_field}
                FROM {cls.table_name}
                WHERE modified >= %(modified)s
                ORDER BY modified, id
                OFFSET %(offset)s
                LIMIT %(limit)s;
                """,
                {'modified': last_modified_dt, 'limit': limit, 'offset': offset}
            )
            cur.execute(query)

            return tuple(i[0] for i in cur.fetchall())

    @classmethod
    def get_film_work_ids_by_modified_row_ids(
            cls,
            pg_conn: connection,
            modified_row_ids: Tuple[str],
            limit: int,
            offset: int
    ) -> Tuple[str]:
        return modified_row_ids


class PersonSpec(PostgresTableSpec):
    table_name = 'person'
    join_clause = 'JOIN person_film_work ON person_film_work.person_id = person.id'
    film_work_id_field = 'film_work_id'


class GenreSpec(PostgresTableSpec):
    table_name = 'genre'
    join_clause = 'JOIN genre_film_work ON genre_film_work.genre_id = genre.id'
    film_work_id_field = 'film_work_id'
