"""
Здесь описан главный класс EtlProcess, реализующий весь ETL процесс.
"""
from abc import abstractmethod
from collections import defaultdict
from datetime import datetime
from typing import Iterable, List, Tuple, Union

from elasticsearch import Elasticsearch, helpers
from psycopg2.extensions import connection
from pydantic import ValidationError
from settings import settings

from etl_components.models import FilmWorkMergedData, GenreMergedData, PersonMergedData
from etl_components.utils import (merged_fw_data_template_factory, merged_genre_data_template_factory,
                                  merged_person_data_template_factory)
from lib.logger import logger
from postgres_components.constants import PersonRoleEnum
from postgres_components.table_spec import (AbstractPostgresTableSpec, FilmWorkSpec, GenreSpec, PersonFilmWorkSpec,
                                            PersonSpec)


class EtlProcess:
    """Общий класс реализующий ETL процесс"""

    @staticmethod
    def postgres_producer(
        pg_conn: connection,
        table_spec: AbstractPostgresTableSpec,
        last_modified_dt: Union[datetime, str],  # либо дата, либо строка с датой в формате iso
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
    @abstractmethod
    def postgres_enricher(
        pg_conn: connection,
        table_spec: AbstractPostgresTableSpec,
        modified_row_ids: Tuple[str],
        batch_limit: int,
        batch_offset: int,
    ):
        """
        Обогащает записи данными из many-to-many таблиц при необходимости
        Для каждого ETL процесса этот метод уникальный
        """

    @staticmethod
    @abstractmethod
    def postgres_merger(pg_conn: connection, film_work_ids: Tuple[str]):
        """
        Собирает и мержит данные для последующей трансформации и отправки в Elastic
        Для каждого ETL процесса этот метод уникальный
        """

    @staticmethod
    def transform(merged_data: Iterable[dict], index_name: str) -> List[dict]:
        """Преобразует входящие из postgres данные в вид подходящий для запроса в Elastic"""
        logger.info(f'RUN transform: {len(merged_data)} will be transformed')
        actions = [
            {
                "_index": index_name,
                "_id": item['id'],
                "_source": item
            }
            for item in merged_data
        ]
        return actions

    @staticmethod
    def elasticsearch_loader(actions: List[dict]):
        """Отправляет запрос в Elastic"""
        logger.info(f'RUN elasticsearch_loader: {len(actions)} will be send')

        es_client = Elasticsearch(f'{settings.ES_HOST}:{settings.ES_PORT}')
        helpers.bulk(es_client, actions)


class EtlFilmWorkProcess(EtlProcess):
    """Класс реализующий ETL процесс для загрузки фильмов"""

    PIPELINE_NAME = 'film_work_pipeline'
    INDEX_NAME = 'movies'
    TARGET_TABLE_SPECS = (FilmWorkSpec, PersonFilmWorkSpec, PersonSpec, GenreSpec)

    @staticmethod
    def postgres_enricher(
        pg_conn: connection,
        table_spec: AbstractPostgresTableSpec,
        modified_row_ids: Tuple[str],
        batch_limit: int,
        batch_offset: int,
    ):
        """Возвращает film_work.id для измененных записей из таблиц genre и person"""
        logger.info(f'RUN postgres_enricher for {table_spec.table_name}: {len(modified_row_ids)} will be enriched')

        return table_spec.get_film_work_ids_by_modified_row_ids(
            pg_conn,
            modified_row_ids=modified_row_ids,
            limit=batch_limit,
            offset=batch_offset
        )

    @staticmethod
    def postgres_merger(pg_conn: connection, film_work_ids: Tuple[str]):
        """Собирает и мержит данные для последующей трансформации и отправки в Elastic"""
        logger.info(f'RUN postgres_merger: {len(film_work_ids)} will be merged')
        with pg_conn.cursor() as cur:
            query = cur.mogrify(
                """
                SELECT
                    fw.id as fw_id,
                    fw.title,
                    fw.description,
                    fw.rating,
                    fw.type,
                    pfw.role,
                    p.id,
                    p.full_name,
                    g.id as genre_id,
                    g.name as genre_name
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

            raw_data = tuple(dict(i) for i in cur.fetchall())
            merged_data = defaultdict(merged_fw_data_template_factory)

            for item in raw_data:
                fw_id = item['fw_id']
                try:
                    valid_item = FilmWorkMergedData(**item)
                except ValidationError:
                    logger.exception(f'ValidationError for film {fw_id}')
                    continue

                merged_data[fw_id]['id'] = fw_id
                merged_data[fw_id]['imdb_rating'] = valid_item.rating
                merged_data[fw_id]['title'] = valid_item.title
                merged_data[fw_id]['description'] = valid_item.description

                if valid_item.genre_id not in [i['id'] for i in merged_data[fw_id]['genres']]:
                    merged_data[fw_id]['genres'].append({
                        'id': valid_item.genre_id,
                        'name': valid_item.genre_name,
                    })

                if valid_item.role == PersonRoleEnum.ACTOR.value \
                    and valid_item.id not in [i['id'] for i in merged_data[fw_id]['actors']]:
                    merged_data[fw_id]['actors'].append({
                        'id': valid_item.id,
                        'name': valid_item.full_name
                    })
                    merged_data[fw_id]['actors_names'].append(valid_item.full_name)

                if valid_item.role == PersonRoleEnum.WRITER.value \
                    and valid_item.id not in [i['id'] for i in merged_data[fw_id]['writers']]:
                    merged_data[fw_id]['writers'].append({
                        'id': valid_item.id,
                        'name': valid_item.full_name
                    })
                    merged_data[fw_id]['writers_names'].append(valid_item.full_name)

                if valid_item.role == PersonRoleEnum.DIRECTOR.value \
                    and valid_item.id not in [i['id'] for i in merged_data[fw_id]['directors']]:
                    merged_data[fw_id]['directors'].append({
                        'id': valid_item.id,
                        'name': valid_item.full_name
                    })
                    merged_data[fw_id]['director'].append(valid_item.full_name)

            return merged_data.values()


class EtlPersonProcess(EtlProcess):
    """Класс реализующий ETL процесс для загрузки персон"""

    PIPELINE_NAME = 'person_pipeline'
    INDEX_NAME = 'persons'
    TARGET_TABLE_SPECS = (PersonFilmWorkSpec, PersonSpec)

    @staticmethod
    def postgres_enricher(
        pg_conn: connection,
        table_spec: Union[PersonFilmWorkSpec, PersonSpec],
        modified_row_ids: Tuple[str],
        batch_limit: int,
        batch_offset: int,
    ):
        """Возвращает film_work.id для измененных записей из таблиц genre и person"""
        logger.info(f'RUN postgres_enricher for {table_spec.table_name}: {len(modified_row_ids)} will be enriched')

        return table_spec.get_person_ids(
            pg_conn,
            modified_row_ids=modified_row_ids,
            limit=batch_limit,
            offset=batch_offset
        )

    @staticmethod
    def postgres_merger(pg_conn: connection, person_ids: Tuple[str]):
        """Собирает и мержит данные для последующей трансформации и отправки в Elastic"""
        logger.info(f'RUN postgres_merger: {len(person_ids)} will be merged')
        with pg_conn.cursor() as cur:
            query = cur.mogrify(
                """
                SELECT
                    p.id,
                    pfw.role,
                    p.full_name,
                    fw.id as film_id
                FROM person p
                LEFT JOIN person_film_work pfw ON pfw.person_id = p.id
                LEFT JOIN film_work fw ON fw.id = pfw.film_work_id

                WHERE p.id IN %(person_ids)s;
                """,
                {'person_ids': person_ids, }
            )
            cur.execute(query)

            raw_data = tuple(dict(i) for i in cur.fetchall())
            merged_data = defaultdict(merged_person_data_template_factory)

            for item in raw_data:
                person_id = item['id']
                try:
                    valid_item = PersonMergedData(**item)
                except ValidationError:
                    logger.exception(f'ValidationError for film {person_id}')
                    continue

                merged_data[person_id]['id'] = person_id
                merged_data[person_id]['full_name'] = valid_item.full_name

                if valid_item.role not in merged_data[person_id]['roles']:
                    merged_data[person_id]['roles'].append(valid_item.role)

                if valid_item.film_id not in merged_data[person_id]['film_ids']:
                    merged_data[person_id]['film_ids'].append(valid_item.film_id)

            return merged_data.values()


class EtlGenreProcess(EtlProcess):
    """Класс реализующий ETL процесс для загрузки жанров"""

    PIPELINE_NAME = 'genre_pipeline'
    INDEX_NAME = 'genres'
    TARGET_TABLE_SPECS = (GenreSpec,)

    @staticmethod
    def postgres_enricher(
        pg_conn: connection,
        table_spec: GenreSpec,
        modified_row_ids: Tuple[str],
        batch_limit: int,
        batch_offset: int,
    ):
        """Возвращает film_work.id для измененных записей из таблиц genre и person"""
        logger.info(f'RUN postgres_enricher for {table_spec.table_name}: {len(modified_row_ids)} will be enriched')

        return table_spec.get_genre_ids(
            pg_conn,
            modified_row_ids=modified_row_ids,
            limit=batch_limit,
            offset=batch_offset
        )

    @staticmethod
    def postgres_merger(pg_conn: connection, genres_id: Tuple[str]):
        """Собирает и мержит данные для последующей трансформации и отправки в Elastic"""
        logger.info(f'RUN postgres_merger: {len(genres_id)} will be merged')
        with pg_conn.cursor() as cur:
            query = cur.mogrify(
                """
                SELECT id, name
                FROM genre p
                WHERE p.id IN %(genres_id)s;
                """,
                {'genres_id': genres_id, }
            )
            cur.execute(query)

            raw_data = tuple(dict(i) for i in cur.fetchall())
            merged_data = defaultdict(merged_genre_data_template_factory)

            for item in raw_data:
                genre_id = item['id']
                try:
                    valid_item = GenreMergedData(**item)
                except ValidationError:
                    logger.exception(f'ValidationError for film {genre_id}')
                    continue

                merged_data[genre_id]['id'] = genre_id
                merged_data[genre_id]['name'] = valid_item.name

            return merged_data.values()
