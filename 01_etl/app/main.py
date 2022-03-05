from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor
from settings import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER

from etl_components.etl_process import EtlProcess
from lib.logger import logger
from postgres_components.constants import TABLE_SPECS
from storage.state import State
from storage.storage import JsonFileStorage

dsl = {
    'dbname': DB_NAME,
    'user': DB_USER,
    'password': DB_PASSWORD,
    'host': DB_HOST,
    'port': DB_PORT,
    'options': "-c search_path=content,public"
}

BATCH_SIZE = 10

storage = JsonFileStorage('./storage.json')
state = State(storage)
# todo: надо валидировать тут storage.json

with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
    while True:
        for table_spec in TABLE_SPECS:
            while True:
                print(table_spec.table_name, state[table_spec.table_name]['producer_offset'])

                modified_row_ids = EtlProcess.postgres_producer(
                    pg_conn,
                    table_spec,
                    last_modified_dt=datetime(2020, 1, 1),
                    batch_limit=BATCH_SIZE,
                    batch_offset=state[table_spec.table_name]['producer_offset']
                )
                logger.info(f'modified_row_ids: {modified_row_ids}')

                if not modified_row_ids:
                    logger.info(f'Table {table_spec.table_name} has been loaded to elastic')
                    state[table_spec.table_name] = {
                        **state[table_spec.table_name],
                        'producer_offset': 0  # todo: и дату обновить
                    }
                    break

                while True:
                    film_work_ids = EtlProcess.postgres_enricher(
                        pg_conn,
                        table_spec,
                        modified_row_ids,
                        batch_limit=BATCH_SIZE,
                        batch_offset=state[table_spec.table_name]['enricher_offset']
                    )
                    logger.info(f'film_work_ids: {film_work_ids}')

                    if not film_work_ids:
                        state[table_spec.table_name] = {
                            **state[table_spec.table_name],
                            'enricher_offset': 0
                        }
                        break

                    merged_data = EtlProcess.postgres_merger(pg_conn, film_work_ids)
                    transformed_data = EtlProcess.transform(merged_data)
                    EtlProcess.elasticsearch_loader(transformed_data)

                    state[table_spec.table_name] = {
                        **state[table_spec.table_name],
                        'enricher_offset': state[table_spec.table_name]['enricher_offset'] + BATCH_SIZE
                    }
                    continue

                state[table_spec.table_name] = {
                    **state[table_spec.table_name],
                    'producer_offset': state[table_spec.table_name]['producer_offset'] + BATCH_SIZE
                }

            state[table_spec.table_name] = {
                **state[table_spec.table_name],
                'producer_offset': 0
            }
