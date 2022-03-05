import os
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import DictCursor

from etl_components.etl_process import EtlProcess
from lib.logger import logger
from postgres_components.constants import TABLE_SPECS
from storage.state import State
from storage.storage import JsonFileStorage

load_dotenv('../.env')

dsl = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'port': os.environ.get('DB_PORT', 5432),
    'options': "-c search_path=content,public"
}

BATCH_SIZE = 50

storage = JsonFileStorage('./storage.json')
state = State(storage)
# todo: надо валидировать тут storage.json

with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
    for i in [1, ]:
        for table_spec in TABLE_SPECS:
            print(table_spec.table_name, state[table_spec.table_name]['producer_offset'])

            modified_row_ids = EtlProcess.postgres_producer(
                pg_conn=pg_conn,
                table_spec=table_spec,
                last_modified_dt=datetime(2020, 1, 1),
                batch_limit=BATCH_SIZE,
                batch_offset=state[table_spec.table_name]['producer_offset']
            )
            logger.info(f'modified_row_ids: {modified_row_ids}')
            if not modified_row_ids:
                continue

            state[table_spec.table_name] = {
                **state[table_spec.table_name],
                'producer_offset': state[table_spec.table_name]['producer_offset'] + BATCH_SIZE
            }

            while True:
                film_work_ids = EtlProcess.postgres_enricher(
                    pg_conn=pg_conn,
                    table_spec=table_spec,
                    modified_row_ids=modified_row_ids,
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

                merged_data = EtlProcess.postgres_merger(pg_conn=pg_conn, film_work_ids=film_work_ids)

                state[table_spec.table_name] = {
                    **state[table_spec.table_name],
                    'enricher_offset': state[table_spec.table_name]['enricher_offset'] + BATCH_SIZE
                }
                continue

        state[table_spec.table_name] = {
            **state[table_spec.table_name],
            'producer_offset': 0
        }
