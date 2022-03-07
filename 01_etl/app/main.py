from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor
from settings import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER

from etl_components.etl_process import EtlProcess
from lib.logger import logger
from lib.utils import backoff
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

BATCH_SIZE = 2000

storage = JsonFileStorage('./storage.json')
state = State(storage)


# todo: надо валидировать тут storage.json

@backoff(psycopg2.DatabaseError)
def run_etl():
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        while True:
            for table_spec in TABLE_SPECS:
                while True:
                    current_table_name = table_spec.table_name
                    logger.info(f'Current table: {current_table_name}')

                    modified_row_ids = EtlProcess.postgres_producer(
                        pg_conn,
                        table_spec,
                        last_modified_dt=state[current_table_name]['last_modified_dt'],
                        batch_limit=BATCH_SIZE,
                        batch_offset=state[current_table_name]['producer_offset']
                    )
                    logger.info(f'modified_row_ids: {modified_row_ids}')

                    if not modified_row_ids:
                        logger.info(f'Table {current_table_name} has been loaded to elastic')
                        state[current_table_name] = {
                            **state[current_table_name],
                            'last_modified_dt': datetime.now().isoformat(),
                            'producer_offset': 0
                        }
                        break

                    while True:
                        film_work_ids = EtlProcess.postgres_enricher(
                            pg_conn,
                            table_spec,
                            modified_row_ids,
                            batch_limit=BATCH_SIZE,
                            batch_offset=state[current_table_name]['enricher_offset']
                        )
                        logger.info(f'film_work_ids: {film_work_ids}')

                        if not film_work_ids:
                            state[current_table_name] = {
                                **state[current_table_name],
                                'enricher_offset': 0
                            }
                            break

                        merged_data = EtlProcess.postgres_merger(pg_conn, film_work_ids)
                        transformed_data = EtlProcess.transform(merged_data)
                        EtlProcess.elasticsearch_loader(transformed_data)

                        state[current_table_name] = {
                            **state[current_table_name],
                            'enricher_offset': state[current_table_name]['enricher_offset'] + BATCH_SIZE
                        }
                        continue

                    state[current_table_name] = {
                        **state[current_table_name],
                        'producer_offset': state[current_table_name]['producer_offset'] + BATCH_SIZE
                    }

                state[current_table_name] = {
                    **state[current_table_name],
                    'producer_offset': 0
                }


if __name__ == '__main__':
    run_etl()
