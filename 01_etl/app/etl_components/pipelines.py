from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor
from settings import settings

from etl_components.etl_process import EtlFilmWorkProcess
from lib.logger import logger
from lib.utils import backoff
from postgres_components.constants import FILM_WORK_PIPELINE_TABLE_SPECS
from storage.state import State


@backoff(psycopg2.OperationalError)
def run_film_work_pipeline(dsl: dict, state: State, batch_size):
    """
    Пайплайн для заполнения индекса с фильмами
    """
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        while True:
            for table_spec in FILM_WORK_PIPELINE_TABLE_SPECS:
                while True:
                    current_table_name = table_spec.table_name
                    logger.info(f'Current table: {current_table_name}')

                    modified_row_ids = EtlFilmWorkProcess.postgres_producer(
                        pg_conn,
                        table_spec,
                        last_modified_dt=state[current_table_name]['last_modified_dt'],
                        batch_limit=batch_size,
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
                        film_work_ids = EtlFilmWorkProcess.postgres_enricher(
                            pg_conn,
                            table_spec,
                            modified_row_ids,
                            batch_limit=batch_size,
                            batch_offset=state[current_table_name]['enricher_offset']
                        )
                        logger.info(f'film_work_ids: {film_work_ids}')

                        if not film_work_ids:
                            state[current_table_name] = {
                                **state[current_table_name],
                                'enricher_offset': 0
                            }
                            break

                        merged_data = EtlFilmWorkProcess.postgres_merger(pg_conn, film_work_ids)
                        transformed_data = EtlFilmWorkProcess.transform(merged_data, settings.ES_INDEX)
                        EtlFilmWorkProcess.elasticsearch_loader(transformed_data)

                        state[current_table_name] = {
                            **state[current_table_name],
                            'enricher_offset': state[current_table_name]['enricher_offset'] + batch_size
                        }
                        continue

                    state[current_table_name] = {
                        **state[current_table_name],
                        'producer_offset': state[current_table_name]['producer_offset'] + batch_size
                    }

                state[current_table_name] = {
                    **state[current_table_name],
                    'producer_offset': 0
                }
