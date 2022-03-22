from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor
from settings import settings

from etl_components.etl_process import EtlFilmWorkProcess, EtlPersonProcess
from lib.logger import logger
from lib.utils import backoff
from storage.state import State
from storage.use_cases import update_storage_data_in_pipeline_table

PIPELINES = [
    EtlFilmWorkProcess,
    EtlPersonProcess,
]


@backoff(psycopg2.OperationalError)
def run_etl(dsl: dict, state: State, batch_size):
    """
    Синхронно по очереди запускает все пайплайны для отправки данных в Elastic
    """

    while True:
        for pipeline in PIPELINES:
            pipeline_name = pipeline.PIPELINE_NAME

            for table_spec in pipeline.TARGET_TABLE_SPECS:
                with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
                    while True:
                        current_table_name = table_spec.table_name
                        logger.info(f'Current table: {current_table_name}')

                        modified_row_ids = pipeline.postgres_producer(
                            pg_conn,
                            table_spec,
                            last_modified_dt=state[pipeline_name][current_table_name]['last_modified_dt'],
                            batch_limit=batch_size,
                            batch_offset=state[pipeline_name][current_table_name]['producer_offset']
                        )
                        logger.info(f'modified_row_ids: {modified_row_ids}')

                        if not modified_row_ids:
                            logger.info(f'Table {current_table_name} has been loaded to elastic')

                            update_storage_data_in_pipeline_table(
                                state,
                                pipline_name=pipeline_name,
                                table_name=current_table_name,
                                data={
                                    'last_modified_dt': datetime.now().isoformat(),
                                    'producer_offset': 0
                                }
                            )
                            break

                        while True:
                            film_work_ids = pipeline.postgres_enricher(
                                pg_conn,
                                table_spec,
                                modified_row_ids,
                                batch_limit=batch_size,
                                batch_offset=state[pipeline_name][current_table_name]['enricher_offset']
                            )
                            logger.info(f'film_work_ids: {film_work_ids}')

                            if not film_work_ids:
                                update_storage_data_in_pipeline_table(
                                    state,
                                    pipline_name=pipeline_name,
                                    table_name=current_table_name,
                                    data={'enricher_offset': 0}
                                )
                                break

                            merged_data = pipeline.postgres_merger(pg_conn, film_work_ids)
                            transformed_data = pipeline.transform(merged_data, settings.ES_INDEX)
                            pipeline.elasticsearch_loader(transformed_data)

                            update_storage_data_in_pipeline_table(
                                state,
                                pipline_name=pipeline_name,
                                table_name=current_table_name,
                                data={
                                    'enricher_offset': state[pipeline_name][current_table_name]['enricher_offset'] + batch_size
                                }
                            )
                            continue

                        update_storage_data_in_pipeline_table(
                            state,
                            pipline_name=pipeline_name,
                            table_name=current_table_name,
                            data={
                                'producer_offset': state[pipeline_name][current_table_name]['producer_offset'] + batch_size
                            }
                        )

                update_storage_data_in_pipeline_table(
                    state,
                    pipline_name=pipeline_name,
                    table_name=current_table_name,
                    data={'producer_offset': 0}
                )
