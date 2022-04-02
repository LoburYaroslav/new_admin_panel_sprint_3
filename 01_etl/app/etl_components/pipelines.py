import psycopg2
from settings import settings

from etl_components.etl_process import EtlFilmWorkProcess, EtlGenreProcess, EtlPersonProcess
from etl_components.use_cases import process_pipeline
from lib.utils import backoff
from storage.state import State
from storage.storage import JsonFileStorage

PIPELINES = [
    EtlFilmWorkProcess,
    EtlPersonProcess,
    EtlGenreProcess,
]


@backoff(psycopg2.OperationalError)
def run_etl():
    """
    Синхронно по очереди запускает все пайплайны для отправки данных в Elastic
    """
    storage = JsonFileStorage('./storage.json')
    state = State(storage)

    while True:
        for pipeline in PIPELINES:
            process_pipeline(pipeline, settings.dsl, state, settings.BATCH_SIZE)
