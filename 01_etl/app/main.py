from settings import settings

from etl_components.pipelines import run_etl
from storage.state import State
from storage.storage import JsonFileStorage

dsl = {
    'dbname': settings.DB_NAME,
    'user': settings.DB_USER,
    'password': settings.DB_PASSWORD,
    'host': settings.DB_HOST,
    'port': settings.DB_PORT,
    'options': "-c search_path=content,public"
}

BATCH_SIZE = 500

storage = JsonFileStorage('./storage.json')
state = State(storage)

if __name__ == '__main__':
    run_etl(dsl, state, BATCH_SIZE)
