import os
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from etl_components.etl_process import EtlProcess
from postgres_components.constants import TABLE_SPECS
from psycopg2.extras import DictCursor

load_dotenv('../.env.db')

dsl = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'port': os.environ.get('DB_PORT', 5432),
}

BATCH_SIZE = 3

storage = {
    'film_work': {
        'offset': 0
    }
}

with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
    for i in [1, 2, 3]:
        for table_spec in TABLE_SPECS:
            print(table_spec.table_name, storage[table_spec.table_name]['offset'])

            ids = EtlProcess.postgres_producer(
                pg_conn=pg_conn,
                table_spec=table_spec,
                last_modified_dt=datetime(2020, 1, 1),
                batch_limit=BATCH_SIZE,
                batch_offset=storage[table_spec.table_name]['offset']
            )
            print(ids)

            storage[table_spec.table_name]['offset'] += BATCH_SIZE

print(storage)
