import os

from dotenv import load_dotenv

load_dotenv('../.env')

# DB
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
DB_PORT = os.environ.get('DB_PORT', 5432)

# ES
ES_HOST = os.environ.get('ES_HOST', '127.0.0.1')
ES_PORT = os.environ.get('ES_PORT', 9200)
ES_INDEX = os.environ.get('ES_INDEX', 'movies')
