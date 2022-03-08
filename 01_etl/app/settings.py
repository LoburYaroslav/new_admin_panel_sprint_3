from typing import Union

from pydantic import BaseSettings


class Settings(BaseSettings):
    DB_NAME = 'movies_database'
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str = '127.0.0.1'
    DB_PORT: Union[int, str] = 5432

    ES_HOST: str = '127.0.0.1'
    ES_PORT: Union[int, str] = 9200
    ES_INDEX: str = 'movies'


settings = Settings(_env_file='../.env', _env_file_encoding='utf-8')
