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

    BATCH_SIZE = 500

    @property
    def dsl(self):
        return {
            'dbname': self.DB_NAME,
            'user': self.DB_USER,
            'password': self.DB_PASSWORD,
            'host': self.DB_HOST,
            'port': self.DB_PORT,
            'options': "-c search_path=content,public"
        }


settings = Settings(_env_file='.env', _env_file_encoding='utf-8')
