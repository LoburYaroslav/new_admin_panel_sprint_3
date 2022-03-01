from enum import Enum

from postgres_components.table_spec import FilmWorkSpec


class MoviesDatabaseTableEnum(Enum):
    """Имена целевых таблиц в БД movies_database"""

    FILM_WORK = 'film_work'
    GENRE = 'genre'
    PERSON = 'person'
    GENRE_FILM_WORK = 'genre_film_work'
    PERSON_FILM_WORK = 'person_film_work'


TABLE_SPECS = [
    FilmWorkSpec,
]
