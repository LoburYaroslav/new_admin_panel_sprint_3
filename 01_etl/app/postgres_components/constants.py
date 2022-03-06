from enum import Enum

from postgres_components.table_spec import FilmWorkSpec, GenreSpec, PersonFilmWorkSpec, PersonSpec


class PersonRoleEnum(Enum):
    """Роли участников кино"""

    ACTOR = 'actor'
    DIRECTOR = 'director'
    WRITER = 'writer'


# по этим таблицам осуществляется обход и дальнейшая загрузка в elastic
TABLE_SPECS = [
    FilmWorkSpec,
    PersonFilmWorkSpec,
    PersonSpec,
    GenreSpec
]
