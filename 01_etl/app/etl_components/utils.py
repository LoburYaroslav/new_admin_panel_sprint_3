def merged_fw_data_template_factory():
    template = {
        "id": None,
        "imdb_rating": None,
        "genres": [],
        "title": None,
        "description": None,
        "director": [],
        "actors_names": [],
        "writers_names": [],
        "actors": [],
        "writers": [],
        "directors": [],
    }
    return template


def merged_person_data_template_factory():
    template = {
        "id": None,
        "full_name": None,
        "roles": [],
        "film_ids": []
    }
    return template


def merged_genre_data_template_factory():
    template = {
        "id": None,
        "name": None,
    }
    return template
