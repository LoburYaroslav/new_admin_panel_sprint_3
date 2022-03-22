def merged_fw_data_template_factory():
    template = {
        "id": None,
        "imdb_rating": None,
        "genre": [],
        "title": None,
        "description": None,
        "director": [],
        "actors_names": [],
        "writers_names": [],
        "actors": [],
        "writers": []
    }
    return template


def merged_person_data_template_factory():
    template = {
        "id": None,
        "full_name": None,
        "role": None,
        "film_ids": []
    }
    return template
