from storage.state import State


def update_storage_data_in_pipeline_table(
    state: State,
    pipline_name: str,
    table_name: str,
    data: dict
):
    """
    Обертка для удобного сохранения данных в storage.json

    :param state: Объект для доступа к состоянию
    :param pipline_name: имя пайплайна в котором нужно что то изменить
    :param table_name: имя таблицы из пайплайна в которой нужно обновить данные
    :param data: dict с полями которые будут обновлены в таблице пайплайна
    :return: None
    """
    state[pipline_name] = {
        **state[pipline_name],
        table_name: {
            **state[pipline_name][table_name],
            **data
        }
    }
