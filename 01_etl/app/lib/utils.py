import time
from functools import wraps
from typing import Tuple, Type, Union

from lib.logger import logger


def backoff(
        exception_or_exceptions: Union[Type[Exception], Tuple[Type[Exception]]],
        start_sleep_time=0.1,
        factor=2,
        border_sleep_time=10,
):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :param exception_or_exceptions: исключение или исключения, при возникновении которых будем делать ретраи
    :return: результат выполнения функции
    """

    # Если не переданы конкретные исключения по дефолту ловим все.
    # Рекомендуется передавать конкретные исключения!
    catch_exceptions = exception_or_exceptions or Exception

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            retries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except catch_exceptions:
                    logger.exception('An known exception occurred, so retry will be done.')
                    sleep_time = start_sleep_time + factor ** retries
                    if sleep_time >= border_sleep_time:
                        sleep_time = border_sleep_time
                    time.sleep(sleep_time)
                    retries += 1

        return inner

    return func_wrapper
