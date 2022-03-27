import csv
import functools
import logging
from datetime import datetime

from vk_api import ApiError

log_file = "./logfile.log"
log_level = logging.INFO
logging.basicConfig(
    level=log_level, filename=log_file, filemode="a+", format="%(asctime)-15s %(levelname)-8s %(message)s"
)
logger = logging.getLogger("date_parser")
logger.addHandler(logging.StreamHandler())


ERROR_RATE_LIMIT_EXCEEDED = 29

# ----------------------------------------------------------------------------------------------------------------------
def from_unix_time(ts):
    return datetime.utcfromtimestamp(ts)


def unwind_value(d, prefix=''):
    prefix = f'{prefix}_' if prefix else prefix
    result = {}
    for k, v in d.items():
        if type(v) == dict:
            result.update(unwind_value(v, prefix=f'{prefix}{k}'))
        elif type(v) == list:
            for idx, elem in enumerate(v):
                if type(elem) == dict:
                    result.update(unwind_value(elem, prefix=f'{prefix}{k}_{idx}'))
                else:
                    result[f'{prefix}{k}_{idx}'] = elem
        else:
            result[f'{prefix}{k}'] = v
    return result


def read_users_from_csv(filename, search_count):
    with open(filename, 'r') as f:
        reader = csv.DictReader(f, quotechar='"', delimiter=',')
        users = [line for line in reader]
    chunk_size = search_count
    for x in range(0, len(users), chunk_size):
        users_chunk = users[x: x + chunk_size]
        yield len(users), users_chunk


def login_retrier(func):
    @functools.wraps(func)
    def inner(client, *args, **kwargs):
        try:
            result = func(client, *args, **kwargs)
            yield from result

        except ApiError as ex:
            if ex.code == ERROR_RATE_LIMIT_EXCEEDED:
                # client._obj = client._session.get_api()
                client.auth()
                result = func(client, *args, **kwargs)
                yield from result
            else:
                raise
    return inner
