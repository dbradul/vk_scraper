import csv
import functools
import itertools
import logging
from datetime import datetime

from vk_api import ApiError

from models import VkClientProxy

log_file = "./logfile.log"
log_level = logging.INFO
logging.basicConfig(
    level=log_level, filename=log_file, filemode="a+", format="%(asctime)-15s %(levelname)-8s %(message)s"
)
logger = logging.getLogger("date_parser")
logger.addHandler(logging.StreamHandler())


ERROR_RATE_LIMIT_EXCEEDED = 29
class RateLimitException(Exception):
    pass

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


def read_users_from_csv(filename, config, column='id'):
    with open(filename, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, quotechar='"', delimiter=',')
        users = [line for line in reader]
    if config.resume_from:
        resumed_users = itertools.dropwhile(
            lambda x: x[column] != config.resume_from, users
        )
        next(resumed_users)
        users = list(resumed_users)
    chunk_size = config.search_count
    for x in range(0, len(users), chunk_size):
        users_chunk = users[x: x + chunk_size]
        yield len(users), users_chunk


def repack_exc(func):
    @functools.wraps(func)
    def inner(client, *args, **kwargs):
        try:
            result = func(client, *args, **kwargs)
            yield from result

        except ApiError as ex:
            if ex.code == ERROR_RATE_LIMIT_EXCEEDED:
                raise RateLimitException(str(ex))
            else:
                raise
    return inner


def login_retrier(func):
    @functools.wraps(func)
    def inner(client: VkClientProxy, *args, **kwargs):
        try:
            result = func(client, *args, **kwargs)
            yield from result

        except RateLimitException as ex:
            logger.error(f'Retrying after error: {ex}')
            for account, _ in client._accounts:
                try:
                    client.auth()
                    result = func(client, *args, **kwargs)
                    yield from result
                    break
                except RateLimitException as ex:
                    logger.error(f'Failed with account {account}. Retrying after error: {ex}')
            else:
                raise RateLimitException(str(ex))
    return inner
