import csv
import functools
import json
import os
import sys
from collections import defaultdict
from pprint import pprint as pp
from io import StringIO

import vk_api
from dotenv import load_dotenv

import config
import models
from models import VkResponse
from utils import from_unix_time, unwind_value, logger, read_users_from_csv, login_retrier

load_dotenv()

column_name = 'id'


class VkClientProxy:
    PROFILE_PHONE_NUMBER_PREFIX = 'USER_PHONE_NUMBER'
    PROFILE_PASSWORD_PREFIX = 'USER_PASSWORD'

    def __init__(self):
        self._obj = None
        self._config = None
        self._session = None
        self._accounts = []

    def __getattr__(self, item):
        return getattr(self._obj, item)

    def set_proxy_obj(self, instance):
        if isinstance(instance, dict):
            for k, v in instance.items():
                setattr(self, k, v)
        else:
            self._obj = instance

    def load_accounts(self):
        accounts = []
        for i in range(1, 10):
            env_phone_number_var_name = f'{self.PROFILE_PHONE_NUMBER_PREFIX}_{i}'
            env_password_var_name = f'{self.PROFILE_PASSWORD_PREFIX}_{i}'
            if os.getenv(env_phone_number_var_name):
                accounts.append((
                    os.getenv(env_phone_number_var_name),
                    os.getenv(env_password_var_name)
                ))
            else:
                break

        self._accounts = accounts

    def next_account(self):
        result = None, None
        if self._accounts:
            result = self._accounts.pop(0)
            self._accounts.append(result)

        return result

    def auth(self):
        self._session = vk_api.VkApi(*self.next_account())
        self._session.auth()
        self.set_proxy_obj(self._session.get_api())
        self._config = models.Config(**config.data)


# ----------------------------------------------------------------------------------------------------------------------
def search_entities(search_func, search_params, return_count=False):
    data_available = True
    offset = 0
    while data_available:
        response = VkResponse(**search_func(**search_params))
        total = response.count
        result = (response.count, response.items) if return_count else response.items
        yield result
        offset += len(response.items)
        search_params['offset'] = offset
        data_available = (offset < total) and len(response.items) > 0


@login_retrier
def get_post_range_ts(client, user_info):
    result_recent, result_latest = None, None
    try:
        response = VkResponse(**client.wall.get(owner_id=user_info['id']))
        if response.count > 0:
            recent_post = response.items[0]
            response = VkResponse(**client.wall.get(
                owner_id=user_info['id'],
                offset=response.count - 1
            ))
            latest_post = response.items[0]
            result_recent = str(from_unix_time(recent_post['date']))
            result_latest = str(from_unix_time(latest_post['date']))
    except Exception as ex:
        logger.error(f'Couldnt fetch user\'s posts: id={user_info["id"]}, {ex}')
        raise
    return result_recent, result_latest


def normalize_row(row, config):
    vals = []
    for field in (config.csv_fields + config.custom_csv_fields):
        if field in row:
            if type(row[field]) == str:
                row[field] = row[field].replace("\n", ' ').replace("\r", ' ')
            elif type(row[field]) == bool:
                row[field] = int(row[field])
            if field == 'bdate':
                bdate_elems = row[field].split('.')
                if len(bdate_elems) == 2:
                    bdate_elems.append('1900')
                row[field] = '.'.join(['{:02}'.format(int(elem)) for elem in bdate_elems])
            vals.append(row[field])
        else:
            vals.append('')
    return vals


@login_retrier
def vk_get_users(vk_client, user_ids):
    return vk_client.users.get(
        user_ids=user_ids,
        fields=', '.join(vk_client._config.fetch_fields)
    )


def fetch_from_source(vk_client, users_sourse):
    # fields = set()
    with open('result.csv', 'w+') as f:
        # writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer = csv.writer(f)
        writer.writerow(vk_client._config.csv_fields + vk_client._config.custom_csv_fields)
        idx = 1
        for count, users in users_sourse():
            user_ids = [u[column_name] for u in users]
            # user_infos = vk_client.users.get(
            #     user_ids=user_ids,
            #     fields=', '.join(_config.fetch_fields
            # ))
            user_infos = vk_get_users(vk_client, user_ids)
            for user_info in user_infos:
                try:
                    row = unwind_value(user_info)
                    row['last_seen_time'] = str(from_unix_time(row['last_seen_time']))
                    row['recent_post_created'], row['earliest_post_created'] = \
                        get_post_range_ts(vk_client, user_info)
                    writer.writerow(normalize_row(row, vk_client._config))
                    logger.info(f'Processed user {idx}/{count}')
                    # fields.update(set(row.keys()))
                    idx += 1
                except Exception as ex:
                    logger.error(f'Error while fetching user'
                                 f' id={user_info.get("id")},'
                                 f' first_name={user_info.get("first_name")},'
                                 f' deactivated={user_info.get("deactivated")}, {ex}')

    # print(list(fields))
    logger.info('\nSUCCESSFULLY FINISHED!')


def dump_mappings(vk_client: VkClientProxy):
    dumped_cities = {}
    dumped_unis = {}

    city_params = {'country_id': 1, 'need_all': 0, 'count': vk_client._config.search_count}
    for cities in search_entities(vk_client.database.getCities, city_params):
        for city in cities:
            uni_params = {'country_id': 1, 'city_id': city['id'], 'count': vk_client._config.search_count}
            for universities in search_entities(vk_client.database.getUniversities, uni_params):
                for uni in universities:
                    dumped_cities[str(city['id'])] = city['title']
                    dumped_unis[str(uni['id'])] = uni['title']

    with open('./mappings.json', 'r+') as f:
        mappings = defaultdict(dict)
        if content := f.read():
            mappings.update(json.loads(content))

    with open('./mappings.json', 'w+') as f:
        mappings['city'].update(dumped_cities)
        mappings['university'].update(dumped_unis)

        new_mappings = {}
        for map_name, map_dict in mappings.items():
            new_mappings[map_name] = {k:map_dict[k] for k in sorted(map_dict, key=map_dict.get)}

        stream = StringIO()
        pp(new_mappings, width=300, stream=stream, sort_dicts=False)
        stream.seek(0)
        f.write(stream.read().replace("'", '"'))



def main():
    global column_name

    vk_client = VkClientProxy()
    vk_client.load_accounts()
    vk_client.auth()

    if len(sys.argv) > 1:
        param = sys.argv[1]
        if param == 'dump':
            dump_mappings(vk_client)
            return
        else:
            if len(sys.argv) > 2:
                column_name = sys.argv[2]
            users_sourse = functools.partial(read_users_from_csv, param, vk_client._config.search_count)
    else:
        params = {k: v for k, v in vk_client._config.search_criteria.items() if v}
        params.update({'count': vk_client._config.search_count})
        users_sourse = functools.partial(
            search_entities,
            vk_client.users.search, params, return_count=True
        )

    try:
        fetch_from_source(vk_client, users_sourse)
    except Exception as ex:
        logger.error(f'{ex}')


if __name__ == '__main__':
    main()
