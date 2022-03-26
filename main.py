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
from utils import from_unix_time, unwind_value, logger, read_users_from_csv

load_dotenv()

column_name = 'id'

# ----------------------------------------------------------------------------------------------------------------------

def search_users(client, _config):
    page_size = _config.search_count
    offset = 0
    params = {k: v for k, v in _config.search_criteria.items() if v}
    params['count'] = page_size
    response = VkResponse(**client.users.search(**params))
    yield response.count, response.items
    processed = len(response.items)
    total = response.count
    while (processed < total) and len(response.items) > 0:
        offset += len(response.items)
        params['offset'] = offset
        response = VkResponse(**client.users.search(**params))
        yield response.count, response.items
        processed += len(response.items)


def dump_cities(client, _config):
    page_size = _config.search_count
    offset = 0
    # params = {k: v for k, v in _config.search_criteria.items() if v}
    params = {'country_id': 1, 'need_all': 0}
    params['count'] = page_size
    response = VkResponse(**client.database.getCities(**params))
    yield response.count, response.items
    processed = len(response.items)
    total = response.count
    while (processed < total) and len(response.items) > 0:
        offset += len(response.items)
        params['offset'] = offset
        response = VkResponse(**client.database.getCities(**params))
        yield response.count, response.items
        processed += len(response.items)


def dump_universities(client, _config, city_id):
    page_size = _config.search_count
    offset = 0
    # params = {k: v for k, v in _config.search_criteria.items() if v}
    params = {'country_id': 1, 'city_id': city_id}
    params['count'] = page_size
    response = VkResponse(**client.database.getUniversities(**params))
    yield response.items
    processed = len(response.items)
    total = response.count
    while (processed < total) and len(response.items) > 0:
        offset += len(response.items)
        params['offset'] = offset
        response = VkResponse(**client.database.getUniversities(**params))
        yield response.items
        processed += len(response.items)


def get_post_range_ts(client, user_info):
    result_recent, result_latest = None, None
    try:
        # posts = client.wall.get(owner_id=user_info['id'])
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
        logger.error(f'id={user_info["id"]}, {ex}')
    return result_recent, result_latest


def normalize_row(row, config):
    vals = []
    for field in (config.csv_fields + config.custom_csv_fields):
        if field in row:
            if type(row[field]) == str:
                row[field] = row[field].replace("\n", ' ')
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


def fetch_from_source(vk_client, _config, users_sourse):
    # fields = set()
    with open('result.csv', 'w+') as f:
        # writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer = csv.writer(f)
        writer.writerow(_config.csv_fields + _config.custom_csv_fields)
        idx = 1
        for count, users in users_sourse():
            user_ids = [u[column_name] for u in users]
            user_infos = vk_client.users.get(
                user_ids=user_ids,
                fields=', '.join(_config.fetch_fields
            ))
            for user_info in user_infos:
                try:
                    row = unwind_value(user_info)
                    row['last_seen_time'] = str(from_unix_time(row['last_seen_time']))
                    row['recent_post_created'], row['earliest_post_created'] = \
                        get_post_range_ts(vk_client, user_info)
                    writer.writerow(normalize_row(row, _config))
                    logger.info(f'Processed user {idx}/{count}')
                    # fields.update(set(row.keys()))
                    idx += 1
                except Exception as ex:
                    logger.error(f'id={user_info["id"]}, {ex}')

    # print(list(fields))
    logger.info('\nSUCCESSFULLY FINISHED!')


def dump_mappings(vk_client, config):
    dumped_cities = {}
    dumped_unis = {}
    for num, cities in dump_cities(vk_client, config):
        for city in cities:
            for universities in dump_universities(vk_client, config, city['id']):
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

    vk_session = vk_api.VkApi(os.getenv('USER_PHONE_NUMBER'), os.getenv('USER_PASSWORD'))
    vk_session.auth()
    vk_client = vk_session.get_api()
    _config = models.Config(**config.data)

    if len(sys.argv) > 1:
        param = sys.argv[1]
        if param == 'dump':
            dump_mappings(vk_client, _config)
        else:
            if len(sys.argv) > 2:
                column_name = sys.argv[2]
            users_sourse = functools.partial(read_users_from_csv, param)
    else:
        users_sourse = functools.partial(search_users, vk_client, _config)

    fetch_from_source(vk_client, _config, users_sourse)


if __name__ == '__main__':
    main()
