import copy
import csv
from datetime import datetime
import os

import vk_api
from dotenv import load_dotenv

import config
import models

load_dotenv()


def search_users(client, _config):
    page_size = _config.search_count
    offset = 0
    params = {k: v for k, v in _config.search_criteria.items() if v}
    params['count'] = page_size
    users = client.users.search(**params)
    yield users['count'], users['items']
    while len(users['items']) >= page_size-1: # WTF: requested N, returned N-1?!
        offset += page_size
        params['offset'] = offset
        users = client.users.search(**params)
        yield users['count'], users['items']

# def dump_mapping(users_info):
#     cities = {}
#     countries = {}
#     for user_info in users_info:
#         if 'city' in user_info:
#             cities[user_info['city']['id']] = user_info['city']['title']
#         if 'country' in user_info:
#             countries[user_info['country']['id']] = user_info['country']['title']


def from_unix_time(ts):
    return datetime.utcfromtimestamp(ts)


def get_1st_post_ts(client, user_info):
    result = None
    try:
        posts = client.wall.get(owner_id=user_info['id'])
        if posts['count'] > 0:
            # last_post = client.wall.get(owner_id=user_info['id'], offset=posts.count-1)
            last_post = client.wall.get(owner_id=user_info['id'], offset=posts['count'] - 1)['items'][0]
            result = str(from_unix_time(last_post['date']))
    except Exception as ex:
        print(f'ERROR: id={user_info["id"]}, {ex}')
    return result


def unwind_value(d, prefix=''):
    result = {}
    for k, v in d.items():
        if type(v) == dict:
            result.update(unwind_value(v, prefix=f'{k}_'))
        elif type(v) == list:
            for idx, elem in enumerate(v):
                result.update(unwind_value(elem, prefix=f'{k}_{idx}_'))
        else:
            result[prefix+k] = v
    return result


def normalize_row(row, config):
    vals = []
    for field in config.csv_fields:
        if field in row:
            if type(row[field]) == str:
                row[field] = row[field].replace("\n", ' ')
            vals.append(row[field])
        else:
            vals.append('')
    return vals


def main():
    vk_session = vk_api.VkApi(os.getenv('USER_PHONE_NUMBER'), os.getenv('USER_PASSWORD'))
    vk_session.auth()

    vk_client = vk_session.get_api()
    _config = models.Config(**config.data)
    # fields = set()

    with open('result.csv', 'w+') as f:
        # writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer = csv.writer(f)
        writer.writerow(_config.csv_fields)
        idx = 1
        for count, users in search_users(vk_client, _config):
            user_ids = [u['id'] for u in users]
            user_infos = vk_client.users.get(user_ids=user_ids, fields=_config.fetch_fields)
            for user_info in user_infos:
                try:
                    row = unwind_value(user_info)
                    row['last_seen_time'] = str(from_unix_time(row['last_seen_time']))
                    row['first_post_created'] = get_1st_post_ts(vk_client, user_info)
                    writer.writerow(normalize_row(row, _config))
                    print(f'Processed user {idx}/{count}')
                    # fields.update(set(row.keys()))
                    idx += 1
                except Exception as ex:
                    print(f'ERROR: id={user_info["id"]}, {ex}')


if __name__ == '__main__':
    main()