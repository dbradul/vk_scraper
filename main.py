import os

import csv
import functools
import json
import sys
from collections import defaultdict
from datetime import datetime
from io import StringIO
from pprint import pprint as pp

from dotenv import load_dotenv

from models import VkResponse, VkClientProxy
from utils import from_unix_time, unwind_value, logger, read_from_csv, login_retrier, repack_exc, \
    RateLimitException, ProfileIsPrivateException

load_dotenv()

ID_COLUMN_NAME = 'id'
COLUMN_NAME_PARENT_ID = 'ParentId'
COLUMN_NAME_NAME = 'Імя'
COLUMN_NAME_SURNAME = 'Прізвище'
COLUMN_NAME_BDAY = 'Дата'
COLUMN_NAME_GROUP_URL = 'GroupUrl'
COLUMN_NAME_GROUP_NAME = 'GroupName'
RESULT_FILEPATH = 'result.csv'

@login_retrier
@repack_exc
def paginate_func(client, func, params, return_count=False):
    data_available = True
    offset = 0
    while data_available:
        response = VkResponse(**func(**params))
        result = (response.count, response.items) if return_count else response.items
        yield result
        offset += response.count
        params['offset'] = offset
        data_available = len(response.items) > 0


@login_retrier
@repack_exc
def get_post_range_ts(client, user_info):
    result_recent, result_latest = 'NA', 'NA'
    if client.config.parse_posts:
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
            # raise
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
        fields=vk_client.config.get_fetch_fields()
    )


def fetch_from_source(vk_client, users_source):
    # fields = set()
    with open(RESULT_FILEPATH, 'w+') as f:
        # writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer = csv.writer(f)
        writer.writerow(vk_client.config.csv_fields + vk_client.config.custom_csv_fields)
        idx = 1
        for count, users in users_source():
            user_ids = [u[ID_COLUMN_NAME] for u in users]
            user_infos = vk_get_users(vk_client, user_ids)
            for user_info in user_infos:
                dump_user_info(vk_client, writer, user_info)

    # print(list(fields))
    logger.info('\nSUCCESSFULLY FINISHED!')


def dump_user_info(client, writer, user_info, extra_values=None):
    extra_values = extra_values or []
    try:
        row = unwind_value(user_info)
        row['last_seen_time'] = str(from_unix_time(row['last_seen_time']))
        row['recent_post_created'], row['earliest_post_created'] = \
            get_post_range_ts(client, user_info)
        writer.writerow(extra_values + normalize_row(row, client.config))
        logger.info(f'Processed user {row.get("first_name")} {row.get("last_name")},  id={row.get("id")}')
    except RateLimitException as ex:
        raise
    except Exception as ex:
        logger.error(f'Error while fetching user'
                     f' id={user_info.get("id")},'
                     f' first_name={user_info.get("first_name")},'
                     f' deactivated={user_info.get("deactivated")}, {ex}')


def dump_mappings(vk_client: VkClientProxy):
    dumped_cities = {}
    dumped_unis = {}

    city_params = {'country_id': 1, 'need_all': 0, 'count': vk_client.config.search_count}
    for cities in paginate_func(vk_client, vk_client.database.getCities, city_params):
        for city in cities:
            uni_params = {'country_id': 1, 'city_id': city['id'], 'count': vk_client.config.search_count}
            for universities in paginate_func(vk_client, vk_client.database.getUniversities, uni_params):
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


def search_by_name(client, filename):
    EXTRA_FIELDS = [
        COLUMN_NAME_NAME,
        COLUMN_NAME_SURNAME,
        COLUMN_NAME_BDAY
    ]
    with open(RESULT_FILEPATH, 'w+', newline='') as f:
        # writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer = csv.writer(f)
        writer.writerow(EXTRA_FIELDS + client.config.csv_fields + client.config.custom_csv_fields)

        for count, users in read_from_csv(filename, client.config):
            for user in users:
                birth_date = datetime.strptime(user.get(COLUMN_NAME_BDAY, ''), '%d.%m.%Y')
                params= client.get_params({
                    'q': f'{user.get(COLUMN_NAME_SURNAME, "")} {user.get(COLUMN_NAME_NAME, "")}',
                    'birth_day': birth_date.day,
                    'birth_month': birth_date.month,
                    'birth_year': birth_date.year
                })

                for users in paginate_func(client, client.users.search, params):
                    user_ids = [u[ID_COLUMN_NAME] for u in users]
                    user_infos = vk_get_users(client, user_ids)
                    for user_info in user_infos:
                        dump_user_info(
                            client,
                            writer,
                            user_info,
                            extra_values=[user.get(COLUMN_NAME_NAME, ''), user.get(COLUMN_NAME_SURNAME, ''), user.get(COLUMN_NAME_BDAY, '')]
                        )


def find_friends(client: VkClientProxy, filename):
    EXTRA_FIELDS = [
        COLUMN_NAME_PARENT_ID
    ]
    with open(RESULT_FILEPATH, 'w+', newline='') as f:
        # writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer = csv.writer(f)
        writer.writerow(EXTRA_FIELDS + client.config.csv_fields + client.config.custom_csv_fields)

        for count, users in read_from_csv(filename, client.config):
            for user in users:
                params = client.get_params({'user_id': user[ID_COLUMN_NAME]})
                logger.info(f'Started fetching friends for user: {user[ID_COLUMN_NAME]}')
                logger.info('-----------------------------------------------------------')
                try:
                    for friends in paginate_func(client, client.friends.get, params):
                        user_infos = vk_get_users(client, user_ids=friends)
                        for user_info in user_infos:
                            dump_user_info(client, writer, user_info, extra_values=[user[ID_COLUMN_NAME]])
                except Exception as ex:
                    logger.error(f"Couldn't fetch friends for profile: {user[ID_COLUMN_NAME]}")


def parse_groups(client: VkClientProxy, filename):
    EXTRA_FIELDS = [
        COLUMN_NAME_GROUP_URL,
        COLUMN_NAME_GROUP_NAME
    ]
    with open(RESULT_FILEPATH, 'w+', newline='') as f:
        # writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer = csv.writer(f)
        writer.writerow(EXTRA_FIELDS + client.config.csv_fields + client.config.custom_csv_fields)

        for count, lines in read_from_csv(filename, client.config):
            for line in lines:
                group_id = line[ID_COLUMN_NAME].split('/')[-1]
                params = client.get_params({'group_id': group_id})
                group_info = client.groups.getById(**params)[0].get('name')
                logger.info(f'Started fetching members for group: {line[ID_COLUMN_NAME]}')
                logger.info('-----------------------------------------------------------')
                try:
                    for members in paginate_func(client, client.groups.getMembers, params):
                        user_infos = vk_get_users(client, user_ids=members)
                        for user_info in user_infos:
                            dump_user_info(client, writer, user_info, extra_values=[line[ID_COLUMN_NAME], group_info])
                except Exception as ex:
                    logger.error(f"Couldn't fetch members for group '{line[ID_COLUMN_NAME]}': {ex}")


def main():
    global ID_COLUMN_NAME

    vk_client = VkClientProxy()
    vk_client.load_accounts()
    vk_client.auth()

    if len(sys.argv) > 1:
        param = sys.argv[1]
        if param == 'dump':
            dump_mappings(vk_client)
            return
        elif param == '--search_by_name':
            if len(sys.argv) > 2:
                filepath = sys.argv[2]
                search_by_name(vk_client, filepath)
                return
            else:
                logger.error('Filepath is missing for "search_by_name" mode')
        elif param == '--find_friends':
            if len(sys.argv) > 2:
                filepath = sys.argv[2]
                find_friends(vk_client, filepath)
                return
            else:
                logger.error('Filepath is missing for "find_friends" mode')
        elif param == '--parse_groups':
            if len(sys.argv) > 2:
                filepath = sys.argv[2]
                ID_COLUMN_NAME = 'GroupURL'
                parse_groups(vk_client, filepath)
                return
            else:
                logger.error('Filepath is missing for "parse_groups" mode')
        else:
            if len(sys.argv) > 2:
                ID_COLUMN_NAME = sys.argv[2]
            users_source = functools.partial(read_from_csv, param, vk_client.config, ID_COLUMN_NAME)
    else:
        params = {k: v for k, v in vk_client.config.search_criteria.items() if v}
        params.update({'count': vk_client.config.search_count})
        users_source = functools.partial(
            # search_entities,
            paginate_func,
            vk_client, vk_client.users.search, params, return_count=True
        )

    try:
        fetch_from_source(vk_client, users_source)
    except Exception as ex:
        logger.error(f'{ex}')


if __name__ == '__main__':
    main()
