import os
import requests

import vk_api
from pydantic import BaseModel
from typing import List, Optional, Any, Union

import config


class Mapping(BaseModel):
    city: dict
    country: dict


class Config(BaseModel):
    search_criteria: dict
    search_count: Optional[int] = 100
    parse_posts: Optional[bool] = False
    fetch_fields: List[str]
    csv_fields: List[str]
    resume_from: Optional[str] = ''
    custom_csv_fields: Optional[List[str]] = []

    def get_fetch_fields(self):
        return ', '.join(self.fetch_fields)


class VkResponse(BaseModel):
    count: int
    items: List[Union[int, dict]]


class VkClientProxy:
    PROFILE_PHONE_NUMBER_PREFIX = 'USER_PHONE_NUMBER'
    PROFILE_PASSWORD_PREFIX = 'USER_PASSWORD'

    def __init__(self):
        self._obj = None
        self._session = None
        self._accounts = []
        self.config: Config = None

    def __getattr__(self, item):
        return getattr(self._obj, item)

    def set_proxy_obj(self, instance):
        if isinstance(instance, dict):
            for k, v in instance.items():
                setattr(self, k, v)
        else:
            self._obj = instance

    def load_accounts(self):
        from utils import logger

        accounts = []
        for k, v in os.environ.items():
            if k.startswith(self.PROFILE_PHONE_NUMBER_PREFIX):
                suffix = k.replace(f'{self.PROFILE_PHONE_NUMBER_PREFIX}', '')
                if os.environ.get(f'{self.PROFILE_PASSWORD_PREFIX}{suffix}'):
                    accounts.append((
                        os.getenv(k),
                        os.getenv(f'{self.PROFILE_PASSWORD_PREFIX}{suffix}')
                    ))
                else:
                    logger.error(f'Profile PHONE NUMBER env var doesnt match with PASSWORD (different suffix)')

        self._accounts = accounts

    def next_account(self):
        result = None, None
        if self._accounts:
            result = self._accounts.pop(0)
            self._accounts.append(result)

        return result

    def auth(self):
        try:
            self._session = vk_api.VkApi(*self.next_account())
            self._session.auth()
            self.set_proxy_obj(self._session.get_api())
            self.config = Config(**config.data)
        except Exception as ex:
            self.direct_auth(app_id=os.getenv('VK_APP_ID'), client_secret=os.getenv('VK_APP_SECRET'))

    def direct_auth(self, **kw_args):
        username, password = self.next_account()
        app_id, client_secret = kw_args.get('app_id'), kw_args.get('client_secret')
        self._session = vk_api.VkApi(*self.next_account(), **kw_args)
        AUTH_URL = f'https://oauth.vk.com/token?grant_type=password&client_id={app_id}&client_secret={client_secret}&'
        resp = requests.get(AUTH_URL + f'username={username}&password={password}')
        if resp.status_code != 200:
            raise RuntimeError(f'Not Authorized {resp.status_code}, {resp.text}')
        self._session.token = resp.json()
        # self._session.auth(reauth=True, token_only=True)
        self.set_proxy_obj(self._session.get_api())
        self.config = Config(**config.data)

    def get_params(self, extra_params=None):
        params = {'count': self.config.search_count}
        if extra_params:
            params.update(extra_params)
        return params