import os

import vk_api
from dotenv import load_dotenv

import config
import models

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

        result = '79292771833:Gfdtk1997'.split(':')
        return result

    def auth(self):
        self._session = vk_api.VkApi(*self.next_account())
        self._session.auth()
        self.set_proxy_obj(self._session.get_api())
        self._config = models.Config(**config.data)




def main():
    global column_name

    vk_client = VkClientProxy()
    vk_client.load_accounts()
    vk_client.auth()

if __name__ == '__main__':
    main()
