import string
import random
import traceback
import aiohttp
import asyncio
import os
from bson.json_util import loads
import numpy as np


''' PENQUINS - Processing ENormous Queries of ztf Users INStantaneously '''
__version__ = '1.0.0'


def radec_str2rad(_ra_str, _dec_str):
    """

    :param _ra_str: 'H:M:S'
    :param _dec_str: 'D:M:S'
    :return: ra, dec in rad
    """
    # convert to rad:
    _ra = list(map(float, _ra_str.split(':')))
    _ra = (_ra[0] + _ra[1] / 60.0 + _ra[2] / 3600.0) * np.pi / 12.
    _dec = list(map(float, _dec_str.split(':')))
    _sign = np.sign(_dec[0]) if _dec[0] != 0 else 1
    _dec = _sign * (abs(_dec[0]) + abs(_dec[1]) / 60.0 + abs(_dec[2]) / 3600.0) * np.pi / 180.

    return _ra, _dec


def radec_str2geojson(ra_str, dec_str):

    # hms -> ::, dms -> ::
    if isinstance(ra_str, str) and isinstance(dec_str, str):
        if ('h' in ra_str) and ('m' in ra_str) and ('s' in ra_str):
            ra_str = ra_str[:-1]  # strip 's' at the end
            for char in ('h', 'm'):
                ra_str = ra_str.replace(char, ':')
        if ('d' in dec_str) and ('m' in dec_str) and ('s' in dec_str):
            dec_str = dec_str[:-1]  # strip 's' at the end
            for char in ('d', 'm'):
                dec_str = dec_str.replace(char, ':')

        if (':' in ra_str) and (':' in dec_str):
            ra, dec = radec_str2rad(ra_str, dec_str)
            # convert to geojson-friendly degrees:
            ra = ra * 180.0 / np.pi - 180.0
            dec = dec * 180.0 / np.pi
        else:
            raise Exception('Unrecognized string ra/dec format.')
    else:
        # already in degrees?
        ra = float(ra_str)
        # geojson-friendly ra:
        ra -= 180.0
        dec = float(dec_str)

    return ra, dec


class Kowalski(object):
    """
        Query ZTF TDA databases
    """

    def __init__(self, protocol='http', host='localhost', port=8000, verbose=False,
                 username=None, password=None):

    # def __init__(self, protocol='https', host='kowalski.caltech.edu', port=443, verbose=False,
    #              username=None, password=None):

        assert username is not None, 'username must be specified'
        assert password is not None, 'password must be specified'

        # Kowalski, status!
        self.v = verbose

        self.protocol = protocol

        self.host = host
        self.port = port

        self.base_url = f'{self.protocol}://{self.host}:{self.port}'

        self.username = username
        self.password = password

    # use with "async with":
    async def __aenter__(self):
        # print('Starting')
        self.access_token = await self.authenticate()

        headers = {'Authorization': self.access_token}
        self.session = aiohttp.ClientSession(headers=headers)

        return self

    async def __aexit__(self, *exc):
        # print('Finishing')
        # run shut down procedure
        await self.session.close()
        return False

    async def authenticate(self):
        """
            Authenticate user, return access token
        :return:
        """
        access_token = None

        # try:
        # post username and password, get access token
        async with aiohttp.ClientSession() as session:
            async with session.post(os.path.join(self.base_url, 'auth'),
                                    data={"username": self.username, "password": self.password,
                                          "penquins.__version__": __version__}
                                    ) as resp:
                access_token = await resp.read()

        print(access_token)

        # if self.v:
        #     print(auth.json())
        #
        # if 'access_token' not in auth.json():
        #     print('Authentication failed')
        #     raise Exception(auth.json()['msg'])
        #
        # access_token = auth.json()['access_token']
        #
        # if self.v:
        #     print('Successfully authenticated')

        return access_token


class MyAsyncContextManager:
    async def __aenter__(self):
        # await log('entering context')
        # maybe some setup (e.g. await self.setup())
        return self

    async def __aexit__(self, exc_type, exc, tb):
        # maybe closing context (e.g. await self.close())
        # await log('exiting context')
        pass

    async def do_something(self):
        # await log('doing something')
        pass


async def main():
    async with Kowalski(username='admin', password='admin') as k:
        print('lala')

if __name__ == '__main__':

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
