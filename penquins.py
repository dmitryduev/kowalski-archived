import string
import random
import traceback
import time
import requests
import os
from copy import deepcopy
import numpy as np
from typing import Union


''' PENQUINS - Processing ENormous Queries of ztf Users INStantaneously '''
__version__ = '1.0.0'


Num = Union[int, float]
QueryPart = Union['task', 'result']


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

    def __init__(self, protocol='http', host='127.0.0.1', port=8000, verbose=False,
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

        self.access_token = self.authenticate()

        self.headers = {'Authorization': self.access_token}
        self.session = requests.Session()

    # use with "with":
    def __enter__(self):
        # print('Starting')
        return self

    def __exit__(self, *exc):
        # print('Finishing')
        # run shut down procedure
        self.session.close()
        return False

    def authenticate(self):
        """
            Authenticate user, return access token
        :return:
        """

        # try:
        # post username and password, get access token
        auth = requests.post(f'{self.base_url}/auth',
                             json={"username": self.username, "password": self.password,
                                   "penquins.__version__": __version__})

        if self.v:
            print(auth.json())

        if 'token' not in auth.json():
            print('Authentication failed')
            raise Exception(auth.json()['message'])

        access_token = auth.json()['token']

        if self.v:
            print('Successfully authenticated')

        return access_token

    def query(self, query, timeout: Num=5*3600):

        try:
            _query = deepcopy(query)

            # by default, [unless enqueue_only is requested]
            # all queries are not registered in the db and the task/results are stored on disk as json files
            # giving a significant execution speed up. this behaviour can be overridden.
            if ('kwargs' in _query) and ('enqueue_only' in _query['kwargs']) and _query['kwargs']['enqueue_only']:
                save = True
            else:
                save = _query['kwargs']['save'] if (('kwargs' in _query) and ('save' in _query['kwargs'])) else False

            if save:
                if 'kwargs' not in _query:
                    _query['kwargs'] = dict()
                if '_id' not in _query['kwargs']:
                    # generate a unique hash id and store it in query if saving query in db on Kowalski is requested
                    _id = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits)
                                  for _ in range(32)).lower()

                    _query['kwargs']['_id'] = _id

            resp = self.session.put(os.path.join(f'{self.base_url}', 'query'),
                                    json=_query, headers=self.headers, timeout=timeout)

            # print(resp)

            return resp.json()

        except Exception as _e:
            _err = traceback.format_exc()

            return {'status': 'failed', 'message': _err}

    def get_query(self, query_id: str, part: QueryPart = 'result'):
        """
            Fetch json for task or result
        :param query_id:
        :param part:
        :return:
        """
        # todo
        raise NotImplementedError

    def delete_query(self, query_id: str):
        """
            Delete query by query_id
        :param query_id:
        :return:
        """
        # todo
        if query_id == 'all':
            pass

        raise NotImplementedError

    def check_connection(self, collection='ZTF_alerts'):
        """
            Check connection to Kowalski with a trivial query
        :return: True if connection ok, False otherwise
        """
        try:
            _query = {"query_type": "general_search",
                      "query": f"db['{collection}'].find_one({{}}, {{'_id': 1}})",
                      "kwargs": {"save": False}
                      }
            if self.v:
                print(_query)
            _result = self.query(query=_query, timeout=3)

            if self.v:
                print(_result)

            return True if (('status' in _result) and (_result['status'] == 'done')) else False

        except Exception as _e:
            _err = traceback.format_exc()
            print(_err)
            return False


if __name__ == '__main__':

    with Kowalski(username='admin', password='admin', verbose=False) as k:
        qu = {"query_type": "general_search",
              "query": "db['ZTF_alerts'].find_one({}, {'_id': 1})",
              "kwargs": {"save": False}}
        qu2 = {"query_type": "general_search",
               "query": "db['ZTF_alerts'].find_one({}, {'_id': 1})",
               "kwargs": {"enqueue_only": True}}

        for i in range(5):
            tic = time.time()
            result = k.query(query=qu, timeout=0.1)
            # result = k.query(query=qu2, timeout=0.1)
            toc = time.time()
            print(toc-tic)
            print(result)

        alive = k.check_connection()
        print(alive)
