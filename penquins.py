import string
import random
import traceback
import os
import time
from copy import deepcopy
from typing import Union
import requests
from bson.json_util import loads


''' PENQUINS - Processing ENormous Queries of ztf Users INStantaneously '''
__version__ = '1.0.2'


Num = Union[int, float]
QueryPart = Union['task', 'result']
Method = Union['get', 'post', 'put', 'patch', 'delete']


class Kowalski(object):
    """
        Query ZTF TDA databases
    """

    # def __init__(self, protocol='http', host='127.0.0.1', port=8000, verbose=False,
    #              username=None, password=None):

    def __init__(self, protocol='https', host='kowalski.caltech.edu', port=443, verbose=False,
                 username=None, password=None):

        assert username is not None, 'username must be specified'
        assert password is not None, 'password must be specified'

        # Status, Kowalski!
        self.v = verbose

        self.protocol = protocol

        self.host = host
        self.port = port

        self.base_url = f'{self.protocol}://{self.host}:{self.port}'

        self.username = username
        self.password = password

        self.session = requests.Session()

        self.access_token = self.authenticate()

        self.headers = {'Authorization': self.access_token}

        self.methods = {'get': self.session.get,
                        'post': self.session.post,
                        'put': self.session.put,
                        'patch': self.session.patch,
                        'delete': self.session.delete}

    # use with "with":
    def __enter__(self):
        # print('Starting')
        return self

    def __exit__(self, *exc):
        # print('Finishing')
        # run shut down procedure
        self.close()
        return False

    def close(self):
        """
            Shutdown session gracefully
        :return:
        """
        try:
            self.session.close()
            return True
        except Exception as e:
            if self.v:
                print(e)
            return False

    def authenticate(self, retries: int = 3):
        """
            Authenticate user, return access token
        :return:
        """

        for retry in range(retries):
            # post username and password, get access token
            auth = self.session.post(f'{self.base_url}/auth',
                                     json={"username": self.username, "password": self.password,
                                           "penquins.__version__": __version__})

            if auth.status_code == requests.codes.ok:
                if self.v:
                    print(auth.json())

                # mimic a web login, too
                # auth_web = self.session.post(f'{self.base_url}/login',
                #                              json={"username": self.username, "password": self.password,
                #                                    "zvm.__version__": __version__})

                if 'token' not in auth.json():
                    print('Authentication failed')
                    raise Exception(auth.json()['message'])

                access_token = auth.json()['token']

                if self.v:
                    print('Successfully authenticated')

                return access_token

            else:
                # bad status code? sleep before retrying, maybe no connections available due to high load
                time.sleep(0.5)

    def api(self, data: dict, endpoint: str = None, method: Method = None, timeout: Num = 30, retries: int = 3):

        try:
            assert endpoint is not None, 'endpoint not specified'
            # assert data is not None, 'api call not specified'
            assert method in ['get', 'post', 'put', 'patch', 'delete'], f'unsupported method: {method}'

            cookies = {'jwt_token': self.access_token, 'user_id': self.username}

            for retry in range(retries):
                if method.lower() != 'get':
                    resp = self.methods[method.lower()](os.path.join(self.base_url, endpoint),
                                                        json=data, headers=self.headers, timeout=timeout,
                                                        cookies=cookies)
                else:
                    resp = self.methods[method.lower()](os.path.join(self.base_url, endpoint),
                                                        params=data, headers=self.headers, timeout=timeout,
                                                        cookies=cookies)

                # print(resp.text)

                if resp.status_code == requests.codes.ok:
                    return loads(resp.text)
                else:
                    # bad status code? sleep before retrying, maybe no connections available due to high load
                    time.sleep(0.5)

        except Exception as _e:
            _err = traceback.format_exc()

            return {'status': 'failed', 'message': _err}

    def query(self, query, timeout: Num = 5*3600, retries: int = 3):

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

            for retry in range(retries):
                resp = self.session.put(os.path.join(self.base_url, 'query'),
                                        json=_query, headers=self.headers, timeout=timeout)

                # print(resp)

                if resp.status_code == requests.codes.ok:
                    return loads(resp.text)
                else:
                    # bad status code? sleep before retrying, maybe no connections available due to high load
                    time.sleep(0.5)

        except Exception as _e:
            _err = traceback.format_exc()

            return {'status': 'failed', 'message': _err}

    def get_query(self, query_id: str, part: QueryPart = 'result', retries: int = 3):
        """
            Fetch json for task or result by query id
        :param query_id:
        :param part:
        :param retries:
        :return:
        """
        try:
            for retry in range(retries):
                result = self.session.post(os.path.join(self.base_url, 'query'),
                                           json={'task_id': query_id, 'part': part}, headers=self.headers)

                if result.status_code == requests.codes.ok:
                    _result = {'task_id': query_id, 'result': loads(result.text)}

                    return _result

                else:
                    # bad status code? sleep before retrying, maybe no connections available due to high load
                    time.sleep(0.5)

        except Exception as _e:
            _err = traceback.format_exc()

            return {'status': 'failed', 'message': _err}

    def delete_query(self, query_id: str, retries: int = 3):
        """
            Delete query by query_id
        :param query_id:
        :param retries:
        :return:
        """
        try:
            for retry in range(retries):
                result = self.session.delete(os.path.join(self.base_url, 'query'),
                                             json={'task_id': query_id}, headers=self.headers)

                if result.status_code == requests.codes.ok:
                    _result = loads(result.text)

                    return _result

                else:
                    # bad status code? sleep before retrying, maybe no connections available due to high load
                    time.sleep(0.5)

        except Exception as _e:
            _err = traceback.format_exc()

            return {'status': 'failed', 'message': _err}

    def check_connection(self, collection='RFC_2019a') -> bool:
        """
            Check connection to Kowalski with a trivial query
        :return: True if connection ok, False otherwise
        """
        try:
            _query = {"query_type": "find",
                      "query": {
                          "catalog": collection,
                          "filter": {},
                          "projection": {'_id': 1}
                      },
                      "kwargs": {"limit": 1, "save": False}
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
