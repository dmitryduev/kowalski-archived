import string
import random
import traceback
import time
import os
from copy import deepcopy
from typing import Union
import requests
from bson.json_util import loads


''' PENQUINS - Processing ENormous Queries of ztf Users INStantaneously '''
__version__ = '1.0.0'


Num = Union[int, float]
QueryPart = Union['task', 'result']


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

            resp = self.session.put(os.path.join(self.base_url, 'query'),
                                    json=_query, headers=self.headers, timeout=timeout)

            # print(resp)

            return loads(resp.text)

        except Exception as _e:
            _err = traceback.format_exc()

            return {'status': 'failed', 'message': _err}

    def get_query(self, query_id: str, part: QueryPart = 'result'):
        """
            Fetch json for task or result by query id
        :param query_id:
        :param part:
        :return:
        """
        try:
            result = self.session.post(os.path.join(self.base_url, 'query'),
                                       json={'task_id': query_id, 'part': part}, headers=self.headers)

            _result = {'task_id': query_id, 'result': loads(result.text)}

            return _result

        except Exception as _e:
            _err = traceback.format_exc()

            return {'status': 'failed', 'message': _err}

    def delete_query(self, query_id: str):
        """
            Delete query by query_id
        :param query_id:
        :return:
        """
        try:
            result = self.session.delete(os.path.join(self.base_url, 'query'),
                                         json={'task_id': query_id}, headers=self.headers)

            _result = loads(result.text)

            return _result

        except Exception as _e:
            _err = traceback.format_exc()

            return {'status': 'failed', 'message': _err}

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


class TestKowalski(object):

    def test_authenticate(self, username, password):
        pass

    def test_query(self, username, password, q=None):
        # query: enqueue_only, save=True, save=False
        # fetch enqueued/saved query
        # delete saved query
        pass


if __name__ == '__main__':

    pass

    # with Kowalski(protocol='http', host='127.0.0.1', port=8000,
    #               username='admin', password='admin', verbose=False) as k:
    #     qu = {"query_type": "general_search",
    #           "query": "db['ZTF_alerts'].find_one({}, {'_id': 1})",
    #           "kwargs": {"save": False}}
    #     qu2 = {"query_type": "general_search",
    #            "query": "db['ZTF_alerts'].find_one({}, {'_id': 1})",
    #            "kwargs": {"enqueue_only": True}}
    #
    #     for i in range(5):
    #         tic = time.time()
    #         result = k.query(query=qu, timeout=0.1)
    #         toc = time.time()
    #         print(toc-tic)
    #         print(result)
    #
    #     alive = k.check_connection()
    #     print(alive)
    #
    #     result = k.query(query=qu2, timeout=0.1)
    #     time.sleep(0.15)
    #     # print(result)
    #     qid = result['query_id']
    #     result = k.get_query(query_id=qid, part='result')
    #     print(result)
    #
    #     result = k.delete_query(query_id=qid)
    #     print(result)

    # with Kowalski(protocol='http', host='kowalski.caltech.edu', port=8000,
    #               username='admin', password='admin', verbose=False) as k:
    #     qu = {"query_type": "general_search",
    #           "query": "db['TNS'].find_one({}, {'_id': 1, 'ra': 1, 'dec': 1})",
    #           "kwargs": {"save": True}}
    #
    #     qu2 = {"query_type": "cone_search",
    #            "object_coordinates": {"radec": "[('07:05:53.44', '12:53:34.69')]", "cone_search_radius": "1",
    #                                   "cone_search_unit": "arcsec"},
    #            "catalogs": {"TNS": {"filter": "{}", "projection": "{'_id': 1, 'ra': 1, 'dec': 1}"}}}
    #
    #     for i in range(5):
    #         tic = time.time()
    #         result = k.query(query=qu2, timeout=0.1)
    #         toc = time.time()
    #         print(toc-tic)
    #         print(result)
