from penquins import Kowalski
import time


class TestKowalski(object):

    # remember to escape special chars in password!
    # python -m pytest test_penquins.py --username=admin --password=
    # python -m pytest -s test_penquins.py --username=admin --password=
    # python -m pytest -q test_penquins.py --username=admin --password=

    def test_authenticate(self, username, password):
        # if authentication fails, exception will be raised
        Kowalski(username=username, password=password)

    def test_query(self, username, password, benchmark=False):
        with Kowalski(username=username, password=password) as k:

            assert k.check_connection()

            # base query:
            qu = {"query_type": "general_search",
                  "query": "db['ZTF_alerts'].find_one({}, {'_id': 1})",
                  "kwargs": {}}
            timeout = 1  # seconds

            # query: enqueue_only, save=True, save=False
            qu['kwargs']['enqueue_only'] = True
            resp = k.query(query=qu, timeout=timeout)
            # print(resp)
            assert 'query_id' in resp
            assert resp['status'] == 'enqueued'

            # fetch enqueued/saved query
            time.sleep(timeout)
            qid = resp['query_id']
            result = k.get_query(query_id=qid, part='result')
            # print(result)
            assert 'result' in result
            assert 'query_result' in result['result']

            # delete saved query
            result = k.delete_query(query_id=qid)
            assert 'message' in result
            assert result['message'] == 'success'

            # little benchmark:
            if benchmark:
                qu = {"query_type": "general_search",
                      "query": "db['ZTF_alerts'].aggregate([{'$match': {'candidate.rb': {'$gt': 0.98}}}, {'$project': {'_id': 1}}, {'$sample': {'size': 1}}])",
                      "kwargs": {}}
                timeout = 5  # seconds
                qu['kwargs'] = dict()
                qu['kwargs']['save'] = False
                times = []
                for i in range(5):
                    tic = time.time()
                    result = k.query(query=qu, timeout=timeout)
                    toc = time.time()
                    assert 'result_data' in result
                    assert 'query_result' in result['result_data']
                    times.append(toc-tic)

                print(times)
