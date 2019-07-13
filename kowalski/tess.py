import schedule
import time
import os
import json
import datetime
import pytz
from astropy.time import Time
import pymongo
import tqdm
from bson.json_util import dumps
import subprocess


''' load config and secrets '''
with open('/app/config.json') as cjson:
    config = json.load(cjson)

with open('/app/secrets.json') as sjson:
    secrets = json.load(sjson)

for k_ in secrets:
    config[k_].update(secrets.get(k_, {}))


def utc_now():
    return datetime.datetime.now(pytz.utc)


def time_stamps():
    """

    :return: local time, UTC time
    """
    return datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S'), \
           datetime.datetime.utcnow().strftime('%Y%m%d_%H:%M:%S')


def connect_to_db():
    """ Connect to the mongodb database

    :return:
    """
    try:
        # there's only one instance of DB, it's too big to be replicated
        _client = pymongo.MongoClient(host=config['database']['host'],
                                      port=config['database']['port'])
        # grab main database:
        _db = _client[config['database']['db']]
    except Exception as _e:
        raise ConnectionRefusedError
    try:
        # authenticate
        _db.authenticate(config['database']['user'], config['database']['pwd'])
    except Exception as _e:
        raise ConnectionRefusedError

    return _client, _db


def dump_tess():

    # connect to MongoDB:
    print(time_stamps(), 'Connecting to DB')
    client, db = connect_to_db()
    print(time_stamps(), 'Successfully connected')

    datestr = datetime.datetime.utcnow().strftime('%Y%m%d')
    # datestr = '20190711'

    path_date = os.path.join(config['path']['path_tess'], datestr)

    # mkdir if necessary
    if not os.path.exists(path_date):
        os.makedirs(path_date)

    jd = Time(datetime.datetime.strptime(datestr, '%Y%m%d')).jd

    collection_alerts = 'ZTF_alerts'

    # query = {'candidate.jd': {'$gt': jd, '$lt': jd + 1},
    #          'candidate.programid': 1}
    query = {'candidate.jd': {'$gt': jd, '$lt': jd + 1},
             'candidate.programpi': 'TESS',
             'candidate.programid': 1}

    # index name to use:
    # hint = 'candidate.jd_1_candidate.programid_1'
    hint = 'jd_1__programpi_1__programid_1'

    num_doc = db[collection_alerts].count_documents(query, hint=hint)
    print(f'Alerts in TESS fields to compress: {num_doc}')

    if len(num_doc) > 0:

        cursor = db[collection_alerts].find(query).hint(hint).limit(3)

        # for alert in cursor.limit(1):
        for alert in tqdm.tqdm(cursor, total=num_doc):
            # print(alert['candid'])
            try:
                with open(os.path.join(path_date, f"{alert['candid']}.json"), 'w') as f:
                    f.write(dumps(alert))
            except Exception as e:
                print(time_stamps(), str(e))

        # compress
        print(time_stamps(), 'Compressing')
        subprocess.run(['/bin/tar', '-zcvf', os.path.join(config['path']['path_tess'], f'{datestr}.tar.gz'),
                        '-C', config['path']['path_tess'], datestr])
        print(time_stamps(), 'Compressed')

        # todo: cp to GC

    else:
        print(time_stamps(), 'Nothing to do')

    print(time_stamps(), 'Disconnecting from DB')
    client.close()
    print(time_stamps(), 'Successfully disconnected')


# schedule.every(10).seconds.do(dump_tess)
schedule.every().day.at("14:30").do(dump_tess)


if __name__ == '__main__':

    # dump_tess()

    while True:
        schedule.run_pending()
        time.sleep(60)
