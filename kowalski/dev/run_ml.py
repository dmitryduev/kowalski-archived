import pymongo
import json
import argparse
import traceback
import datetime
import pytz
from astropy.time import Time
import tqdm


''' load config and secrets '''
with open('/app/config.json') as cjson:
    config = json.load(cjson)

with open('/app/secrets.json') as sjson:
    secrets = json.load(sjson)

for k_ in secrets:
    config[k_].update(secrets.get(k_, {}))


def utc_now():
    return datetime.datetime.now(pytz.utc)


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


def insert_db_entry(_db, _collection=None, _db_entry=None):
    """
        Insert a document _doc to collection _collection in DB.
        It is monitored for timeout in case DB connection hangs for some reason
    :param _collection:
    :param _db_entry:
    :return:
    """
    assert _collection is not None, 'Must specify collection'
    assert _db_entry is not None, 'Must specify document'
    try:
        _db[_collection].insert_one(_db_entry)
    except Exception as _e:
        print('Error inserting {:s} into {:s}'.format(str(_db_entry['_id']), _collection))
        traceback.print_exc()
        print(_e)


def insert_multiple_db_entries(_db, _collection=None, _db_entries=None, _verbose=False):
    """
        Insert a document _doc to collection _collection in DB.
        It is monitored for timeout in case DB connection hangs for some reason
    :param _db:
    :param _collection:
    :param _db_entries:
    :param _verbose:
    :return:
    """
    assert _collection is not None, 'Must specify collection'
    assert _db_entries is not None, 'Must specify documents'
    try:
        _db[_collection].insert_many(_db_entries, ordered=False)
    except pymongo.errors.BulkWriteError as bwe:
        if _verbose:
            print(bwe.details)
    except Exception as _e:
        if _verbose:
            traceback.print_exc()
            print(_e)


def main(obs_date=datetime.datetime.utcnow().strftime('%Y%m%d')):

    jd = Time(datetime.datetime.strptime(obs_date, '%Y%m%d')).jd

    collection_alerts = 'ZTF_alerts'

    # connect to MongoDB:
    print('Connecting to DB')
    client, db = connect_to_db()
    print('Successfully connected')

    # db[collection_alerts].create_index([('candidate.jd', pymongo.DESCENDING),
    #                                     ('classifications.braai', pymongo.DESCENDING),
    #                                     ('candid', pymongo.DESCENDING)],
    #                                    background=True)

    num_doc = db[collection_alerts].count_documents({'candidate.jd': {'$gt': jd, '$lt': jd+1}})
    print(num_doc)

    # for doc in cursor:



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Apply ML models on a night worth of ZTF alerts')
    parser.add_argument('--obsdate', help='observing date', default=datetime.datetime.utcnow().strftime('%Y%m%d'))

    args = parser.parse_args()

    main(obs_date=args.obsdate)
