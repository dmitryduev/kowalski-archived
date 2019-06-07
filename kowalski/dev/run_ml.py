import pymongo
import json
import argparse
import traceback
import datetime
import pytz
from astropy.time import Time
import tqdm
from tensorflow.keras.models import load_model
from tensorflow.keras.utils import normalize
from bson.json_util import loads, dumps
import numpy as np
import pandas as pd
import gzip
import io
from astropy.io import fits


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


def make_triplet(alert, to_tpu: bool = False):
    """
        Feed in alert packet
    """
    cutout_dict = dict()

    for cutout in ('science', 'template', 'difference'):
        cutout_data = loads(dumps([alert[f'cutout{cutout.capitalize()}']['stampData']]))[0]

        # unzip
        with gzip.open(io.BytesIO(cutout_data), 'rb') as f:
            with fits.open(io.BytesIO(f.read())) as hdu:
                data = hdu[0].data
                # replace nans with zeros
                cutout_dict[cutout] = np.nan_to_num(data)
                # normalize
                cutout_dict[cutout] = normalize(cutout_dict[cutout])

        # pad to 63x63 if smaller
        shape = cutout_dict[cutout].shape
        if shape != (63, 63):
            # print(f'Shape of {candid}/{cutout}: {shape}, padding to (63, 63)')
            cutout_dict[cutout] = np.pad(cutout_dict[cutout], [(0, 63 - shape[0]), (0, 63 - shape[1])],
                                         mode='constant', constant_values=1e-9)

    triplet = np.zeros((63, 63, 3))
    triplet[:, :, 0] = cutout_dict['science']
    triplet[:, :, 1] = cutout_dict['template']
    triplet[:, :, 2] = cutout_dict['difference']

    if to_tpu:
        # Edge TPUs require additional processing
        triplet = np.rint(triplet * 128 + 128).astype(np.uint8).flatten()

    return triplet


def alert_filter__ml(alert, ml_models: dict = None):
    """Filter to apply to each alert.
    """

    scores = dict()

    try:
        ''' braai '''
        triplet = make_triplet(alert)
        triplets = np.expand_dims(triplet, axis=0)
        braai = ml_models['braai']['model'].predict(x=triplets)[0]
        # braai = 1.0
        scores['braai'] = float(braai)
        scores['braai_version'] = ml_models['braai']['version']
    except Exception as e:
        print(*time_stamps(), str(e))

    return scores


def main(obs_date=datetime.datetime.utcnow().strftime('%Y%m%d')):

    # ML models:
    ml_models = dict()
    for m in config['ml_models']:
        try:
            m_v = config["ml_models"][m]["version"]
            ml_models[m] = {'model': load_model(f'/app/models/{m}_{m_v}.h5'),
                            'version': m_v}
        except Exception as e:
            print(*time_stamps(), f'Error loading ML model {m}')
            traceback.print_exc()
            print(e)
            continue

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

    cursor = db[collection_alerts].find({'candidate.jd': {'$gt': jd, '$lt': jd+1}},
                                        {'candidate': 0, 'prv_candidates': 0, 'coordinates': 0})

    # for alert in tqdm.tqdm(cursor, total=num_doc):
    for alert in cursor:
        scores = alert_filter__ml(alert, ml_models)
        print(alert['candid'], scores)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Apply ML models on a night worth of ZTF alerts')
    parser.add_argument('--obsdate', help='observing date', default=datetime.datetime.utcnow().strftime('%Y%m%d'))

    args = parser.parse_args()

    main(obs_date=args.obsdate)
