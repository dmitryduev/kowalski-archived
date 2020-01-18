import argparse
import numpy as np
import sys
import json
import pymongo
import traceback
import datetime
import os
import time
import pandas as pd
import pytz
import requests
from utils import radec_str2rad, radec_str2geojson


''' load config and secrets '''
with open('/app/config.json') as cjson:
    config = json.load(cjson)

with open('/app/secrets.json') as sjson:
    secrets = json.load(sjson)

for k in secrets:
    if k in config:
        config[k].update(secrets.get(k, {}))
    else:
        config[k] = secrets.get(k, {})


def connect_to_db(_config):
    """ Connect to the mongodb database

    :return:
    """
    try:
        # there's only one instance of DB, it's too big to be replicated
        _client = pymongo.MongoClient(host=_config['database']['host'],
                                      port=_config['database']['port'])
        # grab main database:
        _db = _client[_config['database']['db']]
    except Exception as _e:
        raise ConnectionRefusedError
    try:
        # authenticate
        _db.authenticate(_config['database']['user'], _config['database']['pwd'])
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


def mongify(doc):

    if doc['ra'] == -99. and doc['dec'] == -99.0:
        return doc

    # GeoJSON for 2D indexing
    doc['coordinates'] = {}
    # doc['coordinates']['epoch'] = 2000.0
    _ra_str = doc['ra']
    _dec_str = doc['dec']

    _radec_str = [_ra_str, _dec_str]
    _ra_geojson, _dec_geojson = radec_str2geojson(_ra_str, _dec_str)

    doc['coordinates']['radec_str'] = _radec_str

    doc['coordinates']['radec_geojson'] = {'type': 'Point',
                                           'coordinates': [_ra_geojson, _dec_geojson]}

    return doc


def get_ops():
    """

    """

    # connect to MongoDB:
    print('Connecting to DB')
    client, db = connect_to_db(config)
    print('Successfully connected')

    collection = 'ZTF_ops'

    db[collection].create_index([('coordinates.radec_geojson', '2dsphere')], background=True)
    db[collection].create_index([('utc_start', pymongo.ASCENDING),
                                 ('utc_end', pymongo.ASCENDING),
                                 ('fileroot', pymongo.ASCENDING)], background=True)

    # fetch full table
    url = secrets['ztf_ops']['url']
    r = requests.get(url, auth=(secrets['ztf_ops']['username'], secrets['ztf_ops']['password']))
    if r.status_code == requests.codes.ok:
        with open(os.path.join(config['path']['path_tmp'], 'allexp.tbl'), 'wb') as f:
            f.write(r.content)
    else:
        print(datetime.datetime.utcnow())
        raise Exception('Failed to fetch allexp.tbl')

    c = db[collection].find({}, sort=[["$natural", -1]])
    print(list(c))

    df = pd.read_fwf(os.path.join(config['path']['path_tmp'], 'allexp.tbl'), comment='|', header=None,
                     names=['utc_start', 'sun_elevation',
                            'exp', 'filter', 'type', 'field', 'pid',
                            'ra', 'dec', 'slew', 'wait', 'fileroot', 'programpi', 'qcomment'])

    # drop comments:
    comments = df['utc_start'] == 'UT_START'
    df = df.loc[~comments]

    for col in ['sun_elevation', 'exp', 'filter', 'field', 'pid']:
        df[col] = df[col].apply(lambda x: int(x))
    for col in ['ra', 'dec', 'slew', 'wait']:
        df[col] = df[col].apply(lambda x: float(x))

    df['utc_start'] = df['utc_start'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f'))
    df['utc_end'] = df['utc_start'].add(df['exp'].apply(lambda x: datetime.timedelta(seconds=x)))

    # FIXME: TODO: drop rows with utc_start <= c['utc_start]

    documents = df.to_dict('records')

    documents = [mongify(doc) for doc in documents]

    insert_multiple_db_entries(db, _collection=collection, _db_entries=documents, _verbose=False)

    # kk = 30 if grab_all else 1
    # for page in range(kk):
    #     # print(page)
    #     url = f'https://wis-tns.weizmann.ac.il/search?format=csv&num_page=1000&page={page}'
    #
    #     data = pd.read_csv(url)
    #     # print(data)
    #
    #     documents = []
    #
    #     for index, row in data.iterrows():
    #         doc = mongify(row)
    #         documents.append(doc)
    #
    #     insert_multiple_db_entries(db, _collection=collection, _db_entries=documents, _verbose=False)

    # close connection to db
    client.close()
    print('Disconnected from db')


def main():

    while True:
        try:
            get_ops()

        except KeyboardInterrupt:
            sys.stderr.write('Aborted by user\n')
            sys.exit()

        except Exception as e:
            print(str(e))
            traceback.print_exc()

        #
        time.sleep(600)


if __name__ == '__main__':

    main()
