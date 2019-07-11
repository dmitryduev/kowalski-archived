import pymongo
import json
import argparse
import traceback
import datetime
import pytz
from astropy.time import Time
import tqdm
from bson.json_util import loads, dumps
import numpy as np
import pandas as pd


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


# cone search radius:
cone_search_radius = float(config['xmatch']['cone_search_radius'])
# convert to rad:
if config['xmatch']['cone_search_unit'] == 'arcsec':
    cone_search_radius *= np.pi / 180.0 / 3600.
elif config['xmatch']['cone_search_unit'] == 'arcmin':
    cone_search_radius *= np.pi / 180.0 / 60.
elif config['xmatch']['cone_search_unit'] == 'deg':
    cone_search_radius *= np.pi / 180.0
elif config['xmatch']['cone_search_unit'] == 'rad':
    cone_search_radius *= 1
else:
    raise Exception('Unknown cone search unit. Must be in [deg, rad, arcsec, arcmin]')


def alert_filter__xmatch(db, alert):
    """Filter to apply to each alert.
    """

    xmatches = dict()

    try:
        ra_geojson = float(alert['candidate']['ra'])
        # geojson-friendly ra:
        ra_geojson -= 180.0
        dec_geojson = float(alert['candidate']['dec'])

        ''' catalogs '''
        for catalog in config['xmatch']['catalogs']:
            catalog_filter = config['xmatch']['catalogs'][catalog]['filter']
            catalog_projection = config['xmatch']['catalogs'][catalog]['projection']

            object_position_query = dict()
            object_position_query['coordinates.radec_geojson'] = {
                '$geoWithin': {'$centerSphere': [[ra_geojson, dec_geojson], cone_search_radius]}}
            s = db[catalog].find({**object_position_query, **catalog_filter},
                                 {**catalog_projection})
            xmatches[catalog] = list(s)

    except Exception as e:
        print(*time_stamps(), str(e))

    return xmatches


def main(obs_date=datetime.datetime.utcnow().strftime('%Y%m%d')):

    jd = Time(datetime.datetime.strptime(obs_date, '%Y%m%d')).jd

    collection_alerts = 'ZTF_alerts'

    # connect to MongoDB:
    print('Connecting to DB')
    client, db = connect_to_db()
    print('Successfully connected')

    query = {'candidate.jd': {'$gt': jd, '$lt': jd+1},
             'candidate.programid': 1}

    # index name to use:
    hint = 'candidate.jd_1_candidate.programid_1'

    num_doc = db[collection_alerts].count_documents(query, hint=hint)
    print(num_doc)

    cursor = db[collection_alerts].find(query,
                                        {'candidate.ra': 1, 'candidate.dec': 1}).hint(hint)#.limit(1000)

    # for alert in cursor.limit(1):
    for alert in tqdm.tqdm(cursor, total=num_doc):
        xmatches = alert_filter__xmatch(db, alert)
        # print(alert['_id'], xmatches)
        db[collection_alerts].update_one({'_id': alert['_id']},
                                         {'$set': {'cross_matches': xmatches}})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Cross-match a night worth of ZTF alerts')
    parser.add_argument('--obsdate', help='observing date', default=datetime.datetime.utcnow().strftime('%Y%m%d'))

    args = parser.parse_args()

    main(obs_date=args.obsdate)
