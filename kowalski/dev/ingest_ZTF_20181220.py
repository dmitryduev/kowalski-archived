import numpy as np
import pandas as pd
import pymongo
import json
import argparse
import traceback
import datetime
import pytz
import time
from numba import jit


''' load config and secrets '''

# with open('/Users/dmitryduev/_caltech/python/kowalski/kowalski/config.json') as cjson:
with open('/app/config.json') as cjson:
    config = json.load(cjson)

# with open('/Users/dmitryduev/_caltech/python/kowalski/secrets.json') as sjson:
with open('/app/secrets.json') as sjson:
    secrets = json.load(sjson)

for k in secrets:
    config[k].update(secrets.get(k, {}))


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


def insert_multiple_db_entries(_db, _collection=None, _db_entries=None):
    """
        Insert a document _doc to collection _collection in DB.
        It is monitored for timeout in case DB connection hangs for some reason
    :param _db:
    :param _collection:
    :param _db_entries:
    :return:
    """
    assert _collection is not None, 'Must specify collection'
    assert _db_entries is not None, 'Must specify documents'
    try:
        _db[_collection].insert_many(_db_entries, ordered=False)
    except pymongo.errors.BulkWriteError as bwe:
        print(bwe.details)
    except Exception as _e:
        traceback.print_exc()
        print(_e)


@jit
def deg2hms(x):
    """Transform degrees to *hours:minutes:seconds* strings.

    Parameters
    ----------
    x : float
        The degree value c [0, 360) to be written as a sexagesimal string.

    Returns
    -------
    out : str
        The input angle written as a sexagesimal string, in the
        form, hours:minutes:seconds.

    """
    assert 0.0 <= x < 360.0, 'Bad RA value in degrees'
    # ac = Angle(x, unit='degree')
    # hms = str(ac.to_string(unit='hour', sep=':', pad=True))
    # print(str(hms))
    _h = np.floor(x * 12.0 / 180.)
    _m = np.floor((x * 12.0 / 180. - _h) * 60.0)
    _s = ((x * 12.0 / 180. - _h) * 60.0 - _m) * 60.0
    hms = '{:02.0f}:{:02.0f}:{:07.4f}'.format(_h, _m, _s)
    # print(hms)
    return hms


@jit
def deg2dms(x):
    """Transform degrees to *degrees:arcminutes:arcseconds* strings.

    Parameters
    ----------
    x : float
        The degree value c [-90, 90] to be converted.

    Returns
    -------
    out : str
        The input angle as a string, written as degrees:minutes:seconds.

    """
    assert -90.0 <= x <= 90.0, 'Bad Dec value in degrees'
    # ac = Angle(x, unit='degree')
    # dms = str(ac.to_string(unit='degree', sep=':', pad=True))
    # print(dms)
    _d = np.floor(abs(x)) * np.sign(x)
    _m = np.floor(np.abs(x - _d) * 60.0)
    _s = np.abs(np.abs(x - _d) * 60.0 - _m) * 60.0
    dms = '{:02.0f}:{:02.0f}:{:06.3f}'.format(_d, _m, _s)
    # print(dms)
    return dms


if __name__ == '__main__':
    ''' Create command line argument parser '''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description='')

    args = parser.parse_args()

    # connect to MongoDB:
    print('Connecting to DB')
    client, db = connect_to_db()
    print('Successfully connected')

    _collection = 'ZTF_20181220'

    # create 2d index:
    print('Creating 2d index')
    db[_collection].create_index([('coordinates.radec_geojson', '2dsphere'),
                                  ('_id', pymongo.ASCENDING)], background=True)

    # number of records to insert
    batch_size = 2048
    batch_num = 1
    documents = []

    f_in = '/_tmp/ZTF_20181220.csv'
    # f_in = '/Users/dmitryduev/_caltech/python/kowalski/kowalski/dev/ZTF_20181220.csv'

    # # load csv into memory
    # print('Loading input file into memory:')
    # df = pd.read_csv(f_in)
    # print('Done')

    # total = df.shape[0]
    total = 2348920269
    num_chunks = total // batch_size
    print(num_chunks)

    # print(df.to_dict(orient='records'))

    # for ii, dff in enumerate(pd.read_csv(f_in, chunksize=2)):
    for ii, dff in enumerate(pd.read_csv(f_in, chunksize=batch_size)):

        print(f'Processing batch # {ii+1} of {num_chunks}')

        dff.rename(index=str, columns={'id': '_id'}, inplace=True)
        batch = dff.to_dict(orient='records')

        for ie, doc in enumerate(batch):
            # fix types:
            for k in ('_id', 'nobs', 'ngoodobs', 'nbestobs', ):
                doc[k] = int(doc[k])
            for k in ('ra', 'dec', 'refmag', 'bestmedianmag', 'bestmedianabsdev', 'iqr'):
                doc[k] = float(doc[k])

            # GeoJSON for 2D indexing
            doc['coordinates'] = dict()
            # string format: H:M:S, D:M:S
            doc['coordinates']['radec_str'] = [deg2hms(doc['ra']), deg2dms(doc['dec'])]
            # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
            _radec_geojson = [doc['ra'] - 180.0, doc['dec']]
            doc['coordinates']['radec_geojson'] = {'type': 'Point',
                                                   'coordinates': _radec_geojson}

        # print(batch)

        # ingest
        insert_multiple_db_entries(db, _collection=_collection, _db_entries=documents)
