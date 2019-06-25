import pandas as pd
import os
import glob
import time
# from astropy.coordinates import Angle
import numpy as np
import pymongo
import json
import traceback
import datetime
import pytz
from numba import jit


''' load config and secrets '''
with open('/app/config.json') as cjson:
    config = json.load(cjson)

with open('/app/secrets.json') as sjson:
    secrets = json.load(sjson)

for k in secrets:
    config[k].update(secrets.get(k, {}))


def utc_now():
    return datetime.datetime.now(pytz.utc)


def yield_batch(seq, num_batches: int = 20):
    batch_size = int(np.ceil(len(seq) / num_batches))

    for nb in range(num_batches):
        yield seq[nb*batch_size: (nb+1)*batch_size]


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

    # connect to MongoDB:
    print('Connecting to DB')
    client, db = connect_to_db()
    print('Successfully connected')

    t_tag = '20190625'

    _collection = f'CLU_{t_tag}'

    # create 2d index:
    print('Creating 2d index')
    db[_collection].create_index([('coordinates.radec_geojson', '2dsphere'),
                                  ('_id', pymongo.ASCENDING)], background=True)

    clu_fits = f'/_tmp/clu/CLU_{t_tag}.hdf5'

    data = pd.read_hdf(clu_fits, key='data')

    for ii, dff in enumerate(yield_batch(data, num_batches=100)):

        print(f'Processing chunk # {ii+1} of 100')

        # number of records to insert
        documents = []

        # if ii == 63267:

        # dff.rename(index=str, columns={'id': '_id'}, inplace=True)
        dff['_id'] = dff['cluid']
        batch = dff.to_dict(orient='records')

        for ie, doc in enumerate(batch):
            try:
                # # print(cf)
                # doc = {k: v for k, v in zip(field_names, cf)}
                # # print(cf)
                #
                # for ii, kk in enumerate(field_names):
                #     if doc[kk] == 'N/A' or doc[kk] == 'n/a':
                #         doc[kk] = None
                #     else:
                #         if field_data_types[ii] in (float, int):
                #             doc[kk] = field_data_types[ii](doc[kk])
                #         else:
                #             doc[kk] = str(doc[kk]).strip()

                doc['_id'] = doc['cluid']

                # GeoJSON for 2D indexing
                doc['coordinates'] = {}
                # doc['coordinates']['epoch'] = doc['jd']
                _ra = doc['ra']
                _dec = doc['dec']
                _radec = [_ra, _dec]
                # string format: H:M:S, D:M:S
                # tic = time.time()
                _radec_str = [deg2hms(_ra), deg2dms(_dec)]
                # print(time.time() - tic)
                # print(_radec_str)
                doc['coordinates']['radec_str'] = _radec_str
                # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
                _radec_geojson = [_ra - 180.0, _dec]
                doc['coordinates']['radec_geojson'] = {'type': 'Point',
                                                       'coordinates': _radec_geojson}
                # radians and degrees:
                doc['coordinates']['radec_rad'] = [_ra * np.pi / 180.0, _dec * np.pi / 180.0]
                doc['coordinates']['radec_deg'] = [_ra, _dec]

                documents.append(doc)

            except Exception as e:
                traceback.print_exc()
                print(e)
                continue

        #
        if len(documents) > 0:
            print(f'inserting chunk #{ii+1}')
            insert_multiple_db_entries(db, _collection=_collection, _db_entries=documents)

    print('All done')
