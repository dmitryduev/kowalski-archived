import os
import glob
import time
# from astropy.coordinates import Angle
import numpy as np
import pymongo
import json
import argparse
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


# @timeout_decorator.timeout(60, use_signals=False)
# @timeout(seconds_before_timeout=60)
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


# @timeout(seconds_before_timeout=60)
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


@jit(forceobj=True)
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
    hms = '{:02.0f}:{:02.0f}:{:09.6f}'.format(_h, _m, _s)
    # print(hms)
    return hms


@jit(forceobj=True)
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
    dms = '{:02.0f}:{:02.0f}:{:08.5f}'.format(_d, _m, _s)
    # print(dms)
    return dms


if __name__ == '__main__':
    ''' Create command line argument parser '''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description='')

    parser.add_argument('config_file', metavar='config_file',
                        action='store', help='path to config file.', type=str)

    # connect to MongoDB:
    print('Connecting to DB')
    client, db = connect_to_db()
    print('Successfully connected')

    # _collection = 'RFC_2017c'
    # _collection = 'RFC_2018d'
    # _collection = 'RFC_2019a'
    _collection = 'RFC_2019d'

    # create 2d index:
    print('Creating 2d index')
    db[_collection].create_index([('coordinates.radec_geojson', '2dsphere'),
                                  ('_id', pymongo.ASCENDING)], background=True)

    # number of records to insert
    batch_size = 2048
    batch_num = 1
    documents = []

    f_in = f'/_tmp/{_collection.lower()}_cat.txt'

    field_names = ['category', 'IVS_name', 'J2000_name', 'ra', 'dec', 'ra_error_mas',
                   'dec_error_mas', 'corr', 'n_obs',
                   'S_band_flux_total', 'S_band_flux_unres', 'C_band_flux_total', 'C_band_flux_unres',
                   'X_band_flux_total', 'X_band_flux_unres', 'U_band_flux_total', 'U_band_flux_unres',
                   'K_band_flux_total', 'K_band_flux_unres', 'type', 'cat']
    field_data_types = [*[str]*3, *[float]*5, int, *[float]*10, *[str]*2]

    with open(f_in, 'r') as f:

        f_lines = [l for l in f.readlines() if l[0] != '#']

        total = len(f_lines)

        for ci, cf in enumerate(f_lines):
            print(f'processing entry #{ci+1} of {total}')

            try:
                # print(cf)
                tmp = cf.replace('<', '').replace('>', '').split()

                # replace '-1.00' with None
                tmp = [_t if _t != '-1.00' else None for _t in tmp]

                dec_sign = np.sign(float(tmp[6])) if abs(float(tmp[6])) >= 1e-9 else 1
                cf = tmp[0:3] + [(float(tmp[3]) + float(tmp[4])/60.0 + float(tmp[5])/3600.0)*180.0/12.0] + \
                     [dec_sign*(abs(float(tmp[6])) + float(tmp[7]) / 60.0 + float(tmp[8]) / 3600.0)]

                tmp2 = [field_data_types[5+ti](_t) if _t is not None else None for (ti, _t) in enumerate(tmp[9:])]
                cf += tmp2

                # print(cf)

                doc = {k: v for k, v in zip(field_names, cf)}
                # print(cf)

                doc['_id'] = doc['J2000_name']

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
                # doc['coordinates']['radec_rad'] = [_ra * np.pi / 180.0, _dec * np.pi / 180.0]
                # doc['coordinates']['radec_deg'] = [_ra, _dec]

                # print(doc['coordinates'])
                documents.append(doc)

                # insert batch, then flush
                if len(documents) == batch_size:
                    print(f'inserting batch #{batch_num}')
                    insert_multiple_db_entries(db, _collection=_collection, _db_entries=documents)
                    # flush:
                    documents = []
                    batch_num += 1

            except Exception as e:
                traceback.print_exc()
                print(e)
                continue

        # stuff left from the last file?
        if len(documents) > 0:
            print(f'inserting batch #{batch_num}')
            insert_multiple_db_entries(db, _collection=_collection, _db_entries=documents)

        print('All done')
