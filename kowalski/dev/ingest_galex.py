import csv
import os
import glob
import time
# from astropy.coordinates import Angle
import numpy as np
import pandas as pd
import pymongo
import json
import argparse
import traceback
import datetime
import pytz
from numba import jit
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor


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
    hms = '{:02.0f}:{:02.0f}:{:07.4f}'.format(_h, _m, _s)
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
    dms = '{:02.0f}:{:02.0f}:{:06.3f}'.format(_d, _m, _s)
    # print(dms)
    return dms


def process_file(_file, _collection, _batch_size=2048, verbose=False, _dry_run=False):

    # connect to MongoDB:
    if verbose:
        print('Connecting to DB')
    _client, _db = connect_to_db()
    if verbose:
        print('Successfully connected')

    print(f'processing {_file}')

    for ii, df in enumerate(pd.read_csv(_file,
                            skip_blank_lines=True, comment='#',
                            sep='|', chunksize=_batch_size)):

        print(f'{_file}: processing batch # {ii + 1}')

        df = df.drop([0, 1]).drop(['_RAJ2000', '_DEJ2000'], axis=1).reset_index(drop=True)
        df.rename(columns={'RAJ2000': 'ra', 'DEJ2000': 'dec'}, inplace=True)
        for col in ['ra', 'dec', 'FUVmag', 'e_FUVmag', 'NUVmag', 'e_NUVmag', 'Fr', 'Nr']:
            df[col] = df[col].apply(lambda x: float(x) if len(x.strip()) > 0 else np.nan)
        for col in ['b', 'Fafl', 'Nafl', 'Fexf', 'Nexf']:
            df[col] = df[col].apply(lambda x: int(x) if len(x.strip()) > 0 else np.nan)

        batch = [{k: v for k, v in m.items() if pd.notnull(v)} for m in df.to_dict(orient='rows')]

        bad_doc_ind = []

        for ie, doc in enumerate(batch):
            try:
                # GeoJSON for 2D indexing
                # print(doc['ra'], doc['dec'])
                doc['coordinates'] = dict()
                # string format: H:M:S, D:M:S
                doc['coordinates']['radec_str'] = [deg2hms(doc['ra']), deg2dms(doc['dec'])]
                # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
                _radec_geojson = [doc['ra'] - 180.0, doc['dec']]
                doc['coordinates']['radec_geojson'] = {'type': 'Point',
                                                       'coordinates': _radec_geojson}
            except Exception as e:
                print(str(e))
                bad_doc_ind.append(ie)

        if len(bad_doc_ind) > 0:
            print('removing bad docs')
            for index in sorted(bad_doc_ind, reverse=True):
                del batch[index]

        # print(batch)

        # ingest
        insert_multiple_db_entries(_db, _collection=_collection, _db_entries=batch)

    # disconnect from db:
    try:
        _client.close()
    finally:
        if verbose:
            print('Successfully disconnected from db')


if __name__ == '__main__':
    ''' Create command line argument parser '''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description='')

    parser.add_argument('--dryrun', action='store_true', help='enforce execution')

    args = parser.parse_args()

    dry_run = args.dryrun

    # connect to MongoDB:
    print('Connecting to DB')
    client, db = connect_to_db()
    print('Successfully connected')

    collection = 'GALEX'

    # create 2d index:
    print('Creating 2d index')
    if not dry_run:
        db[collection].create_index([('coordinates.radec_geojson', '2dsphere'),
                                     ('name', pymongo.ASCENDING)], background=True)

    # number of records to insert
    batch_size = 4096

    _location = '/_tmp/galex/'

    files = glob.glob(os.path.join(_location, '*.tsv'))

    print(f'# files to process: {len(files)}')

    # init threaded operations
    # pool = ThreadPoolExecutor(2)
    # pool = ProcessPoolExecutor(6)

    # for ff in files[::-1]:
    for ff in sorted(files):
        # pool.submit(process_file, _file=ff, _collection=collection, _batch_size=batch_size,
        #             verbose=True, _dry_run=dry_run)
        process_file(_file=ff, _collection=collection, _batch_size=batch_size,
                     verbose=True, _dry_run=dry_run)

    # wait for everything to finish
    # pool.shutdown(wait=True)

    print('All done')
