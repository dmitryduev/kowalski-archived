import os
import glob
import pandas as pd
import json
import argparse
from concurrent.futures import ProcessPoolExecutor
import datetime
import pytz
import pymongo
import traceback
from numba import jit
import numpy as np


def utc_now():
    return datetime.datetime.now(pytz.utc)


def connect_to_db(host, port, db, user, pwd):
    """ Connect to the mongodb database

    :return:
    """
    try:
        # there's only one instance of DB, it's too big to be replicated
        _client = pymongo.MongoClient(host=host, port=port)
        # grab main database:
        _db = _client[db]
    except Exception as _e:
        raise ConnectionRefusedError
    try:
        # authenticate
        _db.authenticate(user, pwd)
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


''' load config and secrets '''
with open('/app/config.json') as cjson:
    config = json.load(cjson)

with open('/app/secrets.json') as sjson:
    secrets = json.load(sjson)

for k in secrets:
    config[k].update(secrets.get(k, {}))


def process_file(_file, _collection, _batch_size=2048, verbose=False, _dry_run=False):

    # connect to MongoDB:
    if verbose:
        print('Connecting to DB')
    _client, _db = connect_to_db(config['database']['host'], config['database']['port'],
                                 config['database']['db'], config['database']['user'], config['database']['pwd'])
    if verbose:
        print('Successfully connected')

    # column names:
    column_names = [('objID', int),
                    ('projectionID', int),
                    ('skyCellID', int),
                    ('objInfoFlag', int),
                    ('qualityFlag', int),
                    ('raMean', float),
                    ('decMean', float),
                    ('raMeanErr', float),
                    ('decMeanErr', float),
                    ('epochMean', float),
                    ('nStackDetections', int),
                    ('nDetections', int),
                    ('ng', int),
                    ('nr', int),
                    ('ni', int),
                    ('nz', int),
                    ('ny', int),
                    ('gQfPerfect', float),
                    ('gMeanPSFMag', float),
                    ('gMeanPSFMagErr', float),
                    ('gMeanPSFMagStd', float),
                    ('gMeanPSFMagNpt', float),
                    ('gMeanPSFMagMin', float),
                    ('gMeanPSFMagMax', float),
                    ('gMeanKronMag', float),
                    ('gMeanKronMagErr', float),
                    ('gFlags', int),
                    ('rQfPerfect', float),
                    ('rMeanPSFMag', float),
                    ('rMeanPSFMagErr', float),
                    ('rMeanPSFMagStd', float),
                    ('rMeanPSFMagNpt', float),
                    ('rMeanPSFMagMin', float),
                    ('rMeanPSFMagMax', float),
                    ('rMeanKronMag', float),
                    ('rMeanKronMagErr', float),
                    ('rFlags', int),
                    ('iQfPerfect', float),
                    ('iMeanPSFMag', float),
                    ('iMeanPSFMagErr', float),
                    ('iMeanPSFMagStd', float),
                    ('iMeanPSFMagNpt', float),
                    ('iMeanPSFMagMin', float),
                    ('iMeanPSFMagMax', float),
                    ('iMeanKronMag', float),
                    ('iMeanKronMagErr', float),
                    ('iFlags', int),
                    ('zQfPerfect', float),
                    ('zMeanPSFMag', float),
                    ('zMeanPSFMagErr', float),
                    ('zMeanPSFMagStd', float),
                    ('zMeanPSFMagNpt', float),
                    ('zMeanPSFMagMin', float),
                    ('zMeanPSFMagMax', float),
                    ('zMeanKronMag', float),
                    ('zMeanKronMagErr', float),
                    ('zFlags', int),
                    ('yQfPerfect', float),
                    ('yMeanPSFMag', float),
                    ('yMeanPSFMagErr', float),
                    ('yMeanPSFMagStd', float),
                    ('yMeanPSFMagNpt', float),
                    ('yMeanPSFMagMin', float),
                    ('yMeanPSFMagMax', float),
                    ('yMeanKronMag', float),
                    ('yMeanKronMagErr', float),
                    ('yFlags', int)]
    colnames = [c[0] for c in column_names]

    print(f'processing {_file}')

    for ii, dff in enumerate(pd.read_csv(_file, chunksize=_batch_size, header=None, names=colnames)):

        print(f'{_file}: processing batch # {ii + 1}')

        dff.rename(index=str, columns={'objID': '_id'}, inplace=True)
        batch = dff.to_dict(orient='records')

        bad_doc_ind = []

        for ie, doc in enumerate(batch):
            try:
                keys = list(doc.keys())
                for key in keys:
                    if (doc[key] == 'NOT_AVAILABLE') or (doc[key] == -999.0):
                        doc.pop(key, None)

                # GeoJSON for 2D indexing
                doc['coordinates'] = dict()
                # string format: H:M:S, D:M:S
                doc['coordinates']['radec_str'] = [deg2hms(doc['raMean']), deg2dms(doc['decMean'])]
                # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
                _radec_geojson = [doc['raMean'] - 180.0, doc['decMean']]
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
    client, db = connect_to_db(config['database']['host'], config['database']['port'],
                               config['database']['db'], config['database']['user'], config['database']['pwd'])
    print('Successfully connected')

    collection = 'PS1_DR1'

    # create 2d index:
    print('Creating 2d index')
    if not dry_run:
        db[collection].create_index([('coordinates.radec_geojson', '2dsphere'),
                                     ('_id', pymongo.ASCENDING)], background=True)

    # number of records to insert
    batch_size = 4096

    _location = '/_tmp/panstarrs/'

    files = glob.glob(os.path.join(_location, 'OTMO*.csv'))

    print(f'# files to process: {len(files)}')

    # init threaded operations
    # pool = ThreadPoolExecutor(2)
    pool = ProcessPoolExecutor(30)

    # for ff in files[::-1]:
    for ff in sorted(files):
        pool.submit(process_file, _file=ff, _collection=collection, _batch_size=batch_size,
                    verbose=True, _dry_run=dry_run)
        # process_file(_file=ff, _collection=collection, _batch_size=batch_size,
        #              verbose=True, _dry_run=dry_run)

    # wait for everything to finish
    pool.shutdown(wait=True)

    print('All done')
