import csv
import os
import glob
import numpy as np
import pymongo
import json
import argparse
import traceback
import datetime
import pytz
import time
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
        # print(bwe.details)
        print('insertion error')
    except Exception as _e:
        # traceback.print_exc()
        # print(_e)
        print('insertion error')


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

    if verbose:
        print(f'processing {_file}')
    documents = []
    batch_num = 1

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

    try:
        with open(_file) as f:
            reader = csv.reader(f, delimiter=',')
            # skip header:
            f.readline()
            for row in reader:
                try:
                    doc = {column_name[0]: val for column_name, val in zip(column_names, row)}

                    doc['_id'] = doc['objID']

                    # convert
                    for col_name, col_type in column_names:
                        try:
                            if doc[col_name] == 'NOT_AVAILABLE':
                                continue
                            elif doc[col_name] in ('false', 'true'):
                                doc[col_name] = eval(doc[col_name].capitalize())
                            elif len(doc[col_name]) == 0:
                                doc[col_name] = None
                            else:
                                doc[col_name] = col_type(doc[col_name])
                        except Exception as e:
                            traceback.print_exc()
                            print(e)

                    # print(doc)
                    # print('\n')

                    doc['coordinates'] = {}
                    doc['coordinates']['epoch'] = doc['epochMean']
                    _ra = doc['raMean']
                    _dec = doc['decMean']
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

                    # print(doc['coordinates'])

                    documents.append(doc)

                    # time.sleep(1)

                    # insert batch, then flush
                    if len(documents) % batch_size == 0:
                        print(f'inserting batch #{batch_num}')
                        if not _dry_run:
                            insert_multiple_db_entries(_db, _collection=_collection, _db_entries=documents)
                        # flush:
                        documents = []
                        batch_num += 1

                except Exception as e:
                    traceback.print_exc()
                    print(e)
                    continue

    except Exception as e:
        traceback.print_exc()
        print(e)

    # stuff left from the last file?
    while len(documents) > 0:
        try:
            # In case mongo crashed and disconnected, docs will accumulate in documents
            # keep on trying to insert them until successful
            print(f'inserting batch #{batch_num}')
            if not _dry_run:
                insert_multiple_db_entries(_db, _collection=_collection, _db_entries=documents)
                # flush:
                documents = []

        except Exception as e:
            traceback.print_exc()
            print(e)
            print('Failed, waiting 5 seconds to retry')
            time.sleep(5)

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

    collection = 'PanSTARRS1'

    # create 2d index: [placeholder]
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
    pool = ProcessPoolExecutor(10)

    # for ff in files[::-1]:
    for ff in sorted(files):
        pool.submit(process_file, _file=ff, _collection=collection, _batch_size=batch_size,
                    verbose=True, _dry_run=dry_run)
        # process_file(_file=ff, _collection=collection, _batch_size=batch_size,
        #              verbose=True, _dry_run=dry_run)

    # wait for everything to finish
    pool.shutdown(wait=True)

    print('All done')
