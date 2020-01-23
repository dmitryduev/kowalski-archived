import tables
import os
import glob
# from pprint import pp
import time
# from astropy.coordinates import Angle
import h5py
import numpy as np
import pandas as pd
import pymongo
import json
import argparse
import traceback
import datetime
import pytz
from numba import jit
import typing
# from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
from tqdm import tqdm


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


# @jit(forceobj=True)
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
    assert 0.0 <= x < 360.00000000000001, 'Bad RA value in degrees'
    # ac = Angle(x, unit='degree')
    # hms = str(ac.to_string(unit='hour', sep=':', pad=True))
    # print(str(hms))
    _h = np.floor(x * 12.0 / 180.)
    _m = np.floor((x * 12.0 / 180. - _h) * 60.0)
    _s = ((x * 12.0 / 180. - _h) * 60.0 - _m) * 60.0
    hms = '{:02.0f}:{:02.0f}:{:07.4f}'.format(_h, _m, _s)
    # print(hms)
    return hms


# @jit(forceobj=True)
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


@jit
def ccd_quad_2_rc(ccd: int, quad: int) -> int:
    # assert ccd in range(1, 17)
    # assert quad in range(1, 5)
    b = (ccd - 1) * 4
    rc = b + quad - 1
    return rc


# cone search radius in arcsec:
cone_search_radius = 2
# convert arcsec to rad:
cone_search_radius *= np.pi / 180.0 / 3600.


def xmatch(_db, ra, dec):
    """
        Cross-match by position
    """

    xmatches = dict()

    try:
        ra_geojson = float(ra)
        # geojson-friendly ra:
        ra_geojson -= 180.0
        dec_geojson = float(dec)

        ''' catalogs '''
        for catalog in config['xmatch']['catalogs']:
            catalog_filter = config['xmatch']['catalogs'][catalog]['filter']
            catalog_projection = config['xmatch']['catalogs'][catalog]['projection']

            object_position_query = dict()
            object_position_query['coordinates.radec_geojson'] = {
                '$geoWithin': {'$centerSphere': [[ra_geojson, dec_geojson], cone_search_radius]}}
            s = _db[catalog].find({**object_position_query, **catalog_filter},
                                  {**catalog_projection})
            xmatches[catalog] = list(s)

    except Exception as e:
        print(str(e))

    return xmatches


filters = {'zg': 1, 'zr': 2, 'zi': 3}


def process_file(fcvd):
    _file, _collection, verbose, _dry_run = fcvd

    # connect to MongoDB:
    if verbose:
        print('Connecting to DB')
    _client, _db = connect_to_db()
    if verbose:
        print('Successfully connected')

    if verbose:
        print(f'processing {_file}')

    try:
        with h5py.File(_file, 'r') as f:
            features = f['stats'][...]

        column_names = ['_id', 'ra', 'dec', 'period', 'significance', 'pdot',
                        'n', 'median', 'wmean', 'chi2red', 'roms', 'wstd',
                        'norm_peak_to_peak_amp', 'norm_excess_var', 'median_abs_dev', 'iqr',
                        'f60', 'f70', 'f80', 'f90', 'skew', 'smallkurt', 'inv_vonneumannratio',
                        'welch_i', 'stetson_j', 'stetson_k', 'ad', 'sw',
                        'f1_power', 'f1_bic', 'f1_a', 'f1_b', 'f1_amp', 'f1_phi0',
                        'f1_relamp1', 'f1_relphi1', 'f1_relamp2', 'f1_relphi2',
                        'f1_relamp3', 'f1_relphi3', 'f1_relamp4', 'f1_relphi5']

        df = pd.DataFrame(features, columns=column_names)
        df['_id'] = df['_id'].apply(lambda x: int(x))

        docs = df.to_dict(orient='records')

        for doc in docs:
            # Cross-match:
            xmatches = xmatch(_db, doc['ra'], doc['dec'])
            doc['cross_matches'] = xmatches

            # GeoJSON for 2D indexing
            doc['coordinates'] = {}
            _ra = doc['ra']
            _dec = doc['dec']
            _radec = [_ra, _dec]
            _radec_str = [deg2hms(_ra), deg2dms(_dec)]
            doc['coordinates']['radec_str'] = _radec_str
            # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
            _radec_geojson = [_ra - 180.0, _dec]
            doc['coordinates']['radec_geojson'] = {'type': 'Point',
                                                   'coordinates': _radec_geojson}
            # radians and degrees:
            # doc['coordinates']['radec_rad'] = [_ra * np.pi / 180.0, _dec * np.pi / 180.0]
            # doc['coordinates']['radec_deg'] = [_ra, _dec]

        if verbose:
            print(f'inserting {_file}')
        if not _dry_run:
            insert_multiple_db_entries(_db, _collection=_collection, _db_entries=docs, _verbose=verbose)

    except Exception as e:
        traceback.print_exc()
        print(e)

    # disconnect from db:
    try:
        _client.close()
        if verbose:
            if verbose:
                print('Successfully disconnected from db')
    finally:
        pass


if __name__ == '__main__':
    ''' Create command line argument parser '''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description='')

    parser.add_argument('--dryrun', action='store_true', help='dry run?')
    parser.add_argument('--verbose', action='store_true', help='verbose?')

    args = parser.parse_args()

    dry_run = args.dryrun
    verbose = args.verbose

    # connect to MongoDB:
    print('Connecting to DB')
    client, db = connect_to_db()
    print('Successfully connected')

    t_tag = '20191101'

    collection = f'ZTF_source_features_{t_tag}'

    # create indices:
    print('Creating indices')
    if not dry_run:
        db[collection].create_index([('coordinates.radec_geojson', '2dsphere'),
                                     ('_id', pymongo.ASCENDING)], background=True)

    _location = f'/_tmp/ztf_variablity_10_fields/'
    files = glob.glob(os.path.join(_location, '*.h5'))

    input_list = [(f, collection, verbose, dry_run) for f in sorted(files) if os.stat(f).st_size != 0]

    print(f'# files to process: {len(input_list)}')

    # process_file(input_list[0])
    with mp.Pool(processes=40) as p:
        results = list(tqdm(p.imap(process_file, input_list), total=len(input_list)))

    print('All done')
