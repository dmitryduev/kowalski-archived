import os
import glob
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
import time
import ast


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


def process_file(_file, _collection, _batch_size=2048, verbose=False, _dry_run=False):

    # connect to MongoDB:
    if verbose:
        print('Connecting to DB')
    _client, _db = connect_to_db()
    if verbose:
        print('Successfully connected')

    print(f'processing {_file}')

    # column names:
    column_names = [['designation', str],
                    ['ra', float],
                    ['dec', float],
                    ['sigra', float],
                    ['sigdec', float],
                    ['sigradec', float],
                    ['glon', float],
                    ['glat', float],
                    ['elon', float],
                    ['elat', float],
                    ['wx', float],
                    ['wy', float],
                    ['cntr', float],
                    ['source_id', str],
                    ['coadd_id', str],
                    ['src', int],
                    ['w1mpro', float],
                    ['w1sigmpro', float],
                    ['w1snr', float],
                    ['w1rchi2', float],
                    ['w2mpro', float],
                    ['w2sigmpro', float],
                    ['w2snr', float],
                    ['w2rchi2', float],
                    ['w3mpro', float],
                    ['w3sigmpro', float],
                    ['w3snr', float],
                    ['w3rchi2', float],
                    ['w4mpro', float],
                    ['w4sigmpro', float],
                    ['w4snr', float],
                    ['w4rchi2', float],
                    ['rchi2', float],
                    ['nb', int],
                    ['na', int],
                    ['w1sat', float],
                    ['w2sat', float],
                    ['w3sat', float],
                    ['w4sat', float],
                    ['satnum', str],
                    ['ra_pm', float],
                    ['dec_pm', float],
                    ['sigra_pm', float],
                    ['sigdec_pm', float],
                    ['sigradec_pm', float],
                    ['pmra', int],
                    ['sigpmra', int],
                    ['pmdec', int],
                    ['sigpmdec', int],
                    ['w1rchi2_pm', float],
                    ['w2rchi2_pm', float],
                    ['w3rchi2_pm', float],
                    ['w4rchi2_pm', float],
                    ['rchi2_pm', float],
                    ['pmcode', str],
                    ['cc_flags', str],
                    ['rel', str],
                    ['ext_flg', int],
                    ['var_flg', str],
                    ['ph_qual', str],
                    ['det_bit', int],
                    ['moon_lev', str],
                    ['w1nm', int],
                    ['w1m', int],
                    ['w2nm', int],
                    ['w2m', int],
                    ['w3nm', int],
                    ['w3m', int],
                    ['w4nm', int],
                    ['w4m', int],
                    ['w1cov', float],
                    ['w2cov', float],
                    ['w3cov', float],
                    ['w4cov', float],
                    ['w1cc_map', int],
                    ['w1cc_map_str', str],
                    ['w2cc_map', int],
                    ['w2cc_map_str', str],
                    ['w3cc_map', int],
                    ['w3cc_map_str', str],
                    ['w4cc_map', int],
                    ['w4cc_map_str', str],
                    ['best_use_cntr', int],
                    ['ngrp', int],
                    ['w1flux', float],
                    ['w1sigflux', float],
                    ['w1sky', float],
                    ['w1sigsk', float],
                    ['w1conf', float],
                    ['w2flux', float],
                    ['w2sigflux', float],
                    ['w2sky', float],
                    ['w2sigsk', float],
                    ['w2conf', float],
                    ['w3flux', float],
                    ['w3sigflux', float],
                    ['w3sky', float],
                    ['w3sigsk', float],
                    ['w3conf', float],
                    ['w4flux', float],
                    ['w4sigflux', float],
                    ['w4sky', float],
                    ['w4sigsk', float],
                    ['w4conf', float],
                    ['w1mag', float],
                    ['w1sigm', float],
                    ['w1flg', int],
                    ['w1mcor', float],
                    ['w2mag', float],
                    ['w2sigm', float],
                    ['w2flg', int],
                    ['w2mcor', float],
                    ['w3mag', float],
                    ['w3sigm', float],
                    ['w3flg', int],
                    ['w3mcor', float],
                    ['w4mag', float],
                    ['w4sigm', float],
                    ['w4flg', int],
                    ['w4mcor', float],
                    ['w1mag_1', float],
                    ['w1sigm_1', float],
                    ['w1flg_1', int],
                    ['w2mag_1', float],
                    ['w2sigm_1', float],
                    ['w2flg_1', int],
                    ['w3mag_1', float],
                    ['w3sigm_1', float],
                    ['w3flg_1', int],
                    ['w4mag_1', float],
                    ['w4sigm_1', float],
                    ['w4flg_1', int],
                    ['w1mag_2', float],
                    ['w1sigm_2', float],
                    ['w1flg_2', int],
                    ['w2mag_2', float],
                    ['w2sigm_2', float],
                    ['w2flg_2', int],
                    ['w3mag_2', float],
                    ['w3sigm_2', float],
                    ['w3flg_2', int],
                    ['w4mag_2', float],
                    ['w4sigm_2', float],
                    ['w4flg_2', int],
                    ['w1mag_3', float],
                    ['w1sigm_3', float],
                    ['w1flg_3', int],
                    ['w2mag_3', float],
                    ['w2sigm_3', float],
                    ['w2flg_3', int],
                    ['w3mag_3', float],
                    ['w3sigm_3', float],
                    ['w3flg_3', int],
                    ['w4mag_3', float],
                    ['w4sigm_3', float],
                    ['w4flg_3', int],
                    ['w1mag_4', float],
                    ['w1sigm_4', float],
                    ['w1flg_4', int],
                    ['w2mag_4', float],
                    ['w2sigm_4', float],
                    ['w2flg_4', int],
                    ['w3mag_4', float],
                    ['w3sigm_4', float],
                    ['w3flg_4', int],
                    ['w4mag_4', float],
                    ['w4sigm_4', float],
                    ['w4flg_4', int],
                    ['w1mag_5', float],
                    ['w1sigm_5', float],
                    ['w1flg_5', int],
                    ['w2mag_5', float],
                    ['w2sigm_5', float],
                    ['w2flg_5', int],
                    ['w3mag_5', float],
                    ['w3sigm_5', float],
                    ['w3flg_5', int],
                    ['w4mag_5', float],
                    ['w4sigm_5', float],
                    ['w4flg_5', int],
                    ['w1mag_6', float],
                    ['w1sigm_6', float],
                    ['w1flg_6', int],
                    ['w2mag_6', float],
                    ['w2sigm_6', float],
                    ['w2flg_6', int],
                    ['w3mag_6', float],
                    ['w3sigm_6', float],
                    ['w3flg_6', int],
                    ['w4mag_6', float],
                    ['w4sigm_6', float],
                    ['w4flg_6', int],
                    ['w1mag_7', float],
                    ['w1sigm_7', float],
                    ['w1flg_7', int],
                    ['w2mag_7', float],
                    ['w2sigm_7', float],
                    ['w2flg_7', int],
                    ['w3mag_7', float],
                    ['w3sigm_7', float],
                    ['w3flg_7', int],
                    ['w4mag_7', float],
                    ['w4sigm_7', float],
                    ['w4flg_7', int],
                    ['w1mag_8', float],
                    ['w1sigm_8', float],
                    ['w1flg_8', int],
                    ['w2mag_8', float],
                    ['w2sigm_8', float],
                    ['w2flg_8', int],
                    ['w3mag_8', float],
                    ['w3sigm_8', float],
                    ['w3flg_8', int],
                    ['w4mag_8', float],
                    ['w4sigm_8', float],
                    ['w4flg_8', int],
                    ['w1magp', float],
                    ['w1sigp1', float],
                    ['w1sigp2', float],
                    ['w1k', float],
                    ['w1ndf', int],
                    ['w1mlq', float],
                    ['w1mjdmin', float],
                    ['w1mjdmax', float],
                    ['w1mjdmean', float],
                    ['w2magp', float],
                    ['w2sigp1', float],
                    ['w2sigp2', float],
                    ['w2k', float],
                    ['w2ndf', int],
                    ['w2mlq', float],
                    ['w2mjdmin', float],
                    ['w2mjdmax', float],
                    ['w2mjdmean', float],
                    ['w3magp', float],
                    ['w3sigp1', float],
                    ['w3sigp2', float],
                    ['w3k', float],
                    ['w3ndf', int],
                    ['w3mlq', float],
                    ['w3mjdmin', float],
                    ['w3mjdmax', float],
                    ['w3mjdmean', float],
                    ['w4magp', float],
                    ['w4sigp1', float],
                    ['w4sigp2', float],
                    ['w4k', float],
                    ['w4ndf', int],
                    ['w4mlq', float],
                    ['w4mjdmin', float],
                    ['w4mjdmax', float],
                    ['w4mjdmean', float],
                    ['rho12', int],
                    ['rho23', int],
                    ['rho34', int],
                    ['q12', int],
                    ['q23', int],
                    ['q34', int],
                    ['xscprox', float],
                    ['w1rsemi', float],
                    ['w1ba', float],
                    ['w1pa', float],
                    ['w1gmag', float],
                    ['w1gerr', float],
                    ['w1gflg', int],
                    ['w2rsemi', float],
                    ['w2ba', float],
                    ['w2pa', float],
                    ['w2gmag', float],
                    ['w2gerr', float],
                    ['w2gflg', int],
                    ['w3rsemi', float],
                    ['w3ba', float],
                    ['w3pa', float],
                    ['w3gmag', float],
                    ['w3gerr', float],
                    ['w3gflg', int],
                    ['w4rsemi', float],
                    ['w4ba', float],
                    ['w4pa', float],
                    ['w4gmag', float],
                    ['w4gerr', float],
                    ['w4gflg', int],
                    ['tmass_key', int],
                    ['r_2mass', float],
                    ['pa_2mass', float],
                    ['n_2mass', int],
                    ['j_m_2mass', float],
                    ['j_msig_2mass', float],
                    ['h_m_2mass', float],
                    ['h_msig_2mass', float],
                    ['k_m_2mass', float],
                    ['k_msig_2mass', float],
                    ['x', float],
                    ['y', float],
                    ['z', float],
                    ['spt_ind', int],
                    ['htm20', int],
                    ['eol', bool]]
    column_names = np.array(column_names)

    for ii, dff in enumerate(pd.read_csv(_file, chunksize=_batch_size, sep='|',
                                         header=None, names=column_names[:, 0])):

        print(f'Processing batch # {ii+1} from {_file}')

        dff['_id'] = dff['cntr']

        drop_columns = ['x', 'y', 'z', 'spt_ind', 'htm20', 'eol']
        # print(dff)
        dff.drop(columns=drop_columns, inplace=True)

        batch = dff.to_dict(orient='records')

        bad_doc_ind = []

        for ie, doc in enumerate(batch):
            try:
                # fix types:
                for col_name, col_type in column_names[:-6]:
                    try:
                        if doc[col_name] == '' or doc[col_name] == np.nan:
                            # doc[col_name] = None
                            doc.pop(col_name, None)
                        else:
                            doc[col_name] = col_type(doc[col_name])
                    except Exception as e:
                        traceback.print_exc()
                        print(e)

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
        if not _dry_run:
            insert_multiple_db_entries(_db, _collection=_collection, _db_entries=batch)

    # disconnect from db:
    try:
        _client.close()
    finally:
        if verbose:
            print(f'Done processing {_file}')
            print('Successfully disconnected from db')


if __name__ == '__main__':
    ''' Create command line argument parser '''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description='')

    parser.add_argument('--dryrun', action='store_true', help='do not ingest, only parse')

    args = parser.parse_args()

    dry_run = args.dryrun

    # connect to MongoDB:
    print('Connecting to DB')
    client, db = connect_to_db()
    print('Successfully connected')

    collection = 'AllWISE'

    # create 2d index:
    print('Creating indices')
    db[collection].create_index([('source_id', pymongo.ASCENDING)], background=True)
    db[collection].create_index([('designation', pymongo.ASCENDING)], background=True)
    db[collection].create_index([('coordinates.radec_geojson', '2dsphere'),
                                 ('_id', pymongo.ASCENDING)], background=True)

    # number of records to insert
    batch_size = 2048

    _location = '/_tmp/allwise/irsa.ipac.caltech.edu/data/download/wise-allwise/'

    files = glob.glob(os.path.join(_location, 'wise-allwise-cat-part*'))

    print(f'# files to process: {len(files)}')

    # init threaded operations
    # pool = ThreadPoolExecutor(2)
    pool = ProcessPoolExecutor(5)

    # for ff in files[::-1]:
    for ff in sorted(files):
        # pool.submit(process_file, _file=ff, _collection=collection, _batch_size=batch_size,
        #             verbose=True, _dry_run=dry_run)
        process_file(_file=ff, _collection=collection, _batch_size=batch_size,
                     verbose=True, _dry_run=dry_run)

    # wait for everything to finish
    pool.shutdown(wait=True)

    print('All done')
