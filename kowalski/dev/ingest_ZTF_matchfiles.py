import tables
import os
import glob
# from pprint import pp
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
# from concurrent.futures import ThreadPoolExecutor
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


@jit
def ccd_quad_2_rc(ccd: int, quad: int) -> int:
    # assert ccd in range(1, 17)
    # assert quad in range(1, 5)
    b = (ccd - 1) * 4
    rc = b + quad - 1
    return rc


def process_file(_file, _collections, _batch_size=2048, keep_all=False,
                 verbose=False, _dry_run=False):

    # connect to MongoDB:
    if verbose:
        print('Connecting to DB')
    _client, _db = connect_to_db()
    if verbose:
        print('Successfully connected')

    print(f'processing {_file}')

    try:
        with tables.open_file(_file) as f:
            # print(f.root['/matches'].attrs)
            group = f.root.matches
            # print(f.root.matches.exposures._v_attrs)
            # print(f.root.matches.sources._v_attrs)
            # print(f.root.matches.sourcedata._v_attrs)

            ff_basename = os.path.basename(_file)

            # base id:
            filters = {'zg': 1, 'zr': 2, 'zi': 3}
            _, field, filt, ccd, quad, _ = ff_basename.split('_')
            field = int(field)
            filt = filters[filt]
            ccd = int(ccd[1:])
            quad = int(quad[1:])

            rc = ccd_quad_2_rc(ccd=ccd, quad=quad)
            baseid = int(1e13 + field * 1e9 + rc * 1e7 + filt * 1e6)
            # print(f'{ff}: {field} {filt} {ccd} {quad}')
            print(f'{ff}: baseid {baseid}')

            # tic = time.time()
            exposures = pd.DataFrame.from_records(group.exposures[:])
            # exposures_colnames = exposures.columns.values
            # print(exposures_colnames)

            # prepare docs to ingest into db:
            docs_exposures = []
            for index, row in exposures.iterrows():
                doc = row.to_dict()
                doc['matchfile'] = ff_basename
                doc['field'] = field
                doc['ccd'] = ccd
                doc['quad'] = quad
                doc['rc'] = rc
                # pprint(doc)
                docs_exposures.append(doc)

            # ingest exposures in one go:
            if not _dry_run:
                print(f'ingesting exposures for {_file}')
                insert_multiple_db_entries(_db, _collection=_collections['exposures'], _db_entries=docs_exposures)
                print(f'done ingesting exposures for {_file}')

            docs_sources = []
            batch_num = 1
            # fixme: no transients for now!
            # for source_type in ('source', 'transient'):
            for source_type in ('source',):

                sources_colnames = group[f'{source_type}s'].colnames
                sources = np.array(group[f'{source_type}s'].read())
                # sources = group[f'{source_type}s'].read()

                # sourcedata = pd.DataFrame.from_records(group[f'{source_type}data'][:])
                # sourcedata_colnames = sourcedata.columns.values
                sourcedata_colnames = group[f'{source_type}data'].colnames
                # sourcedata = np.array(group[f'{source_type}data'].read())

                for source in sources:
                    doc = dict(zip(sources_colnames, source))
                    # convert types for pymongo:
                    for k, v in doc.items():
                        # types.add(type(v))
                        if np.issubdtype(type(v), np.integer):
                            doc[k] = int(doc[k])
                        if np.issubdtype(type(v), np.inexact):
                            doc[k] = float(doc[k])
                        # convert numpy arrays into lists
                        if type(v) == np.ndarray:
                            doc[k] = doc[k].tolist()

                    # generate unique _id:
                    doc['_id'] = baseid + doc['matchid']

                    doc['iqr'] = doc['bestpercentiles'][8] - doc['bestpercentiles'][3]

                    doc['matchfile'] = ff_basename
                    doc['field'] = field
                    doc['ccd'] = ccd
                    doc['quad'] = quad
                    doc['rc'] = rc

                    # doc['source_type'] = source_type

                    # GeoJSON for 2D indexing
                    doc['coordinates'] = {}
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

                    # grab data
                    sourcedata = np.array(group[f'{source_type}data'].read_where(f'matchid == {doc["matchid"]}'))
                    # print(sourcedata)
                    doc_data = [dict(zip(sourcedata_colnames, sd)) for sd in sourcedata]

                    doc['data'] = doc_data
                    # print(doc['data'])

                    # convert types for pymongo:
                    for dd in doc['data']:
                        for k, v in dd.items():
                            # types.add(type(v))
                            if np.issubdtype(type(v), np.integer):
                                dd[k] = int(dd[k])
                            if np.issubdtype(type(v), np.inexact):
                                dd[k] = float(dd[k])
                            # convert numpy arrays into lists
                            if type(v) == np.ndarray:
                                dd[k] = dd[k].tolist()

                    if not keep_all:
                        # fixme: do not store all fields to save space
                        sources_fields_to_keep = ('_id', 'coordinates', 'matchfile', 'iqr', 'data') + \
                                                 ('refmag', 'refmagerr')
                        doc_keys = list(doc.keys())
                        for kk in doc_keys:
                            if kk not in sources_fields_to_keep:
                                doc.pop(kk)

                        # fixme: do not store all fields to save space
                        if len(doc_data) > 0:
                            sourcedata_fields_to_keep = ('expid', 'hjd',
                                                         'mag', 'magerr',
                                                         'psfmag', 'psfmagerr',
                                                         'mjd', 'programid')
                            doc_keys = list(doc_data[0].keys())
                            for ddi, ddp in enumerate(doc['data']):
                                for kk in doc_keys:
                                    if kk not in sourcedata_fields_to_keep:
                                        doc['data'][ddi].pop(kk)

                    # pprint(doc)
                    docs_sources.append(doc)

                    # ingest in batches
                    if len(docs_sources) % _batch_size == 0:
                        print(f'inserting batch #{batch_num} for {_file}')
                        if not _dry_run:
                            insert_multiple_db_entries(_db, _collection=_collections['sources'],
                                                       _db_entries=docs_sources, _verbose=True)
                        # flush:
                        docs_sources = []
                        batch_num += 1

            # ingest remaining
            # stuff left from the last file?
            if len(docs_sources) > 0:
                print(f'inserting last batch #{batch_num} for {_file}')
                if not _dry_run:
                    insert_multiple_db_entries(_db, _collection=_collections['sources'],
                                               _db_entries=docs_sources)

    except Exception as e:
        traceback.print_exc()
        print(e)

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

    parser.add_argument('--keepall', action='store_true', help='keep all fields from the matchfiles?')
    parser.add_argument('--dryrun', action='store_true', help='dry run?')

    args = parser.parse_args()

    dry_run = args.dryrun
    keep_all = args.keepall

    # connect to MongoDB:
    print('Connecting to DB')
    client, db = connect_to_db()
    print('Successfully connected')

    collections = {'exposures': 'ZTF_matchfiles_exposures_20181219',
                   'sources': 'ZTF_matchfiles_sources_20181219'}

    # create 2d index:
    print('Creating 2d index')
    if not dry_run:
        db[collections['exposures']].create_index([('expid', pymongo.ASCENDING)], background=True)
        db[collections['sources']].create_index([('coordinates.radec_geojson', '2dsphere'),
                                                 ('_id', pymongo.ASCENDING)], background=True)
        # db[collections['sources']].create_index([('matchid', pymongo.ASCENDING)], background=True)
        # db[collections['sources']].create_index([('x', pymongo.ASCENDING)], background=True)
        # db[collections['sources']].create_index([('y', pymongo.ASCENDING)], background=True)
        # db[collections['sources']].create_index([('z', pymongo.ASCENDING)], background=True)
        db[collections['sources']].create_index([('data.programid', pymongo.ASCENDING)], background=True)

    # number of records to insert
    batch_size = 2048
    # batch_size = 1

    # fixme:
    # test
    # _location = '/_tmp/ztf_matchfiles_20181219/'
    # files = glob.glob(os.path.join(_location, 'ztf_*.pytable'))

    # production
    _location = '/matchfiles/'
    files = glob.glob(os.path.join(_location, '*', '*', 'ztf_*.pytable'))
    # files = glob.glob(os.path.join(_location, '*', '*', 'ztf_*.pytable'))[:100]
    # files = ['/matchfiles/rc63/fr000301-000350/ztf_000303_zr_c16_q4_match.pytable',
    #          '/matchfiles/rc63/fr000301-000350/ztf_000303_zg_c16_q4_match.pytable']
    # print(files)
    # file_sizes = [os.path.getsize(ff) for ff in files]
    # total_file_size = np.sum(file_sizes) / 1e6
    # print(f'Total file size: {total_file_size} MB')

    print(f'# files to process: {len(files)}')

    # init threaded operations
    # pool = ThreadPoolExecutor(2)
    # pool = ProcessPoolExecutor(1)
    pool = ProcessPoolExecutor(10)

    # for ff in files[::-1]:
    for ff in sorted(files):
        pool.submit(process_file, _file=ff, _collections=collections, _batch_size=batch_size,
                    keep_all=keep_all, verbose=True, _dry_run=dry_run)

    # wait for everything to finish
    pool.shutdown(wait=True)

    print('All done')
