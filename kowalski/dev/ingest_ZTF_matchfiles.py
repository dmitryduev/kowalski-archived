import tables
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


def process_file(_file, _collections, _batch_size=2048, verbose=False, _dry_run=False):

    # connect to MongoDB:
    if verbose:
        print('Connecting to DB')
    _client, _db = connect_to_db()
    if verbose:
        print('Successfully connected')

    print(f'processing {_file}')

    try:
        with tables.open_file(ff) as f:
            # print(f.root['/matches'].attrs)
            group = f.root.matches
            # print(f.root.matches.exposures._v_attrs)
            # print(f.root.matches.sources._v_attrs)
            # print(f.root.matches.sourcedata._v_attrs)

            # tic = time.time()
            exposures = pd.DataFrame.from_records(group.exposures[:])
            # exposures_colnames = exposures.columns.values
            # print(exposures_colnames)

            # prepare docs to ingest into db:
            docs_exposures = []
            for index, row in exposures.iterrows():
                doc = row.to_dict()
                doc['matchfile'] = os.path.basename(ff)
                # pprint(doc)
                docs_exposures.append(doc)

            # ingest exposures in one go:
            print(f'ingesting exposures for {_file}')
            insert_multiple_db_entries(_db, _collection=_collections['exposures'], _db_entries=docs_exposures)
            print(f'done ingesting exposures for {_file}')

            docs_sources = []
            batch_num = 1
            for source_type in ('source', 'transient'):

                sources_colnames = group[f'{source_type}s'].colnames
                sources = np.array(group[f'{source_type}s'].read())
                # sources = group[f'{source_type}s'].read()

                # sourcedata = pd.DataFrame.from_records(group[f'{source_type}data'][:])
                # sourcedata_colnames = sourcedata.columns.values
                sourcedata_colnames = group[f'{source_type}data'].colnames
                # sourcedata = np.array(group[f'{source_type}data'].read())

                # let mongodb generate _id's for us in case we need to go switch to bucketing LC data in the future

                for source in sources:
                    doc = dict(zip(sources_colnames, source))
                    # convert numpy arrays into lists to ingest into mongodb?
                    for k, v in doc.items():
                        if type(v) == np.ndarray:
                            doc[k] = doc[k].tolist()

                    doc['source_type'] = source_type

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
                    doc['data'] = [dict(zip(sourcedata_colnames, sd)) for sd in sourcedata]
                    print(doc['data'])

                    # pprint(doc)
                    docs_sources.append(doc)

                    # ingest in batches
                    if len(docs_sources) % _batch_size == 0:
                        print(f'inserting batch #{batch_num}')
                        if not _dry_run:
                            insert_multiple_db_entries(_db, _collection=_collections['sources'],
                                                       _db_entries=docs_sources)
                        # flush:
                        docs_sources = []
                        batch_num += 1

            # ingest remaining
            # stuff left from the last file?
            if len(docs_sources) > 0:
                print(f'inserting last batch #{batch_num}')
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

    parser.add_argument('--dryrun', action='store_true', help='enforce execution')

    args = parser.parse_args()

    dry_run = args.dryrun

    # connect to MongoDB:
    print('Connecting to DB')
    client, db = connect_to_db()
    print('Successfully connected')

    collections = {'exposures': 'ZTF_matchfiles_exposures_20181219',
                   'sources': 'ZTF_matchfiles_sources_20181219'}

    # create 2d index:
    print('Creating 2d index')
    if not dry_run:
        db[collections['sources']].create_index([('coordinates.radec_geojson', '2dsphere'),
                                     ('_id', pymongo.ASCENDING)], background=True)

    # number of records to insert
    batch_size = 2048

    _location = '/_tmp/ztf_matchfiles_20181219/'

    files = glob.glob(os.path.join(_location, 'ztf_*.pytable'))

    print(f'# files to process: {len(files)}')

    # init threaded operations
    # pool = ThreadPoolExecutor(2)
    pool = ProcessPoolExecutor(1)

    # for ff in files[::-1]:
    for ff in sorted(files):
        pool.submit(process_file, _file=ff, _collections=collections, _batch_size=batch_size,
                    verbose=True, _dry_run=dry_run)
        # process_file(_file=ff, _collections=collections, _batch_size=batch_size,
        #              verbose=True, _dry_run=dry_run)

    # wait for everything to finish
    pool.shutdown(wait=True)

    print('All done')
