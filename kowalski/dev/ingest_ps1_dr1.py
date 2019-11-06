import os
import glob
import pandas as pd
import pymongo
import json
import argparse
import traceback
from concurrent.futures import ProcessPoolExecutor
from ingest_utils import connect_to_db, deg2dms, deg2hms, insert_multiple_db_entries


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

    print(f'processing {_file}')

    for ii, dff in enumerate(pd.read_csv(_file, chunksize=_batch_size)):

        print(f'{_file}: processing batch # {ii + 1}')

        dff.rename(index=str, columns={'objID': '_id'}, inplace=True)
        batch = dff.to_dict(orient='records')

        bad_doc_ind = []

        for ie, doc in enumerate(batch):
            try:
                doc['_id'] = doc['objID']
                doc.pop('objID', None)

                # convert types and pop empty fields
                for col_name, col_type in column_names:
                    try:
                        if doc[col_name] == 'NOT_AVAILABLE':
                            # continue
                            doc.pop(col_name, None)
                        elif doc[col_name] == 'false':
                            doc[col_name] = False
                        elif doc[col_name] == 'true':
                            doc[col_name] = True
                        elif len(doc[col_name]) == 0:
                            # doc[col_name] = None
                            doc.pop(col_name, None)
                        else:
                            doc[col_name] = col_type(doc[col_name])
                    except Exception as e:
                        traceback.print_exc()
                        print(e)

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
