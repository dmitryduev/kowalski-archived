import os
import glob
import time
import numpy as np
import pymongo
import json
import argparse
import traceback
import datetime
import pytz
from numba import jit
import fastavro as avro
from tensorflow.keras.models import load_model
import gzip
import io
from astropy.io import fits
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


def find_files(root_dir):
    for dir_name, subdir_list, file_list in os.walk(root_dir, followlinks=True):
        for f_name in file_list:
            if f_name.endswith('.avro'):
                yield os.path.join(dir_name, f_name)


def make_triplet(alert, to_tpu: bool = False):
    """
        Feed in alert packet
    """
    cutout_dict = dict()

    for cutout in ('science', 'template', 'difference'):
        # cutout_data = loads(dumps([alert[f'cutout{cutout.capitalize()}']['stampData']]))[0]
        cutout_data = alert[f'cutout{cutout.capitalize()}']['stampData']

        # unzip
        with gzip.open(io.BytesIO(cutout_data), 'rb') as f:
            with fits.open(io.BytesIO(f.read())) as hdu:
                data = hdu[0].data
                # replace nans with zeros
                cutout_dict[cutout] = np.nan_to_num(data)
                # L2-normalize
                cutout_dict[cutout] /= np.linalg.norm(cutout_dict[cutout])

        # pad to 63x63 if smaller
        shape = cutout_dict[cutout].shape
        if shape != (63, 63):
            # print(f'Shape of {candid}/{cutout}: {shape}, padding to (63, 63)')
            cutout_dict[cutout] = np.pad(cutout_dict[cutout], [(0, 63 - shape[0]), (0, 63 - shape[1])],
                                         mode='constant', constant_values=1e-9)

    triplet = np.zeros((63, 63, 3))
    triplet[:, :, 0] = cutout_dict['science']
    triplet[:, :, 1] = cutout_dict['template']
    triplet[:, :, 2] = cutout_dict['difference']

    if to_tpu:
        # Edge TPUs require additional processing
        triplet = np.rint(triplet * 128 + 128).astype(np.uint8).flatten()

    return triplet


def alert_filter__ml(alert, ml_models: dict = None):
    """Filter to apply to each alert.
    """

    scores = dict()

    try:
        ''' braai '''
        triplet = make_triplet(alert)
        triplets = np.expand_dims(triplet, axis=0)
        braai = ml_models['braai']['model'].predict(x=triplets)[0]
        # braai = 1.0
        scores['braai'] = float(braai)
        scores['braai_version'] = ml_models['braai']['version']
    except Exception as e:
        print(str(e))

    return scores


# cone search radius:
cone_search_radius = float(config['xmatch']['cone_search_radius'])
# convert to rad:
if config['xmatch']['cone_search_unit'] == 'arcsec':
    cone_search_radius *= np.pi / 180.0 / 3600.
elif config['xmatch']['cone_search_unit'] == 'arcmin':
    cone_search_radius *= np.pi / 180.0 / 60.
elif config['xmatch']['cone_search_unit'] == 'deg':
    cone_search_radius *= np.pi / 180.0
elif config['xmatch']['cone_search_unit'] == 'rad':
    cone_search_radius *= 1
else:
    raise Exception('Unknown cone search unit. Must be in [deg, rad, arcsec, arcmin]')


def alert_filter__xmatch(db, alert):
    """Filter to apply to each alert.
    """

    xmatches = dict()

    try:
        ra_geojson = float(alert['candidate']['ra'])
        # geojson-friendly ra:
        ra_geojson -= 180.0
        dec_geojson = float(alert['candidate']['dec'])

        ''' catalogs '''
        for catalog in config['xmatch']['catalogs']:
            catalog_filter = config['xmatch']['catalogs'][catalog]['filter']
            catalog_projection = config['xmatch']['catalogs'][catalog]['projection']

            object_position_query = dict()
            object_position_query['coordinates.radec_geojson'] = {
                '$geoWithin': {'$centerSphere': [[ra_geojson, dec_geojson], cone_search_radius]}}
            s = db[catalog].find({**object_position_query, **catalog_filter},
                                 {**catalog_projection})
            xmatches[catalog] = list(s)

    except Exception as e:
        print(str(e))

    return xmatches


def process_file(_date, _path_alerts, _collection, _batch_size=2048, verbose=False):

    # connect to MongoDB:
    if verbose:
        print(f'{_date} :: Connecting to DB')
    _client, _db = connect_to_db()
    if verbose:
        print(f'{_date} :: Successfully connected')

    # ML models:
    print(f'{_date} :: Loading ML models')
    ml_models = dict()
    for m in config['ml_models']:
        try:
            m_v = config["ml_models"][m]["version"]
            ml_models[m] = {'model': load_model(f'/app/models/{m}_{m_v}.h5'),
                            'version': m_v}
        except Exception as e:
            print(f'Error loading ML model {m}')
            traceback.print_exc()
            print(e)
            continue

    print(f'{_date} :: processing {_date}')
    documents = []
    batch_num = 1

    # location/YYYMMDD/ALERTS/candid.avro:
    # avros = glob.glob(os.path.join(_path_alerts, f'{_date}/*/*.avro'))

    # print(f'{_date} :: # files to process: {len(avros)}')

    ci = 1
    for cf in find_files(os.path.join(_path_alerts, _date)):
        # print(f'{_date} :: processing file #{ci+1} of {len(avros)}: {os.path.basename(cf)}')
        print(f'{_date} :: processing file #{ci}: {os.path.basename(cf)}')

        try:
            with open(cf, 'rb') as f:
                reader = avro.reader(f)
                for alert in reader:
                    try:
                        doc = dict(alert)

                        # candid+objectId is a unique combination:
                        doc['_id'] = f"{doc['candid']}_{doc['objectId']}"

                        # GeoJSON for 2D indexing
                        doc['coordinates'] = {}
                        # doc['coordinates']['epoch'] = doc['candidate']['jd']
                        _ra = doc['candidate']['ra']
                        _dec = doc['candidate']['dec']
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

                        # alert filters:

                        # ML models:
                        scores = alert_filter__ml(alert, ml_models=ml_models)
                        alert['classifications'] = scores

                        # cross-match with external catalogs:
                        # print(alert['candid'], alert['candidate']['programpi'])
                        # if record['candidate']['programpi'].strip() == 'TESS':
                        # tic = time.time()
                        xmatches = alert_filter__xmatch(_db, alert)
                        alert['cross_matches'] = xmatches

                        documents.append(doc)

                        # time.sleep(1)

                        # insert batch, then flush
                        if len(documents) % _batch_size == 0:
                            print(f'{_date} :: inserting batch #{batch_num}')
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
            continue

        ci += 1

    # stuff left from the last file?
    while len(documents) > 0:
        try:
            # In case mongo crashed and disconnected, docs will accumulate in documents
            # keep on trying to insert them until successful
            print(f'{_date} :: inserting batch #{batch_num}')
            insert_multiple_db_entries(_db, _collection=_collection, _db_entries=documents)
            # flush:
            documents = []

        except Exception as e:
            traceback.print_exc()
            print(e)
            print(f'{_date} :: Failed, waiting 5 seconds to retry')
            time.sleep(5)

    # disconnect from db:
    try:
        _client.close()
    finally:
        if verbose:
            print(f'{_date} :: Successfully disconnected from db')


if __name__ == '__main__':
    ''' Create command line argument parser '''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description='Ingest avro-packets with ZTF alerts')

    parser.add_argument('--obsdate', help='observing date')

    args = parser.parse_args()

    # connect to MongoDB:
    print('Connecting to DB')
    client, db = connect_to_db()
    print('Successfully connected')

    location = '/_tmp/ztf_alerts2'

    dates = os.listdir(location) if not args.obsdate else [args.obsdate]
    dates = [d for d in dates if os.path.isdir(os.path.join(location, d))]
    print(dates)

    # number of records to insert per loop iteration
    batch_size = 1024

    # collection name
    collection = 'ZTF_alerts2'

    # indexes
    db[collection].create_index([('coordinates.radec_geojson', '2dsphere'),
                                 ('candid', pymongo.DESCENDING)], background=True)
    db[collection].create_index([('coordinates.radec_geojson', '2dsphere'),
                                 ('objectId', pymongo.DESCENDING)], background=True)
    db[collection].create_index([('objectId', pymongo.ASCENDING)], background=True)
    db[collection].create_index([('candid', pymongo.ASCENDING)], background=True)
    db[collection].create_index([('candidate.pid', pymongo.ASCENDING)], background=True)
    db[collection].create_index([('objectId', pymongo.DESCENDING),
                                 ('candidate.pid', pymongo.ASCENDING)], background=True)
    db[collection].create_index([('candidate.pdiffimfilename', pymongo.ASCENDING)],
                                background=True)
    db[collection].create_index([('candidate.jd', pymongo.ASCENDING),
                                 ('candidate.programid', pymongo.ASCENDING),
                                 ('candidate.programpi', pymongo.ASCENDING)],
                                background=True)
    db[collection].create_index([('candidate.jd', pymongo.DESCENDING),
                                 ('classifications.braai', pymongo.DESCENDING),
                                 ('candid', pymongo.DESCENDING)],
                                background=True)
    # db[collection].create_index([('candidate.jd', 1),
    #                              ('candidate.field', 1),
    #                              ('candidate.rb', 1),
    #                              ('candidate.drb', 1),
    #                              ('classifications.braai', 1),
    #                              ('candidate.ndethist', 1),
    #                              ('candidate.magpsf', 1),
    #                              ('candidate.isdiffpos', 1),
    #                              ('objectId', 1)],
    #                             name='jd_field_rb_drb_braai_ndethhist_magpsf_isdiffpos',
    #                             background=True)

    # init threaded operations
    # pool = ThreadPoolExecutor(4)
    pool = ProcessPoolExecutor(min(len(dates), 3))

    for date in sorted(dates):
        pool.submit(process_file, _date=date, _path_alerts=location,
                    _collection=collection, _batch_size=batch_size, verbose=True)
        # process_file(_date=date, _path_alerts=location, _collection=collection, _batch_size=batch_size, verbose=True)

    # wait for everything to finish
    pool.shutdown(wait=True)

    print('All done')
