import csv
import os
import glob
import time
from astropy.coordinates import Angle
import numpy as np
import pymongo
import inspect
import json
import argparse
# import timeout_decorator
import signal
import traceback
import datetime
import pytz
from numba import jit


def utc_now():
    return datetime.datetime.now(pytz.utc)


class TimeoutError(Exception):
    def __init__(self, value='Operation timed out'):
        self.value = value

    def __str__(self):
        return repr(self.value)


def timeout(seconds_before_timeout):
    """
        A decorator that raises a TimeoutError error if a function/method runs longer than seconds_before_timeout
    :param seconds_before_timeout:
    :return:
    """
    def decorate(f):
        def handler(signum, frame):
            raise TimeoutError()

        def new_f(*args, **kwargs):
            old = signal.signal(signal.SIGALRM, handler)
            old_time_left = signal.alarm(seconds_before_timeout)
            if 0 < old_time_left < seconds_before_timeout:  # never lengthen existing timer
                signal.alarm(old_time_left)
            start_time = time.time()
            try:
                result = f(*args, **kwargs)
            finally:
                if old_time_left > 0:  # deduct f's run time from the saved timer
                    old_time_left -= time.time() - start_time
                signal.signal(signal.SIGALRM, old)
                signal.alarm(old_time_left)
            return result
        new_f.__name__ = f.__name__
        return new_f
    return decorate


def get_config(_config_file='config.json'):
    """
        load config data in json format
    """
    try:
        ''' script absolute location '''
        abs_path = os.path.dirname(inspect.getfile(inspect.currentframe()))

        if _config_file[0] not in ('/', '~'):
            if os.path.isfile(os.path.join(abs_path, _config_file)):
                config_path = os.path.join(abs_path, _config_file)
            else:
                raise IOError('Failed to find config file')
        else:
            if os.path.isfile(_config_file):
                config_path = _config_file
            else:
                raise IOError('Failed to find config file')

        with open(config_path) as cjson:
            config_data = json.load(cjson)
            # config must not be empty:
            if len(config_data) > 0:
                return config_data
            else:
                raise Exception('Failed to load config file')

    except Exception as _e:
        print(_e)
        raise Exception('Failed to read in the config file')


def connect_to_db(_config):
    """ Connect to the mongodb database

    :return:
    """
    try:
        # there's only one instance of DB, it's too big to be replicated
        _client = pymongo.MongoClient(host=_config['database']['host'],
                                      port=_config['database']['port'])
        # grab main database:
        _db = _client[_config['database']['db']]
    except Exception as _e:
        raise ConnectionRefusedError
    try:
        # authenticate
        _db.authenticate(_config['database']['user'], _config['database']['pwd'])
    except Exception as _e:
        raise ConnectionRefusedError

    # catalogs:
    _catalogs = dict()
    for catalog in _config['catalogs']:
        if catalog == 'help':
            continue
        try:
            # get collection with Pan-STARRS catalog
            _coll_cat = _db[catalog]
            _catalogs[catalog] = _coll_cat
        except Exception as _e:
            raise NameError

    # ...

    try:
        # get collection with user access credentials
        _coll_usr = _db[_config['database']['collection_pwd']]
    except Exception as _e:
        raise NameError

    return _client, _db, _catalogs, _coll_usr


# @timeout_decorator.timeout(60, use_signals=False)
@timeout(seconds_before_timeout=60)
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
        print(_e)


@timeout(seconds_before_timeout=60)
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
        _db[_collection].insert_many(_db_entries)
    except Exception as _e:
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


if __name__ == '__main__':
    ''' Create command line argument parser '''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description='')

    parser.add_argument('config_file', metavar='config_file',
                        action='store', help='path to config file.', type=str)

    args = parser.parse_args()

    # parse config file:
    config = get_config(_config_file=args.config_file)

    # connect to MongoDB:
    print('Connecting to DB')
    client, db, catalogs, coll_usr = connect_to_db(config)
    print('Successfully connected')
    print(f'Catalogs: {catalogs.keys()}')

    _collection = 'Gaia_DR1'

    # _location = '/Users/dmitryduev/_caltech/python/broker/dev'
    _location = '/data/ztf/tmp/gaia_dr1'

    # number of records to insert
    batch_size = 8192
    batch_num = 1
    documents = []

    csvs = glob.glob(os.path.join(_location, 'Gaia*.csv'))

    # column names:
    column_names = ['solution_id', 'source_id', 'random_index', 'ref_epoch',
                    'ra', 'ra_error', 'dec', 'dec_error', 'parallax', 'parallax_error',
                    'pmra', 'pmra_error', 'pmdec', 'pmdec_error',
                    'ra_dec_corr', 'ra_parallax_corr', 'ra_pmra_corr', 'ra_pmdec_corr',
                    'dec_parallax_corr', 'dec_pmra_corr', 'dec_pmdec_corr', 'parallax_pmra_corr',
                    'parallax_pmdec_corr', 'pmra_pmdec_corr',
                    'astrometric_n_obs_al', 'astrometric_n_obs_ac', 'astrometric_n_good_obs_al',
                    'astrometric_n_good_obs_ac', 'astrometric_n_bad_obs_al', 'astrometric_n_bad_obs_ac',
                    'astrometric_delta_q', 'astrometric_excess_noise', 'astrometric_excess_noise_sig',
                    'astrometric_primary_flag', 'astrometric_relegation_factor', 'astrometric_weight_al',
                    'astrometric_weight_ac', 'astrometric_priors_used',
                    'matched_observations', 'duplicated_source',
                    'scan_direction_strength_k1', 'scan_direction_strength_k2',
                    'scan_direction_strength_k3', 'scan_direction_strength_k4',
                    'scan_direction_mean_k1', 'scan_direction_mean_k2',
                    'scan_direction_mean_k3', 'scan_direction_mean_k4',
                    'phot_g_n_obs', 'phot_g_mean_flux', 'phot_g_mean_flux_error',
                    'phot_g_mean_mag', 'phot_variable_flag', 'l', 'b', 'ecl_lon', 'ecl_lat']

    # print(csvs)
    print(f'# files to process: {len(csvs)}')

    for ci, cf in enumerate(csvs):
        print(f'processing file #{ci+1} of {len(csvs)}: {os.path.basename(cf)}')
        # with open(cf, newline='') as f:
        try:
            with open(cf) as f:
                reader = csv.reader(f, delimiter=',')
                # skip header:
                f.readline()
                for row in reader:
                    try:
                        doc = {column_name: val for column_name, val in zip(column_names, row)}

                        doc['_id'] = doc['source_id']

                        # drop empty fields:
                        for col in ['parallax', 'parallax_error',
                                    'pmra', 'pmra_error', 'pmdec', 'pmdec_error',
                                    'ra_parallax_corr', 'ra_pmra_corr', 'ra_pmdec_corr', 'dec_parallax_corr',
                                    'dec_pmra_corr', 'dec_pmdec_corr', 'parallax_pmra_corr',
                                    'parallax_pmdec_corr', 'pmra_pmdec_corr', 'astrometric_delta_q',
                                    'astrometric_weight_ac']:
                            try:
                                doc.pop(col, None)
                            except Exception as e:
                                traceback.print_exc()
                                print(e)

                        # convert ints
                        for col in ['solution_id', 'source_id', 'random_index', 'astrometric_n_obs_al',
                                    'astrometric_n_obs_ac', 'astrometric_n_good_obs_al', 'astrometric_n_good_obs_ac',
                                    'astrometric_n_bad_obs_al', 'astrometric_n_bad_obs_ac',
                                    'astrometric_priors_used', 'matched_observations', 'phot_g_n_obs']:
                            try:
                                doc[col] = int(doc[col])
                            except Exception as e:
                                traceback.print_exc()
                                print(e)

                        # convert floats
                        for col in ['ref_epoch', 'ra', 'ra_error', 'dec', 'dec_error',
                                    'ra_dec_corr', 'astrometric_excess_noise', 'astrometric_excess_noise_sig',
                                    'astrometric_relegation_factor', 'astrometric_weight_al',
                                    'scan_direction_strength_k1', 'scan_direction_strength_k2',
                                    'scan_direction_strength_k3', 'scan_direction_strength_k4',
                                    'scan_direction_mean_k1', 'scan_direction_mean_k2',
                                    'scan_direction_mean_k3', 'scan_direction_mean_k4',
                                    'phot_g_mean_flux', 'phot_g_mean_flux_error', 'phot_g_mean_mag',
                                    'l', 'b', 'ecl_lon', 'ecl_lat']:
                            try:
                                doc[col] = float(doc[col])
                            except Exception as e:
                                traceback.print_exc()
                                print(e)

                        # convert flags
                        for col in ['astrometric_primary_flag', 'duplicated_source']:
                            try:
                                if doc[col] in ('false', 'true'):
                                    doc[col] = eval(doc[col].capitalize())
                                # if doc[col] in ('NOT_AVAILABLE', ):
                                #     doc[col] = None
                            except Exception as e:
                                traceback.print_exc()
                                print(e)

                        # print(doc)
                        # print('\n')


                        doc['coordinates'] = {}
                        doc['coordinates']['epoch'] = doc['ref_epoch']
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
                        # radians:
                        doc['coordinates']['radec'] = [_ra * np.pi / 180.0, _dec * np.pi / 180.0]

                        # print(doc['coordinates'])

                        # insert into PanSTARRS collection. don't do that! too much overhead
                        # db[_collection].insert_one(doc)
                        # insert_db_entry(db, _collection=_collection, _db_entry=doc)
                        documents.append(doc)

                        # time.sleep(1)

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

        except Exception as e:
            traceback.print_exc()
            print(e)
            continue

    # stuff left from the last file?
    if len(documents) > 0:
        insert_multiple_db_entries(db, _collection=_collection, _db_entries=documents)

    # create 2d index:
    print('Creating 2d index')
    # db[_collection].create_index([('coordinates.radec_geojson', '2dsphere'), ('name', 1)])
    db[_collection].create_index([('coordinates.radec_geojson', '2dsphere')])
    print('All done')