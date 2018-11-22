import csv
import os
import glob
import time
# from astropy.coordinates import Angle
import numpy as np
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
    documents = []
    batch_num = 1

    # column names:
    column_names = [('solution_id', int),
                    ('designation', str),
                    ('source_id', int),
                    ('random_index', int),
                    ('ref_epoch', float),
                    ('ra', float),
                    ('ra_error', float),
                    ('dec', float),
                    ('dec_error', float),
                    ('parallax', float),
                    ('parallax_error', float),
                    ('parallax_over_error', float),
                    ('pmra', float),
                    ('pmra_error', float),
                    ('pmdec', float),
                    ('pmdec_error', float),
                    ('ra_dec_corr', float),
                    ('ra_parallax_corr', float),
                    ('ra_pmra_corr', float),
                    ('ra_pmdec_corr', float),
                    ('dec_parallax_corr', float),
                    ('dec_pmra_corr', float),
                    ('dec_pmdec_corr', float),
                    ('parallax_pmra_corr', float),
                    ('parallax_pmdec_corr', float),
                    ('pmra_pmdec_corr', float),
                    ('astrometric_n_obs_al', int),
                    ('astrometric_n_obs_ac', int),
                    ('astrometric_n_good_obs_al', int),
                    ('astrometric_n_bad_obs_al', int),
                    ('astrometric_gof_al', float),
                    ('astrometric_chi2_al', float),
                    ('astrometric_excess_noise', float),
                    ('astrometric_excess_noise_sig', float),
                    ('astrometric_params_solved', float),
                    ('astrometric_primary_flag', int),
                    ('astrometric_weight_al', float),
                    ('astrometric_pseudo_colour', float),
                    ('astrometric_pseudo_colour_error', float),
                    ('mean_varpi_factor_al', float),
                    ('astrometric_matched_observations', int),
                    ('visibility_periods_used', int),
                    ('astrometric_sigma5d_max', float),
                    ('frame_rotator_object_type', int),
                    ('matched_observations', int),
                    ('duplicated_source', int),
                    ('phot_g_n_obs', int),
                    ('phot_g_mean_flux', float),
                    ('phot_g_mean_flux_error', float),
                    ('phot_g_mean_flux_over_error', float),
                    ('phot_g_mean_mag', float),
                    ('phot_bp_n_obs', int),
                    ('phot_bp_mean_flux', float),
                    ('phot_bp_mean_flux_error', float),
                    ('phot_bp_mean_flux_over_error', float),
                    ('phot_bp_mean_mag', float),
                    ('phot_rp_n_obs', int),
                    ('phot_rp_mean_flux', float),
                    ('phot_rp_mean_flux_error', float),
                    ('phot_rp_mean_flux_over_error', float),
                    ('phot_rp_mean_mag', float),
                    ('phot_bp_rp_excess_factor', float),
                    ('phot_proc_mode', int),
                    ('bp_rp', float),
                    ('bp_g', float),
                    ('g_rp', float),
                    ('radial_velocity', float),
                    ('radial_velocity_error', float),
                    ('rv_nb_transits', int),
                    ('rv_template_teff', float),
                    ('rv_template_logg', float),
                    ('rv_template_fe_h', float),
                    ('phot_variable_flag', str),
                    ('l', float),
                    ('b', float),
                    ('ecl_lon', float),
                    ('ecl_lat', float),
                    ('priam_flags', int),
                    ('teff_val', float),
                    ('teff_percentile_lower', float),
                    ('teff_percentile_upper', float),
                    ('a_g_val', float),
                    ('a_g_percentile_lower', float),
                    ('a_g_percentile_upper', float),
                    ('e_bp_min_rp_val', float),
                    ('e_bp_min_rp_percentile_lower', float),
                    ('e_bp_min_rp_percentile_upper', float),
                    ('flame_flags', float),
                    ('radius_val', float),
                    ('radius_percentile_lower', float),
                    ('radius_percentile_upper', float),
                    ('lum_val', float),
                    ('lum_percentile_lower', float),
                    ('lum_percentile_upper', float)]

    try:
        with open(_file) as f:
            reader = csv.reader(f, delimiter=',')
            # skip header:
            f.readline()
            for row in reader:
                try:
                    doc = {column_name[0]: val for column_name, val in zip(column_names, row)}

                    doc['_id'] = doc['source_id']

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

                    documents.append(doc)

                    # time.sleep(1)

                    # insert batch, then flush
                    if len(documents) == _batch_size:
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
    if len(documents) > 0:
        print(f'inserting batch #{batch_num}')
        if not _dry_run:
            insert_multiple_db_entries(_db, _collection=_collection, _db_entries=documents)

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

    collection = 'Gaia_DR2'

    # number of records to insert
    batch_size = 4096

    _location = '/_tmp/gaia_dr2/'

    files = glob.glob(os.path.join(_location, 'Gaia*.csv'))

    print(f'# files to process: {len(files)}')

    # init threaded operations
    # pool = ThreadPoolExecutor(2)
    pool = ProcessPoolExecutor(20)

    # for ff in files[::-1]:
    for ff in sorted(files):
        pool.submit(process_file, _file=ff, _collection=collection, _batch_size=batch_size,
                    verbose=True, _dry_run=dry_run)
        # process_file(_file=ff, _collection=collection, _batch_size=batch_size,
        #              verbose=True, _dry_run=dry_run)

    # wait for everything to finish
    pool.shutdown(wait=True)

    # create 2d index:
    print('Creating 2d index')
    if not dry_run:
        db[collection].create_index([('coordinates.radec_geojson', '2dsphere')])

    print('All done')
