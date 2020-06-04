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


@jit
def great_circle_distance(phi1, lambda1, phi2, lambda2):
    # input: dec1, ra1, dec2, ra2 [rad]
    # this is orders of magnitude faster than astropy.coordinates.Skycoord.separation
    delta_lambda = np.abs(lambda2 - lambda1)
    return np.arctan2(np.sqrt((np.cos(phi2) * np.sin(delta_lambda)) ** 2
                              + (np.cos(phi1) * np.sin(phi2) -
                                 np.sin(phi1) * np.cos(phi2) * np.cos(delta_lambda)) ** 2),
                      np.sin(phi1) * np.sin(phi2) + np.cos(phi1) * np.cos(phi2) * np.cos(delta_lambda))


def radec_str2rad(_ra_str, _dec_str):
    """
    :param _ra_str: 'H:M:S'
    :param _dec_str: 'D:M:S'
    :return: ra, dec in rad
    """
    # convert to rad:
    _ra = list(map(float, _ra_str.split(':')))
    _ra = (_ra[0] + _ra[1] / 60.0 + _ra[2] / 3600.0) * np.pi / 12.
    _dec = list(map(float, _dec_str.split(':')))
    _sign = -1 if _dec_str.strip()[0] == '-' else 1
    _dec = _sign * (abs(_dec[0]) + abs(_dec[1]) / 60.0 + abs(_dec[2]) / 3600.0) * np.pi / 180.

    return _ra, _dec


# cone search radius in arcsec:
cone_search_radius = 2
# convert arcsec to rad:
cone_search_radius *= np.pi / 180.0 / 3600.


def xmatch(_db, ra, dec):
    """
        Cross-match by position
    """

    xmatches = dict()
    features = dict()

    # catalogs = config['xmatch']['catalogs']
    catalogs = {
        "AllWISE": {
            "filter": {},
            "projection": {
                "_id": 1,
                "coordinates.radec_str": 1,
                # "ra": 1,
                # "dec": 1,
                "w1mpro": 1,
                "w1sigmpro": 1,
                "w2mpro": 1,
                "w2sigmpro": 1,
                "w3mpro": 1,
                "w3sigmpro": 1,
                "w4mpro": 1,
                "w4sigmpro": 1,
                "ph_qual": 1
            }
        },
        "Gaia_DR2": {
            "filter": {},
            "projection": {
                "_id": 1,
                "coordinates.radec_str": 1,
                # "ra": 1,
                # "dec": 1,
                "phot_g_mean_mag": 1,
                "phot_bp_mean_mag": 1,
                "phot_rp_mean_mag": 1,
                "parallax": 1,
                "parallax_error": 1,
                "pmra": 1,
                "pmra_error": 1,
                "pmdec": 1,
                "pmdec_error": 1,
                "astrometric_excess_noise": 1,
                "phot_bp_rp_excess_factor": 1,
            }
        },
        "PS1_DR1": {
            "filter": {},
            "projection": {
                "_id": 1,
                "coordinates.radec_str": 1,
                # "raMean": 1,
                # "decMean": 1,
                "gMeanPSFMag": 1, "gMeanPSFMagErr": 1,
                "rMeanPSFMag": 1, "rMeanPSFMagErr": 1,
                "iMeanPSFMag": 1, "iMeanPSFMagErr": 1,
                "zMeanPSFMag": 1, "zMeanPSFMagErr": 1,
                "yMeanPSFMag": 1, "yMeanPSFMagErr": 1,
                "qualityFlag": 1
            }
        }
    }

    try:
        ra_geojson = float(ra)
        # geojson-friendly ra:
        ra_geojson -= 180.0
        dec_geojson = float(dec)

        ''' catalogs '''
        for catalog in catalogs:
            catalog_filter = config['xmatch']['catalogs'][catalog]['filter']
            catalog_projection = config['xmatch']['catalogs'][catalog]['projection']

            object_position_query = dict()
            object_position_query['coordinates.radec_geojson'] = {
                '$geoWithin': {'$centerSphere': [[ra_geojson, dec_geojson], cone_search_radius]}}
            s = _db[catalog].find(
                {**object_position_query, **catalog_filter},
                {**catalog_projection}
            )
            xmatches[catalog] = list(s)

        # convert into a dict of features
        for catalog in catalogs:
            if len(xmatches[catalog]) > 0:
                # pick the nearest match:
                if len(xmatches[catalog]) > 1:
                    ii = np.argmin([great_circle_distance(dec * np.pi / 180, ra * np.pi / 180,
                                                          *radec_str2rad(*dd['coordinates']['radec_str'])[::-1])
                                    for dd in xmatches[catalog]])

                    xmatch = xmatches[catalog][int(ii)]
                else:
                    xmatch = xmatches[catalog][0]
            else:
                xmatch = dict()

            for feature in catalogs[catalog]['projection'].keys():
                if feature == 'coordinates.radec_str':
                    continue
                f = xmatch.get(feature, None)
                f_name = f"{catalog}__{feature}"
                features[f_name] = f

    except Exception as e:
        print(str(e))

    return features


def get_n_ztf_alerts(_db, ra, dec):
    """
        Cross-match by position
    """

    try:
        ra_geojson = float(ra)
        # geojson-friendly ra:
        ra_geojson -= 180.0
        dec_geojson = float(dec)

        ''' catalogs '''
        catalog = 'ZTF_alerts'
        object_position_query = dict()
        object_position_query['coordinates.radec_geojson'] = {
            '$geoWithin': {'$centerSphere': [[ra_geojson, dec_geojson], cone_search_radius]}}
        n = int(_db[catalog].count_documents(object_position_query))

    except Exception as e:
        print(str(e))
        n = None

    return n


def get_mean_ztf_alert_braai(_db, ra, dec):
    """
        Cross-match by position and get mean alert braai score
    """

    try:
        ra_geojson = float(ra)
        # geojson-friendly ra:
        ra_geojson -= 180.0
        dec_geojson = float(dec)

        ''' catalogs '''
        catalog = 'ZTF_alerts'
        object_position_query = dict()
        object_position_query['coordinates.radec_geojson'] = {
            '$geoWithin': {'$centerSphere': [[ra_geojson, dec_geojson], cone_search_radius]}}
        # n = int(_db[catalog].count_documents(object_position_query))
        objects = list(_db[catalog].aggregate([
            {'$match': object_position_query},
            {'$project': {'objectId': 1, 'classifications.braai': 1}},
            {'$group': {'_id': '$objectId', 'braai_avg': {'$avg': '$classifications.braai'}}}
        ]))
        if len(objects) > 0:
            # there may be multiple objectId's in the match due to astrometric errors:
            braais = [float(o.get('braai_avg', 0)) for o in objects if o.get('braai_avg', None)]
            if len(braais) > 0:
                braai_avg = np.mean(braais)
            else:
                braai_avg = None
        else:
            braai_avg = None

    except Exception as e:
        print(str(e))
        braai_avg = None

    return braai_avg


# Ashish's original for CRTS
# dmints = [-8, -5, -3, -2.5, -2, -1.5, -1, -0.5, -0.3, -0.2, -0.1, 0,
#           0.1, 0.2, 0.3, 0.5, 1, 1.5, 2, 2.5, 3, 5, 8]
# dtints = [0.0, 1.0 / 145, 2.0 / 145, 3.0 / 145, 4.0 / 145,
#           1.0 / 25, 2.0 / 25, 3.0 / 25, 1.5, 2.5, 3.5, 4.5, 5.5, 7,
#           10, 20, 30, 60, 90, 120, 240, 600, 960, 2000, 4000]
# v. 20200202
# dmints = [-8, -5, -4, -3, -2.5, -2, -1.5, -1, -0.5, -0.3, -0.2, -0.1, -0.05, 0,
#           0.05, 0.1, 0.2, 0.3, 0.5, 1, 1.5, 2, 2.5, 3, 4, 5, 8]
# dtints = [0.0, 1.0 / 25, 2.0 / 25, 3.0 / 25, 0.3, 0.5, 0.75, 1, 1.5, 2.5, 3.5, 4.5, 5.5, 7,
#           10, 20, 30, 60, 90, 120, 240, 500, 650, 900, 1200, 1500, 2000]
# v. 20200205: optimized based on random 100,000 examples from the 20 pilot fields
# dmints = [-8, -5, -4, -3, -2.5, -2, -1.5, -1, -0.5, -0.3, -0.2, -0.1, -0.05, 0,
#           0.05, 0.1, 0.2, 0.3, 0.5, 1, 1.5, 2, 2.5, 3, 4, 5, 8]
# dtints = [0.0, 4.0 / 145, 1.0 / 25, 2.0 / 25, 3.0 / 25, 0.3, 0.75, 1, 1.5, 2.5, 3.5, 4.5, 5.5, 7,
#           10, 20, 30, 45, 60, 90, 120, 180, 240, 360, 500, 650, 2000]
# v. 20200318: maximum baseline limited at 240 days
dmints = [-8, -4.5, -3, -2.5, -2, -1.5, -1.25, -0.75, -0.5, -0.3, -0.2, -0.1, -0.05, 0,
          0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1.25, 1.5, 2, 2.5, 3, 4.5, 8]
dtints = [0.0, 4.0 / 145, 1.0 / 25, 2.0 / 25, 3.0 / 25, 0.3, 0.5, 0.75, 1, 1.5, 2.5, 3.5, 4.5, 5.5, 7,
          10, 20, 30, 45, 60, 75, 90, 120, 150, 180, 210, 240]

dmdt_ints = {
    'original': {
        'dmints': [-8, -5, -3, -2.5, -2, -1.5, -1, -0.5, -0.3, -0.2, -0.1, 0,
                   0.1, 0.2, 0.3, 0.5, 1, 1.5, 2, 2.5, 3, 5, 8],
        'dtints': [0.0, 1.0 / 145, 2.0 / 145, 3.0 / 145, 4.0 / 145, 1.0 / 25, 2.0 / 25, 3.0 / 25,
                   1.5, 2.5, 3.5, 4.5, 5.5, 7, 10, 20, 30, 60, 90, 120, 240, 600, 960, 2000, 4000]
    },
    'v20200205': {
        'dmints': [-8, -5, -4, -3, -2.5, -2, -1.5, -1, -0.5, -0.3, -0.2, -0.1, -0.05, 0,
                   0.05, 0.1, 0.2, 0.3, 0.5, 1, 1.5, 2, 2.5, 3, 4, 5, 8],
        'dtints': [0.0, 4.0 / 145, 1.0 / 25, 2.0 / 25, 3.0 / 25, 0.3, 0.75, 1, 1.5, 2.5, 3.5, 4.5, 5.5, 7,
                   10, 20, 30, 45, 60, 90, 120, 180, 240, 360, 500, 650, 2000]
    },
    'v20200318': {
        'dmints': [-8, -4.5, -3, -2.5, -2, -1.5, -1.25, -0.75, -0.5, -0.3, -0.2, -0.1, -0.05, 0,
                   0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1.25, 1.5, 2, 2.5, 3, 4.5, 8],
        'dtints': [0.0, 4.0 / 145, 1.0 / 25, 2.0 / 25, 3.0 / 25, 0.3, 0.5, 0.75, 1, 1.5, 2.5, 3.5, 4.5, 5.5, 7,
                   10, 20, 30, 45, 60, 75, 90, 120, 150, 180, 210, 240]
    }
}


@jit
def pwd_for(a):
    """
        Compute pairwise differences with for loops
    """
    return np.array([a[j] - a[i] for i in range(len(a)) for j in range(i + 1, len(a))])


@jit
def pwd_np(a):
    """
        Compute pairwise differences with numpy
    """
    diff = (np.expand_dims(a, axis=0) - np.expand_dims(a, axis=1))
    i, j = np.where(np.triu(diff) != 0)
    return diff[i, j]


# @jit
def compute_dmdt(jd, mag, dmdt_ints_v: str = 'v20200318'):
    jd_diff = pwd_for(jd)
    mag_diff = pwd_for(mag)

    hh, ex, ey = np.histogram2d(jd_diff, mag_diff,
                                bins=[dmdt_ints[dmdt_ints_v]['dtints'], dmdt_ints[dmdt_ints_v]['dmints']])
    # extent = [ex[0], ex[-1], ey[0], ey[-1]]
    dmdt = hh
    dmdt = np.transpose(dmdt)
    # dmdt = (maxval * dmdt / dmdt.shape[0])
    norm = np.linalg.norm(dmdt)
    if norm != 0.0:
        dmdt /= np.linalg.norm(dmdt)
    else:
        dmdt = np.zeros_like(dmdt)

    return dmdt


def lc_dmdt(_db, _id, catalog: str = 'ZTF_sources_20191101', dmdt_ints_v: str = 'v20200318'):
    try:
        c = _db[catalog].find({'_id': _id}, {"_id": 0, "data.catflags": 1, "data.hjd": 1, "data.mag": 1})
        lc = list(c)[0]

        df_lc = pd.DataFrame.from_records(lc['data'])
        w_good = df_lc['catflags'] == 0
        df_lc = df_lc.loc[w_good]

        dmdt = compute_dmdt(df_lc['hjd'].values, df_lc['mag'].values, dmdt_ints_v)
    except:
        dmdt = np.zeros((len(dmdt_ints[dmdt_ints_v]['dtints']), len(dmdt_ints[dmdt_ints_v]['dtints'])))

    return dmdt


filters = {'zg': 1, 'zr': 2, 'zi': 3}


def process_file(fcvdx):
    _file, _collections, _verbose, _dry_run, _xmatch = fcvdx

    # connect to MongoDB:
    if _verbose:
        print('Connecting to DB')
    _client, _db = connect_to_db()
    if _verbose:
        print('Successfully connected')

    if _verbose:
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

            # get number of ZTF alerts within 2"
            n_ztf_alerts = get_n_ztf_alerts(_db, doc['ra'], doc['dec'])
            doc['n_ztf_alerts'] = n_ztf_alerts

            if n_ztf_alerts > 0:
                doc['mean_ztf_alert_braai'] = get_mean_ztf_alert_braai(_db, doc['ra'], doc['dec'])
            else:
                doc['mean_ztf_alert_braai'] = None

            # compute dmdt's
            # with long time baseline:
            # dmdt_long = lc_dmdt(_db, doc['_id'], catalog=_collections['sources'], dmdt_ints_v='v20200205')
            # doc['dmdt_long'] = dmdt_long.tolist()
            # with short time baseline:
            dmdt = lc_dmdt(_db, doc['_id'], catalog=_collections['sources'], dmdt_ints_v='v20200318')
            doc['dmdt'] = dmdt.tolist()

            # Cross-match:
            # print(_xmatch)
            if _xmatch:
                # xmatches = xmatch(_db, doc['ra'], doc['dec'])
                # doc['cross_matches'] = xmatches
                features = xmatch(_db, doc['ra'], doc['dec'])
                # print(features)
                for feature in features:
                    doc[feature] = features[feature]

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

        if _verbose:
            print(f'ingesting contents of {_file}')
        if not _dry_run:
            insert_multiple_db_entries(_db, _collection=_collections['features'],
                                       _db_entries=docs, _verbose=_verbose)

    except Exception as e:
        traceback.print_exc()
        print(e)

    # disconnect from db:
    try:
        _client.close()
        if _verbose:
            print('Successfully disconnected from db')
    finally:
        pass


if __name__ == '__main__':
    ''' Create command line argument parser '''
    parser = argparse.ArgumentParser()

    parser.add_argument('--dryrun', action='store_true', help='dry run?')
    parser.add_argument('--verbose', action='store_true', help='verbose?')
    parser.add_argument('--xmatch', action='store_true', help='cross match?')

    args = parser.parse_args()

    dry_run = args.dryrun
    verbose = args.verbose
    cross_match = args.xmatch

    # connect to MongoDB:
    print('Connecting to DB')
    client, db = connect_to_db()
    print('Successfully connected')

    t_tag = '20191101'

    collections = {'sources': f'ZTF_sources_{t_tag}',
                   'features': f'ZTF_source_features_{t_tag}_dr2'}

    # create indices:
    print('Creating indices')
    if not dry_run:
        db[collections['features']].create_index([('coordinates.radec_geojson', '2dsphere'),
                                                  ('_id', pymongo.ASCENDING)], background=True)
        db[collections['features']].create_index([('period', pymongo.DESCENDING),
                                                  ('significance', pymongo.DESCENDING)], background=True)

    # _location = f'/_tmp/ztf_variability/quadrants_GCE_LS_AOV_0_300/catalog/GCE_LS_AOV/'
    # _location = f'/_tmp/ztf_variability/quadrants_GCE_LS_AOV_300_400/catalog/GCE_LS_AOV/'
    # _location = f'/_tmp/ztf_variability/quadrants_GCE_LS_AOV_400_500/catalog/GCE_LS_AOV/'
    # _location = f'/_tmp/ztf_variability/quadrants_GCE_LS_AOV_500_600/catalog/GCE_LS_AOV/'
    # _location = f'/_tmp/ztf_variability/quadrants_GCE_LS_AOV_600_700/catalog/GCE_LS_AOV/'
    # _location = f'/_tmp/ztf_variability/quadrants_GCE_LS_AOV_700_800/catalog/GCE_LS_AOV/'
    # _location = f'/_tmp/ztf_variability/quadrants_GCE_LS_AOV_800_900/catalog/GCE_LS_AOV/'
    _location = f'/_tmp/ztf_variability/quadrants_GCE_LS_AOV_20Fields_v2/catalog/GCE_LS_AOV/'

    files = glob.glob(os.path.join(_location, '*.h5'))

    input_list = [(f, collections, verbose, dry_run, cross_match) for f in sorted(files) if os.stat(f).st_size != 0]

    print(f'# files to process: {len(input_list)}')

    # process_file(input_list[0])
    with mp.Pool(processes=40) as p:
        results = list(tqdm(p.imap(process_file, input_list), total=len(input_list)))

    print('All done')
