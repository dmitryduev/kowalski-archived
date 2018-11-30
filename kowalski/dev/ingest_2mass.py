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
from numba import jit
import time
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


def process_psc(_file, _collection, _batch_size=2048, verbose=False):
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
    column_names = ['ra', 'decl', 'err_maj', 'err_min', 'err_ang', 'designation',
                    'j_m', 'j_cmsig', 'j_msigcom', 'j_snr',
                    'h_m', 'h_cmsig', 'h_msigcom', 'h_snr',
                    'k_m', 'k_cmsig', 'k_msigcom', 'k_snr',
                    'ph_qual', 'rd_flg', 'bl_flg', 'cc_flg',
                    'ndet', 'prox', 'pxpa', 'pxcntr', 'gal_contam',
                    'mp_flg', 'pts_key', 'hemis', 'date', 'scan', 'glon', 'glat',
                    'x_scan', 'jdate', 'j_psfchi', 'h_psfchi', 'k_psfchi', 'j_m_stdap', 'j_msig_stdap',
                    'h_m_stdap', 'h_msig_stdap', 'k_m_stdap', 'k_msig_stdap',
                    'dist_edge_ns', 'dist_edge_ew', 'dist_edge_flg', 'dup_src',
                    'use_src', 'a', 'dist_opt', 'phi_opt', 'b_m_opt', 'vr_m_opt',
                    'nopt_mchs', 'ext_key', 'scan_key', 'coadd_key', 'coadd']

    try:
        with open(_file) as f:
            reader = csv.reader(f, delimiter='|')
            # skip header:
            f.readline()
            for row in reader:
                try:
                    doc = {column_name: val for column_name, val in zip(column_names, row)}

                    doc['_id'] = doc['designation']

                    # convert ints
                    for col in ['err_ang', 'pxpa', 'pxcntr', 'gal_contam', 'mp_flg', 'pts_key', 'scan',
                                'dist_edge_ns', 'dist_edge_ew', 'dup_src', 'use_src', 'phi_opt',
                                'nopt_mchs', 'ext_key', 'scan_key', 'coadd_key', 'coadd']:
                        try:
                            if doc[col] == '\\N':
                                doc[col] = None
                            else:
                                doc[col] = int(doc[col])
                        except Exception as e:
                            traceback.print_exc()
                            print(e)

                    # convert floats
                    for col in ['ra', 'decl', 'err_maj', 'err_min',
                                'j_m', 'j_cmsig', 'j_msigcom', 'j_snr',
                                'h_m', 'h_cmsig', 'h_msigcom', 'h_snr',
                                'k_m', 'k_cmsig', 'k_msigcom', 'k_snr',
                                'prox', 'glon', 'glat', 'x_scan', 'jdate',
                                'j_psfchi', 'h_psfchi', 'k_psfchi', 'j_m_stdap', 'j_msig_stdap',
                                'h_m_stdap', 'h_msig_stdap', 'k_m_stdap', 'k_msig_stdap',
                                'dist_opt', 'b_m_opt', 'vr_m_opt']:
                        try:
                            if doc[col] == '\\N':
                                doc[col] = None
                            else:
                                doc[col] = float(doc[col])
                        except Exception as e:
                            traceback.print_exc()
                            print(e)

                    doc['coordinates'] = {}
                    doc['coordinates']['epoch'] = doc['date']
                    _ra = doc['ra']
                    _dec = doc['decl']
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
                    if len(documents) % _batch_size == 0:
                        print(f'inserting batch #{batch_num}')
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


def process_xsc(_file, _collection, _batch_size=2048, verbose=False):
    pass


if __name__ == '__main__':
    ''' Create command line argument parser '''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description='')
    args = parser.parse_args()

    # connect to MongoDB:
    print('Connecting to DB')
    client, db = connect_to_db()
    print('Successfully connected')

    _location = '/_tmp/twomass'

    # number of records to insert
    batch_size = 8192
    batch_num = 1
    documents = []

    ''' point source data '''
    collection = '2MASS_PSC'
    print(collection)

    # create 2d index:
    print('Creating 2d index')
    # db[_collection].create_index([('coordinates.radec_geojson', '2dsphere'), ('name', 1)])
    db[collection].create_index([('coordinates.radec_geojson', '2dsphere')], background=True)

    csvs = glob.glob(os.path.join(_location, 'psc_*'))
    print(f'# files to process: {len(csvs)}')

    # init threaded operations
    # pool = ThreadPoolExecutor(2)
    pool = ProcessPoolExecutor(4)

    # for ff in files[::-1]:
    for ff in sorted(csvs):
        pool.submit(process_psc, _file=ff, _collection=collection, _batch_size=batch_size, verbose=True)

    # wait for everything to finish
    pool.shutdown(wait=True)

    ''' extended source data '''
    _collection = '2MASS_XSC'
    print(_collection)

    # create 2d index:
    print('Creating 2d index')
    # db[_collection].create_index([('coordinates.radec_geojson', '2dsphere'), ('name', 1)])
    db[_collection].create_index([('coordinates.radec_geojson', '2dsphere')], background=True)

    csvs = glob.glob(os.path.join(_location, 'xsc_*'))

    documents = []

    # column names:
    column_names = ['jdate', 'designation', 'ra', 'decl', 'sup_ra', 'sup_dec', 'glon', 'glat',
                    'density', 'r_k20fe', 'j_m_k20fe', 'j_msig_k20fe', 'j_flg_k20fe',
                    'h_m_k20fe', 'h_msig_k20fe', 'h_flg_k20fe', 'k_m_k20fe', 'k_msig_k20fe', 'k_flg_k20fe',
                    'r_3sig', 'j_ba', 'j_phi', 'h_ba', 'h_phi', 'k_ba', 'k_phi', 'sup_r_3sig',
                    'sup_ba', 'sup_phi', 'r_fe', 'j_m_fe', 'j_msig_fe', 'j_flg_fe', 'h_m_fe',
                    'h_msig_fe', 'h_flg_fe', 'k_m_fe', 'k_msig_fe', 'k_flg_fe', 'r_ext',
                    'j_m_ext', 'j_msig_ext', 'j_pchi', 'h_m_ext', 'h_msig_ext', 'h_pchi',
                    'k_m_ext', 'k_msig_ext', 'k_pchi', 'j_r_eff', 'j_mnsurfb_eff', 'h_r_eff',
                    'h_mnsurfb_eff', 'k_r_eff', 'k_mnsurfb_eff', 'j_con_indx', 'h_con_indx',
                    'k_con_indx', 'j_peak', 'h_peak', 'k_peak', 'j_5surf', 'h_5surf', 'k_5surf',
                    'e_score', 'g_score', 'vc', 'cc_flg', 'im_nx', 'r_k20fc', 'j_m_k20fc',
                    'j_msig_k20fc', 'j_flg_k20fc', 'h_m_k20fc', 'h_msig_k20fc', 'h_flg_k20fc',
                    'k_m_k20fc', 'k_msig_k20fc', 'k_flg_k20fc', 'j_r_e', 'j_m_e', 'j_msig_e',
                    'j_flg_e', 'h_r_e', 'h_m_e', 'h_msig_e', 'h_flg_e', 'k_r_e', 'k_m_e',
                    'k_msig_e', 'k_flg_e', 'j_r_c', 'j_m_c', 'j_msig_c', 'j_flg_c', 'h_r_c',
                    'h_m_c', 'h_msig_c', 'h_flg_c', 'k_r_c', 'k_m_c', 'k_msig_c', 'k_flg_c',
                    'r_fc', 'j_m_fc', 'j_msig_fc', 'j_flg_fc', 'h_m_fc', 'h_msig_fc', 'h_flg_fc',
                    'k_m_fc', 'k_msig_fc', 'k_flg_fc', 'j_r_i20e', 'j_m_i20e', 'j_msig_i20e',
                    'j_flg_i20e', 'h_r_i20e', 'h_m_i20e', 'h_msig_i20e', 'h_flg_i20e', 'k_r_i20e',
                    'k_m_i20e', 'k_msig_i20e', 'k_flg_i20e', 'j_r_i20c', 'j_m_i20c', 'j_msig_i20c',
                    'j_flg_i20c', 'h_r_i20c', 'h_m_i20c', 'h_msig_i20c', 'h_flg_i20c', 'k_r_i20c',
                    'k_m_i20c', 'k_msig_i20c', 'k_flg_i20c', 'j_r_i21e', 'j_m_i21e', 'j_msig_i21e',
                    'j_flg_i21e', 'h_r_i21e', 'h_m_i21e', 'h_msig_i21e', 'h_flg_i21e', 'k_r_i21e',
                    'k_m_i21e', 'k_msig_i21e', 'k_flg_i21e', 'r_j21fe', 'j_m_j21fe', 'j_msig_j21fe',
                    'j_flg_j21fe', 'h_m_j21fe', 'h_msig_j21fe', 'h_flg_j21fe', 'k_m_j21fe',
                    'k_msig_j21fe', 'k_flg_j21fe', 'j_r_i21c', 'j_m_i21c', 'j_msig_i21c', 'j_flg_i21c',
                    'h_r_i21c', 'h_m_i21c', 'h_msig_i21c', 'h_flg_i21c', 'k_r_i21c', 'k_m_i21c',
                    'k_msig_i21c', 'k_flg_i21c', 'r_j21fc', 'j_m_j21fc', 'j_msig_j21fc', 'j_flg_j21fc',
                    'h_m_j21fc', 'h_msig_j21fc', 'h_flg_j21fc', 'k_m_j21fc', 'k_msig_j21fc',
                    'k_flg_j21fc', 'j_m_5', 'j_msig_5', 'j_flg_5', 'h_m_5', 'h_msig_5', 'h_flg_5',
                    'k_m_5', 'k_msig_5', 'k_flg_5', 'j_m_7', 'j_msig_7', 'j_flg_7', 'h_m_7',
                    'h_msig_7', 'h_flg_7', 'k_m_7', 'k_msig_7', 'k_flg_7', 'j_m_10',
                    'j_msig_10', 'j_flg_10', 'h_m_10', 'h_msig_10', 'h_flg_10', 'k_m_10',
                    'k_msig_10', 'k_flg_10', 'j_m_15', 'j_msig_15', 'j_flg_15', 'h_m_15',
                    'h_msig_15', 'h_flg_15', 'k_m_15', 'k_msig_15', 'k_flg_15', 'j_m_20',
                    'j_msig_20', 'j_flg_20', 'h_m_20', 'h_msig_20', 'h_flg_20', 'k_m_20',
                    'k_msig_20', 'k_flg_20', 'j_m_25', 'j_msig_25', 'j_flg_25', 'h_m_25',
                    'h_msig_25', 'h_flg_25', 'k_m_25', 'k_msig_25', 'k_flg_25', 'j_m_30',
                    'j_msig_30', 'j_flg_30', 'h_m_30', 'h_msig_30', 'h_flg_30', 'k_m_30',
                    'k_msig_30', 'k_flg_30', 'j_m_40', 'j_msig_40', 'j_flg_40', 'h_m_40',
                    'h_msig_40', 'h_flg_40', 'k_m_40', 'k_msig_40', 'k_flg_40', 'j_m_50',
                    'j_msig_50', 'j_flg_50', 'h_m_50', 'h_msig_50', 'h_flg_50', 'k_m_50',
                    'k_msig_50', 'k_flg_50', 'j_m_60', 'j_msig_60', 'j_flg_60', 'h_m_60',
                    'h_msig_60', 'h_flg_60', 'k_m_60', 'k_msig_60', 'k_flg_60', 'j_m_70',
                    'j_msig_70', 'j_flg_70', 'h_m_70', 'h_msig_70', 'h_flg_70', 'k_m_70',
                    'k_msig_70', 'k_flg_70', 'j_m_sys', 'j_msig_sys', 'h_m_sys', 'h_msig_sys',
                    'k_m_sys', 'k_msig_sys', 'sys_flg', 'contam_flg', 'j_5sig_ba', 'j_5sig_phi',
                    'h_5sig_ba', 'h_5sig_phi', 'k_5sig_ba', 'k_5sig_phi', 'j_d_area', 'j_perc_darea',
                    'h_d_area', 'h_perc_darea', 'k_d_area', 'k_perc_darea', 'j_bisym_rat',
                    'j_bisym_chi', 'h_bisym_rat', 'h_bisym_chi', 'k_bisym_rat', 'k_bisym_chi',
                    'j_sh0', 'j_sig_sh0', 'h_sh0', 'h_sig_sh0', 'k_sh0', 'k_sig_sh0', 'j_sc_mxdn',
                    'j_sc_sh', 'j_sc_wsh', 'j_sc_r23', 'j_sc_1mm', 'j_sc_2mm', 'j_sc_vint', 'j_sc_r1',
                    'j_sc_msh', 'h_sc_mxdn', 'h_sc_sh', 'h_sc_wsh', 'h_sc_r23', 'h_sc_1mm', 'h_sc_2mm',
                    'h_sc_vint', 'h_sc_r1', 'h_sc_msh', 'k_sc_mxdn', 'k_sc_sh', 'k_sc_wsh', 'k_sc_r23',
                    'k_sc_1mm', 'k_sc_2mm', 'k_sc_vint', 'k_sc_r1', 'k_sc_msh', 'j_chif_ellf', 'k_chif_ellf',
                    'ellfit_flg', 'sup_chif_ellf', 'n_blank', 'n_sub', 'bl_sub_flg',
                    'id_flg', 'id_cat', 'fg_flg', 'blk_fac', 'dup_src', 'use_src',
                    'prox', 'pxpa', 'pxcntr', 'dist_edge_ns', 'dist_edge_ew', 'dist_edge_flg',
                    'pts_key', 'mp_key', 'night_key', 'scan_key', 'coadd_key', 'hemis',
                    'date', 'scan', 'coadd', 'id', 'x_coadd', 'y_coadd', 'j_subst2', 'h_subst2',
                    'k_subst2', 'j_back', 'h_back', 'k_back', 'j_resid_ann', 'h_resid_ann',
                    'k_resid_ann', 'j_bndg_per', 'j_bndg_amp', 'h_bndg_per', 'h_bndg_amp',
                    'k_bndg_per', 'k_bndg_amp', 'j_seetrack', 'h_seetrack', 'k_seetrack', 'ext_key']

    # print(csvs)
    print(f'# files to process: {len(csvs)}')

    for ci, cf in enumerate(csvs):
        print(f'processing file #{ci+1} of {len(csvs)}: {os.path.basename(cf)}')
        # with open(cf, newline='') as f:
        try:
            with open(cf) as f:
                reader = csv.reader(f, delimiter='|')
                # skip header:
                f.readline()
                for row in reader:
                    try:
                        doc = {column_name: val for column_name, val in zip(column_names, row)}

                        doc['_id'] = doc['designation']

                        # convert ints
                        for col in ['j_flg_k20fe', 'h_flg_k20fe', 'k_flg_k20fe', 'j_phi', 'h_phi', 'k_phi',
                                    'sup_phi', 'j_flg_fe', 'h_flg_fe', 'k_flg_fe', 'vc', 'im_nx',
                                    'j_flg_k20fc', 'h_flg_k20fc', 'k_flg_k20fc', 'j_flg_e', 'h_flg_e',
                                    'k_flg_e', 'j_flg_c', 'h_flg_c', 'k_flg_c', 'j_flg_fc', 'h_flg_fc',
                                    'k_flg_fc', 'j_flg_i20e', 'h_flg_i20e', 'k_flg_i20e', 'j_flg_i20c',
                                    'h_flg_i20c', 'k_flg_i20c', 'j_flg_i21e', 'h_flg_i21e', 'k_flg_i21e',
                                    'j_flg_j21fe', 'h_flg_j21fe', 'k_flg_j21fe', 'j_flg_i21c', 'h_flg_i21c',
                                    'k_flg_i21c', 'j_flg_j21fc', 'h_flg_j21fc', 'k_flg_j21fc', 'j_flg_5',
                                    'h_flg_5', 'k_flg_5', 'j_flg_7', 'h_flg_7', 'k_flg_7', 'j_flg_10',
                                    'h_flg_10', 'k_flg_10', 'j_flg_15', 'h_flg_15', 'k_flg_15', 'j_flg_20',
                                    'h_flg_20', 'k_flg_20', 'j_flg_25', 'h_flg_25', 'k_flg_25', 'j_flg_30',
                                    'h_flg_30', 'k_flg_30', 'j_flg_40', 'h_flg_40', 'k_flg_40', 'j_flg_50',
                                    'h_flg_50', 'k_flg_50', 'j_flg_60', 'h_flg_60', 'k_flg_60', 'j_flg_70',
                                    'h_flg_70', 'k_flg_70', 'sys_flg', 'contam_flg', 'j_5sig_phi', 'h_5sig_phi',
                                    'k_5sig_phi', 'j_d_area', 'j_perc_darea', 'h_d_area', 'h_perc_darea',
                                    'k_d_area', 'k_perc_darea', 'ellfit_flg', 'n_blank', 'n_sub',
                                    'bl_sub_flg', 'id_flg', 'blk_fac', 'dup_src', 'use_src',
                                    'pxpa', 'pxcntr', 'dist_edge_ns', 'dist_edge_ew', 'pts_key',
                                    'mp_key', 'night_key', 'scan_key', 'coadd_key', 'scan', 'coadd',
                                    'id', 'j_bndg_per', 'h_bndg_per', 'k_bndg_per', 'ext_key']:
                            try:
                                if doc[col] == '\\N':
                                    doc[col] = None
                                else:
                                    doc[col] = int(doc[col])
                            except Exception as e:
                                traceback.print_exc()
                                print(e)

                        # convert floats
                        for col in ['jdate', 'ra', 'decl', 'sup_ra', 'sup_dec', 'glon', 'glat', 'density',
                                    'r_k20fe', 'j_m_k20fe', 'j_msig_k20fe', 'h_m_k20fe', 'h_msig_k20fe',
                                    'k_m_k20fe', 'k_msig_k20fe', 'r_3sig', 'j_ba', 'h_ba', 'k_ba', 'sup_r_3sig',
                                    'sup_ba', 'r_fe', 'j_m_fe', 'j_msig_fe', 'h_m_fe', 'h_msig_fe', 'k_m_fe',
                                    'k_msig_fe', 'r_ext', 'j_m_ext', 'j_msig_ext', 'j_pchi', 'h_m_ext',
                                    'h_msig_ext', 'h_pchi', 'k_m_ext', 'k_msig_ext', 'k_pchi', 'j_r_eff',
                                    'j_mnsurfb_eff', 'h_r_eff', 'h_mnsurfb_eff', 'k_r_eff', 'k_mnsurfb_eff',
                                    'j_con_indx', 'h_con_indx', 'k_con_indx', 'j_peak', 'h_peak', 'k_peak',
                                    'j_5surf', 'h_5surf', 'k_5surf', 'e_score', 'g_score', 'r_k20fc', 'j_m_k20fc',
                                    'j_msig_k20fc', 'h_m_k20fc', 'h_msig_k20fc', 'k_m_k20fc', 'k_msig_k20fc',
                                    'j_r_e', 'j_m_e', 'j_msig_e', 'h_r_e', 'h_m_e', 'h_msig_e', 'k_r_e', 'k_m_e',
                                    'k_msig_e', 'j_r_c', 'j_m_c', 'j_msig_c', 'h_r_c', 'h_m_c', 'h_msig_c',
                                    'k_r_c', 'k_m_c', 'k_msig_c', 'r_fc', 'j_m_fc', 'j_msig_fc', 'h_m_fc',
                                    'h_msig_fc', 'k_m_fc', 'k_msig_fc', 'j_r_i20e', 'j_m_i20e', 'j_msig_i20e',
                                    'h_r_i20e', 'h_m_i20e', 'h_msig_i20e', 'k_r_i20e', 'k_m_i20e', 'k_msig_i20e',
                                    'j_r_i20c', 'j_m_i20c', 'j_msig_i20c', 'h_r_i20c', 'h_m_i20c', 'h_msig_i20c',
                                    'k_r_i20c', 'k_m_i20c', 'k_msig_i20c', 'j_r_i21e', 'j_m_i21e', 'j_msig_i21e',
                                    'h_r_i21e', 'h_m_i21e', 'h_msig_i21e', 'k_r_i21e', 'k_m_i21e', 'k_msig_i21e',
                                    'r_j21fe', 'j_m_j21fe', 'j_msig_j21fe', 'h_m_j21fe', 'h_msig_j21fe', 'k_m_j21fe',
                                    'k_msig_j21fe', 'j_r_i21c', 'j_m_i21c', 'j_msig_i21c', 'h_r_i21c', 'h_m_i21c',
                                    'h_msig_i21c', 'k_r_i21c', 'k_m_i21c', 'k_msig_i21c', 'r_j21fc', 'j_m_j21fc',
                                    'j_msig_j21fc', 'h_m_j21fc', 'h_msig_j21fc', 'k_m_j21fc', 'k_msig_j21fc',
                                    'j_m_5', 'j_msig_5', 'h_m_5', 'h_msig_5', 'k_m_5', 'k_msig_5', 'j_m_7',
                                    'j_msig_7', 'h_m_7', 'h_msig_7', 'k_m_7', 'k_msig_7', 'j_m_10', 'j_msig_10',
                                    'h_m_10', 'h_msig_10', 'k_m_10', 'k_msig_10', 'j_m_15', 'j_msig_15', 'h_m_15',
                                    'h_msig_15', 'k_m_15', 'k_msig_15', 'j_m_20', 'j_msig_20', 'h_m_20', 'h_msig_20',
                                    'k_m_20', 'k_msig_20', 'j_m_25', 'j_msig_25', 'h_m_25', 'h_msig_25', 'k_m_25',
                                    'k_msig_25', 'j_m_30', 'j_msig_30', 'h_m_30', 'h_msig_30', 'k_m_30', 'k_msig_30',
                                    'j_m_40', 'j_msig_40', 'h_m_40', 'h_msig_40', 'k_m_40', 'k_msig_40', 'j_m_50',
                                    'j_msig_50', 'h_m_50', 'h_msig_50', 'k_m_50', 'k_msig_50', 'j_m_60', 'j_msig_60',
                                    'h_m_60', 'h_msig_60', 'k_m_60', 'k_msig_60', 'j_m_70', 'j_msig_70', 'h_m_70',
                                    'h_msig_70', 'k_m_70', 'k_msig_70', 'j_m_sys', 'j_msig_sys', 'h_m_sys',
                                    'h_msig_sys', 'k_m_sys', 'k_msig_sys', 'j_5sig_ba', 'h_5sig_ba', 'k_5sig_ba',
                                    'j_bisym_rat', 'j_bisym_chi', 'h_bisym_rat', 'h_bisym_chi', 'k_bisym_rat',
                                    'k_bisym_chi', 'j_sh0', 'j_sig_sh0', 'h_sh0', 'h_sig_sh0', 'k_sh0', 'k_sig_sh0',
                                    'j_sc_mxdn', 'j_sc_sh', 'j_sc_wsh', 'j_sc_r23', 'j_sc_1mm', 'j_sc_2mm',
                                    'j_sc_vint', 'j_sc_r1', 'j_sc_msh', 'h_sc_mxdn', 'h_sc_sh', 'h_sc_wsh',
                                    'h_sc_r23', 'h_sc_1mm', 'h_sc_2mm', 'h_sc_vint', 'h_sc_r1', 'h_sc_msh',
                                    'k_sc_mxdn', 'k_sc_sh', 'k_sc_wsh', 'k_sc_r23', 'k_sc_1mm', 'k_sc_2mm',
                                    'k_sc_vint', 'k_sc_r1', 'k_sc_msh', 'j_chif_ellf', 'k_chif_ellf',
                                    'sup_chif_ellf', 'prox', 'x_coadd', 'y_coadd', 'j_subst2', 'h_subst2',
                                    'k_subst2', 'j_back', 'h_back', 'k_back', 'j_resid_ann', 'h_resid_ann',
                                    'k_resid_ann', 'j_bndg_amp', 'h_bndg_amp', 'k_bndg_amp', 'j_seetrack',
                                    'h_seetrack', 'k_seetrack']:
                            try:
                                if doc[col] == '\\N':
                                    doc[col] = None
                                else:
                                    doc[col] = float(doc[col])
                            except Exception as e:
                                traceback.print_exc()
                                print(e)

                        doc['coordinates'] = {}
                        doc['coordinates']['epoch'] = doc['date']
                        _ra = doc['ra']
                        _dec = doc['decl']
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

                        # insert into PanSTARRS collection. don't do that! too much overhead
                        # db[_collection].insert_one(doc)
                        # insert_db_entry(db, _collection=_collection, _db_entry=doc)
                        documents.append(doc)

                        # time.sleep(1)

                        # insert batch, then flush
                        if len(documents) % batch_size == 0:
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

        # stuff left?
        if len(documents) > 0:
            insert_multiple_db_entries(db, _collection=_collection, _db_entries=documents)

    print('All done')
