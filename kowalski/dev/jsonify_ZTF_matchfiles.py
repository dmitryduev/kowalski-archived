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
from bson.json_util import dumps
import argparse
import traceback
import datetime
import pytz
from numba import jit
import typing
# from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor

from json import encoder
encoder.FLOAT_REPR = lambda o: format(o, '.7f')


def utc_now():
    return datetime.datetime.now(pytz.utc)


@jit
def deg2hms(x):
    """
    Transform degrees to *hours:minutes:seconds* strings.

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


filters = {'zg': 1, 'zr': 2, 'zi': 3}


def process_file(_file, _keep_all=False, _rm_file=False, verbose=False):

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
            _, field, filt, ccd, quad, _ = ff_basename.split('_')
            field = int(field)
            filt = filters[filt]
            ccd = int(ccd[1:])
            quad = int(quad[1:])

            rc = ccd_quad_2_rc(ccd=ccd, quad=quad)
            baseid = int(1e13 + field * 1e9 + rc * 1e7 + filt * 1e6)
            # print(f'{_file}: {field} {filt} {ccd} {quad}')
            print(f'{_file}: baseid {baseid}')

            exp_baseid = int(1e16 + field * 1e12 + rc * 1e10 + filt * 1e9)
            # print(int(1e16), int(field*1e12), int(rc*1e10), int(filt*1e9), exp_baseid)

            exposures = pd.DataFrame.from_records(group.exposures[:])
            # exposures_colnames = exposures.columns.values
            # print(exposures_colnames)

            # prepare docs to ingest into db:
            docs_exposures = []
            for index, row in exposures.iterrows():
                try:
                    doc = row.to_dict()

                    # unique exposure id:
                    doc['_id'] = exp_baseid + doc['expid']
                    # print(exp_baseid, doc['expid'], doc['_id'])

                    doc['matchfile'] = ff_basename
                    doc['filter'] = filt
                    doc['field'] = field
                    doc['ccd'] = ccd
                    doc['quad'] = quad
                    doc['rc'] = rc
                    # pprint(doc)
                    docs_exposures.append(doc)
                except Exception as e_:
                    print(str(e_))

            docs_sources = []
            # fixme? skip transients
            # for source_type in ('source', 'transient'):
            for source_type in ('source', ):

                sources_colnames = group[f'{source_type}s'].colnames
                sources = np.array(group[f'{source_type}s'].read())
                # sources = group[f'{source_type}s'].read()

                # sourcedata = pd.DataFrame.from_records(group[f'{source_type}data'][:])
                # sourcedata_colnames = sourcedata.columns.values
                sourcedata_colnames = group[f'{source_type}data'].colnames
                # sourcedata = np.array(group[f'{source_type}data'].read())

                for source in sources:
                    try:
                        doc = dict(zip(sources_colnames, source))

                        # grab data first
                        sourcedata = np.array(group[f'{source_type}data'].read_where(f'matchid == {doc["matchid"]}'))
                        # print(sourcedata)
                        doc_data = [dict(zip(sourcedata_colnames, sd)) for sd in sourcedata]

                        # skip sources that are only detected in the reference image:
                        if len(doc_data) == 0:
                            continue

                        # dump unwanted fields:
                        if not _keep_all:
                            # refmagerr = 1.0857/refsnr
                            sources_fields_to_keep = ('meanmag',
                                                      'percentiles',
                                                      'vonneumannratio',
                                                      'dec', 'matchid', 'nobs',
                                                      'ra', 'refchi', 'refmag', 'refmagerr', 'refsharp')

                            doc_keys = list(doc.keys())
                            for kk in doc_keys:
                                if kk not in sources_fields_to_keep:
                                    doc.pop(kk)

                        # convert types for pymongo:
                        for k, v in doc.items():
                            # types.add(type(v))
                            if np.issubdtype(type(v), np.integer):
                                doc[k] = int(doc[k])
                            if np.issubdtype(type(v), np.inexact):
                                doc[k] = float(doc[k])
                                if k not in ('ra', 'dec'):
                                    doc[k] = round(doc[k], 3)
                            # convert numpy arrays into lists
                            if type(v) == np.ndarray:
                                doc[k] = doc[k].tolist()

                        # generate unique _id:
                        doc['_id'] = baseid + doc['matchid']

                        # from Frank Masci: compute ObjectID, same as serial key in ZTF Objects DB table in IRSA.
                        # oid = ((fieldid * 100000 + fid * 10000 + ccdid * 100 + qid * 10) * 10 ** 7) + int(matchid)

                        doc['iqr'] = doc['percentiles'][8] - doc['percentiles'][3]
                        doc['iqr'] = round(doc['iqr'], 3)
                        doc.pop('percentiles')

                        # doc['matchfile'] = ff_basename
                        doc['filter'] = filt
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
                        # doc['coordinates']['radec_rad'] = [_ra * np.pi / 180.0, _dec * np.pi / 180.0]
                        # doc['coordinates']['radec_deg'] = [_ra, _dec]

                        # data

                        doc['data'] = doc_data
                        # print(doc['data'])

                        if not _keep_all:
                            # do not store all fields to save space
                            if len(doc_data) > 0:
                                # magerr = 1.0857/snr
                                sourcedata_fields_to_keep = ('catflags', 'chi', 'dec', 'expid', 'hjd',
                                                             'mag', 'magerr', 'programid',
                                                             'ra',  # 'relphotflags', 'snr',
                                                             'sharp')
                                doc_keys = list(doc_data[0].keys())
                                for ddi, ddp in enumerate(doc['data']):
                                    for kk in doc_keys:
                                        if kk not in sourcedata_fields_to_keep:
                                            doc['data'][ddi].pop(kk)

                        for dd in doc['data']:
                            # convert types for pymongo:
                            for k, v in dd.items():
                                # types.add(type(v))
                                if np.issubdtype(type(v), np.integer):
                                    dd[k] = int(dd[k])
                                if np.issubdtype(type(v), np.inexact):
                                    dd[k] = float(dd[k])
                                    if k not in ('ra', 'dec', 'hjd'):
                                        dd[k] = round(dd[k], 3)
                                    elif k == 'hjd':
                                        dd[k] = round(dd[k], 5)
                                # convert numpy arrays into lists
                                if type(v) == np.ndarray:
                                    dd[k] = dd[k].tolist()

                            # generate unique exposure id's that match _id's in exposures collection
                            dd['uexpid'] = exp_baseid + dd['expid']

                        # pprint(doc)
                        docs_sources.append(doc)

                    except Exception as e_:
                        print(str(e_))

        # dump to json:
        with open(_file.replace('.pytable', '.exposures.json'), 'w') as fe:
            fe.write(dumps(docs_exposures))
        with open(_file.replace('.pytable', '.sources.json'), 'w') as fs:
            # fs.write(dumps(docs_sources))
            json.dump(docs_sources, fs)

    except Exception as e:
        traceback.print_exc()
        print(e)


if __name__ == '__main__':
    ''' Create command line argument parser '''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description='')

    parser.add_argument('--keepall', action='store_true', help='keep all fields from the matchfiles?')
    parser.add_argument('--rm', action='store_true', help='remove matchfiles after ingestion?')

    args = parser.parse_args()

    keep_all = args.keepall
    rm_file = args.rm

    # _location = f'/_tmp/ztf_matchfiles_{t_tag}/'
    _location = '/Users/dmitryduev/_caltech/python/kowalski/kowalski/dev'
    files = glob.glob(os.path.join(_location, 'ztf_*.pytable'))

    print(f'# files to process: {len(files)}')

    # init threaded operations
    # pool = ProcessPoolExecutor(1)

    # for ff in files[::-1]:
    for ff in sorted(files):
        process_file(_file=ff, _keep_all=keep_all, _rm_file=rm_file, verbose=True)
        # pool.submit(process_file, _file=ff, _keep_all=keep_all, _rm_file=rm_file, verbose=True)

    # wait for everything to finish
    # pool.shutdown(wait=True)

    print('All done')
