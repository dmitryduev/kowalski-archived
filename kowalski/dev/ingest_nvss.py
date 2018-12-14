from astropy.io import fits
import numpy as np
import pymongo
import json
import argparse
import traceback
import datetime
import pytz
from numba import jit


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


if __name__ == '__main__':
    ''' Create command line argument parser '''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description='')

    args = parser.parse_args()

    # connect to MongoDB:
    print('Connecting to DB')
    client, db = connect_to_db()
    print('Successfully connected')

    _collection = 'NVSS_41'

    # number of records to insert
    batch_size = 2048
    batch_num = 1
    documents = []

    cat_fits = '/_tmp/nvss/CATALOG41.FIT'

    with fits.open(cat_fits) as hdu:
        print(hdu.info())
        print(repr(hdu[0].header))
        print('\n\n')
        print(repr(hdu[1].header))

        field_names_orig = hdu[1].columns.names
        field_names = hdu[1].columns.names
        field_names = [fn.replace(' ', '_') for fn in field_names]
        field_names[0] = 'RA'
        field_names[1] = 'DEC'
        field_data_types = []

        for fi, ff in enumerate(field_names):
            ftype = hdu[1].header[f'TFORM{fi+1}'].strip()
            # print(ftype)
            if 'E' in ftype or 'D' in ftype:
                field_data_types.append(float)
            if 'I' in ftype or 'J' in ftype or 'K' in ftype:
                field_data_types.append(int)
            elif 'A' in ftype:
                field_data_types.append(str)

        total = len(hdu[1].data)
        print(field_names)
        print(field_data_types)
        # print({k: v for k, v in zip(field_names, field_data_types)})
        # input()

        for ci, cf in enumerate(hdu[1].data):
            print(f'processing entry #{ci+1} of {total}')

            try:
                # print(cf)
                doc = {k: v for k, v in zip(field_names, cf)}
                # print(cf)

                for ii, kk in enumerate(field_names):
                    if doc[kk] == 'N/A':
                        doc[kk] = None
                    else:
                        if field_data_types[ii] in (float, int):
                            doc[kk] = field_data_types[ii](doc[kk])
                        else:
                            doc[kk] = str(doc[kk]).strip()

                # doc['_id'] = doc['CLUID']

                # GeoJSON for 2D indexing
                doc['coordinates'] = {}
                # doc['coordinates']['epoch'] = doc['jd']
                _ra = doc['RA']
                _dec = doc['DEC']
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
                # print(doc)
                # input()

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

        # stuff left from the last file?
        if len(documents) > 0:
            print(f'inserting batch #{batch_num}')
            insert_multiple_db_entries(db, _collection=_collection, _db_entries=documents)

        # create 2d index:
        print('Creating 2d index')
        db[_collection].create_index([('coordinates.radec_geojson', '2dsphere'),
                                      ('_id', pymongo.ASCENDING)], background=True)

        print('All done')
