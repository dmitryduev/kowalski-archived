import schedule
import time
import os
import json
import datetime
import pytz
from astropy.time import Time
import pymongo
import tqdm
from bson.json_util import dumps
import subprocess
import argparse
import multiprocessing as mp
import numpy as np


''' load config and secrets '''
with open('/app/config.json') as cjson:
    config = json.load(cjson)

with open('/app/secrets.json') as sjson:
    secrets = json.load(sjson)

for k_ in secrets:
    config[k_].update(secrets.get(k_, {}))


def utc_now():
    return datetime.datetime.now(pytz.utc)


def time_stamps():
    """

    :return: local time, UTC time
    """
    return datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S'), \
           datetime.datetime.utcnow().strftime('%Y%m%d_%H:%M:%S')


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


def yield_batch(seq, obsdate, num_batches: int = 20):
    batch_size = int(np.ceil(len(seq) / num_batches))

    for nb in range(num_batches):
        yield seq[nb*batch_size: (nb+1)*batch_size], obsdate


def fetch_chunk(ar):
    chunk, obsdate = ar

    datestr = datetime.datetime.utcnow().strftime('%Y%m%d') if obsdate is None else obsdate
    path_date = os.path.join(config['path']['path_tess'], datestr)

    client, db = connect_to_db()
    collection_alerts = 'ZTF_alerts'

    for candid in chunk:
        try:
            alert = db[collection_alerts].find_one({'candid': candid})
            with open(os.path.join(path_date, f"{alert['candid']}.json"), 'w') as f:
                f.write(dumps(alert))
        except Exception as e:
            print(time_stamps(), str(e))


def dump_tess_parallel(obsdate=None):

    try:
        # connect to MongoDB:
        print(time_stamps(), 'Connecting to DB')
        client, db = connect_to_db()
        print(time_stamps(), 'Successfully connected')

        datestr = datetime.datetime.utcnow().strftime('%Y%m%d') if obsdate is None else obsdate

        path_date = os.path.join(config['path']['path_tess'], datestr)

        # mkdir if necessary
        if not os.path.exists(path_date):
            os.makedirs(path_date)

        jd = Time(datetime.datetime.strptime(datestr, '%Y%m%d')).jd

        collection_alerts = 'ZTF_alerts'

        # query = {'candidate.jd': {'$gt': jd, '$lt': jd + 1},
        #          'candidate.programid': 1}
        query = {'candidate.jd': {'$gt': jd, '$lt': jd + 1},
                 'candidate.programid': 1,
                 'candidate.programpi': 'TESS'}

        # index name to use:
        hint = 'candidate.jd_1_candidate.programid_1_candidate.programpi_1'

        num_doc = db[collection_alerts].count_documents(query, hint=hint)
        print(f'Alerts in TESS fields to compress: {num_doc}')

        if num_doc > 0:

            # fetch candid's:
            cursor = db[collection_alerts].find(query, {'_id': 0, 'candid': 1}).hint(hint)  # .limit(3)

            candids = []
            print("Fetching alert candid's:")
            for alert in tqdm.tqdm(cursor, total=num_doc):
                candids.append(alert['candid'])

            n_chunks = 64

            with mp.Pool(processes=4) as p:
                list(tqdm.tqdm(p.imap(fetch_chunk, yield_batch(candids, obsdate, n_chunks)), total=n_chunks))

            # cursor = db[collection_alerts].find(query).hint(hint)#.limit(3)
            #
            # # for alert in cursor.limit(1):
            # for alert in tqdm.tqdm(cursor, total=num_doc):
            #     # print(alert['candid'])
            #     try:
            #         with open(os.path.join(path_date, f"{alert['candid']}.json"), 'w') as f:
            #             f.write(dumps(alert))
            #     except Exception as e:
            #         print(time_stamps(), str(e))

            # # compress
            # print(time_stamps(), 'Compressing')
            # path_tarball_date = os.path.join(config['path']['path_tess'], f'{datestr}.tar.gz')
            # subprocess.run(['/bin/tar', '-zcf', path_tarball_date, '-C', config['path']['path_tess'], datestr])
            # print(time_stamps(), 'Finished compressing')
            #
            # # remove folder
            # print(time_stamps(), f"Removing folder: {path_date}")
            # subprocess.run(['rm', '-rf', path_date])
            # print(time_stamps(), 'Done')
            #
            # # cp to ZTF's Google Cloud Storage with gsutil
            # bucket_name = 'ztf-tess'
            # print(time_stamps(), f'Uploading to gs://{bucket_name} bucket on Google Cloud')
            # subprocess.run(['/usr/local/bin/gsutil', 'cp', path_tarball_date, f'gs://{bucket_name}/'])
            # subprocess.run(['/usr/local/bin/gsutil', 'iam', 'ch', 'allUsers:objectViewer', f'gs://{bucket_name}'])
            # print(time_stamps(), 'Done')

        else:
            print(time_stamps(), 'Nothing to do')

        print(time_stamps(), 'Disconnecting from DB')
        client.close()
        print(time_stamps(), 'Successfully disconnected')

    except Exception as e:
        print(time_stamps(), str(e))


def dump_tess(obsdate=None):

    try:
        # connect to MongoDB:
        print(time_stamps(), 'Connecting to DB')
        client, db = connect_to_db()
        print(time_stamps(), 'Successfully connected')

        datestr = datetime.datetime.utcnow().strftime('%Y%m%d') if obsdate is None else obsdate

        path_date = os.path.join(config['path']['path_tess'], datestr)

        # mkdir if necessary
        if not os.path.exists(path_date):
            os.makedirs(path_date)

        jd = Time(datetime.datetime.strptime(datestr, '%Y%m%d')).jd

        collection_alerts = 'ZTF_alerts'

        # query = {'candidate.jd': {'$gt': jd, '$lt': jd + 1},
        #          'candidate.programid': 1}
        query = {'candidate.jd': {'$gt': jd, '$lt': jd + 1},
                 'candidate.programid': 1,
                 'candidate.programpi': 'TESS'}

        # index name to use:
        hint = 'candidate.jd_1_candidate.programid_1_candidate.programpi_1'

        num_doc = db[collection_alerts].count_documents(query, hint=hint)
        print(f'Alerts in TESS fields to compress: {num_doc}')

        if num_doc > 0:

            cursor = db[collection_alerts].find(query).hint(hint)#.limit(3)

            # for alert in cursor.limit(1):
            for alert in tqdm.tqdm(cursor, total=num_doc):
                # print(alert['candid'])
                try:
                    with open(os.path.join(path_date, f"{alert['candid']}.json"), 'w') as f:
                        f.write(dumps(alert))
                except Exception as e:
                    print(time_stamps(), str(e))

            # compress
            print(time_stamps(), 'Compressing')
            path_tarball_date = os.path.join(config['path']['path_tess'], f'{datestr}.tar.gz')
            subprocess.run(['/bin/tar', '-zcf', path_tarball_date, '-C', config['path']['path_tess'], datestr])
            print(time_stamps(), 'Finished compressing')

            # remove folder
            print(time_stamps(), f"Removing folder: {path_date}")
            subprocess.run(['rm', '-rf', path_date])
            print(time_stamps(), 'Done')

            # cp to ZTF's Google Cloud Storage with gsutil
            bucket_name = 'ztf-tess'
            print(time_stamps(), f'Uploading to gs://{bucket_name} bucket on Google Cloud')
            subprocess.run(['/usr/local/bin/gsutil', 'cp', path_tarball_date, f'gs://{bucket_name}/'])
            subprocess.run(['/usr/local/bin/gsutil', 'iam', 'ch', 'allUsers:objectViewer', f'gs://{bucket_name}'])
            print(time_stamps(), 'Done')

        else:
            print(time_stamps(), 'Nothing to do')

        print(time_stamps(), 'Disconnecting from DB')
        client.close()
        print(time_stamps(), 'Successfully disconnected')

    except Exception as e:
        print(time_stamps(), str(e))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Dump ZTF alerts in TESS northern sectors to Google Cloud')
    parser.add_argument('--obsdate', help='observations date YYYYMMDD')

    args = parser.parse_args()
    obs_date = args.obsdate

    if obs_date is None:

        # schedule.every(10).seconds.do(dump_tess)
        # schedule.every().day.at("15:30").do(dump_tess)
        schedule.every().day.at("15:30").do(dump_tess_parallel)

        while True:
            schedule.run_pending()
            time.sleep(60)

    else:
        # run once for obs_date and exit
        # dump_tess(obs_date)
        dump_tess_parallel(obs_date)
