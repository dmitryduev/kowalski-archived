from penquins import Kowalski
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from time import time


def yield_batch(seq, num_batches: int = 20):
    batch_size = int(np.ceil(len(seq) / num_batches))

    for nb in range(num_batches):
        yield seq[nb*batch_size: (nb+1)*batch_size]


def fetch_lc_radecs(_radecs):
    k = Kowalski(username='USER', password='PASSWORD', verbose=False)

    num_obj = len(_radecs)

    print(f'Total entries: {num_obj}')

    batch_size = 100
    num_batches = int(np.ceil(num_obj / batch_size))

    times = []

    ids = set()

    for nb in range(num_batches):
        # print(_radecs[nb * batch_size: (nb + 1) * batch_size])
        q = {"query_type": "cone_search",
             "object_coordinates": {
                 "radec": f"{_radecs[nb * batch_size: (nb + 1) * batch_size]}",
                 "cone_search_radius": "2",
                 "cone_search_unit": "arcsec"
             },
             "catalogs": {
                 "ZTF_sources_20190412": {"filter": {},
                                          "projection": {"_id": 1,
                                                         "filter": 1,
                                                         "data.expid": 1,
                                                         "data.ra": 1,
                                                         "data.dec": 1,
                                                         "data.programid": 1,
                                                         "data.hjd": 1,
                                                         "data.mag": 1,
                                                         "data.magerr": 1
                                                         }}
             }
             }

        tic = time()
        r = k.query(query=q)
        toc = time()
        times.append(toc - tic)
        print(f'Fetching batch {nb + 1}/{num_batches} with {batch_size} sources/LCs took: {toc - tic:.3f} seconds')

        # Light curves are here:
        data = r['result_data']
        # TODO: your magic here
        # print(data)
        for sc, sources in data['ZTF_sources_20181220'].items():
            ids = ids.union([s['_id'] for s in sources])
        print(len(ids))
        # FIXME: Must filter out data.programid == 1 data

    print(f'min: {np.min(times)}')
    print(f'median: {np.median(times)}')
    print(f'max: {np.max(times)}')


if __name__ == '__main__':

    # c_path = '/Users/dmitryduev/_caltech/python/kowalski/kowalski/dev/agn.txt'
    c_path = '/Users/dmitryduev/_caltech/python/kowalski/kowalski/dev/liner'

    # df = pd.read_csv(c_path, sep='\t', header=None, names=['id', 'ra', 'dec'])
    # df = pd.read_fwf(c_path, header=None, names=['id', 'ra', 'dec'])
    # print(df)
    # ras = df.ra.values
    # decs = df.dec.values
    #
    # radecs = list(zip(ras, decs))

    with open(c_path) as f:
        radecs = [tuple(map(float, l.split()[1:])) for l in f.readlines()]

    ''' single thread: '''
    fetch_lc_radecs(radecs)

    ''' multiple threads: '''
    pool = ProcessPoolExecutor(20)

    for batch in yield_batch(radecs, num_batches=50):
        pool.submit(fetch_lc_radecs, _radecs=batch)

    # wait for everything to finish
    pool.shutdown(wait=True)
