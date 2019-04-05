from penquins import Kowalski
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from time import time


def yield_batch(seq, num_batches: int = 20):
    batch_size = int(np.ceil(len(seq) / num_batches))

    for nb in range(num_batches):
        yield seq[nb*batch_size: (nb+1)*batch_size]


def xmatch(_radecs, batch_size: int = 100, verbose: int = 0):
    k = Kowalski(username='USER', password='PASSWORD', verbose=False)

    num_obj = len(_radecs)

    if verbose:
        print(f'Total entries: {num_obj}')

    num_batches = int(np.ceil(num_obj / batch_size))

    times = []

    # ids = set()

    for nb in range(num_batches):
        # print(_radecs[nb * batch_size: (nb + 1) * batch_size])
        q = {"query_type": "cone_search",
             "object_coordinates": {
                 "radec": f"{_radecs[nb * batch_size: (nb + 1) * batch_size]}",
                 "cone_search_radius": "1",
                 "cone_search_unit": "arcsec"
             },
             "catalogs": {
                 "Gaia_DR2": {"filter": {},
                              "projection": {"_id": 1}
                              }
             }
             }

        tic = time()
        r = k.query(query=q)
        toc = time()
        times.append(toc - tic)
        if verbose:
            print(f'Fetching batch {nb + 1}/{num_batches} with {batch_size} sources/LCs took: {toc - tic:.3f} seconds')

        # Data are here:
        data = r['result_data']
        # TODO: your magic here
        if verbose == 2:
            print(data)
        # for sc, sources in data['Gaia_DR2'].items():
        #     ids = ids.union([s['_id'] for s in sources])
        # print(len(ids))

    if verbose:
        print(f'min: {np.min(times)}')
        print(f'median: {np.median(times)}')
        print(f'max: {np.max(times)}')


if __name__ == '__main__':

    c_path = 'kowalski/dev/cat.csv'

    # 1M sources
    # df = pd.read_csv('kowalski/dev/cat.csv', nrows=1e6)
    df = pd.read_csv('kowalski/dev/cat.csv', nrows=3e3)
    # print(df)
    ras = df.ra.values
    decs = df.dec.values

    radecs = list(zip(ras, decs))

    ''' single thread: '''
    xmatch(radecs)

    ''' multiple threads: '''
    # pool = ProcessPoolExecutor(3)
    #
    # for batch in yield_batch(radecs, num_batches=50):
    #     pool.submit(xmatch, _radecs=batch)
    #
    # # wait for everything to finish
    # pool.shutdown(wait=True)
