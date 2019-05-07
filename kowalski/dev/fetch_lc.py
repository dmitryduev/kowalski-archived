from penquins import Kowalski
from time import time
import numpy as np


if __name__ == '__main__':
    k = Kowalski(username='USER', password='PASSWORD', verbose=False)

    batch_size = 100
    num_batches = 100
    # batch_size = 10
    # num_batches = 100

    times = []

    for nb in range(num_batches):
        qu = {"query_type": "general_search",
              "query": "db['ZTF_sources_20190412'].find({}, " +
                       "{'_id': 1, 'data.programid': 1, 'data.hjd': 1, " +
                       f"'data.mag': 1, 'data.magerr': 1}}).skip({nb*batch_size}).limit({batch_size})"
              }

        # print(qu)
        tic = time()
        r = k.query(query=qu)
        toc = time()
        times.append(toc-tic)
        print(f'Fetching batch {nb+1}/{num_batches} with {batch_size} sources/LCs took: {toc-tic:.3f} seconds')

        # Light curves are here:
        # print(r['result_data']['query_result'])
        # Must filter out data.programid == 1 data

    print(f'min: {np.min(times)}')
    print(f'median: {np.median(times)}')
    print(f'max: {np.max(times)}')
