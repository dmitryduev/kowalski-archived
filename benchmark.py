import argparse
from penquins import Kowalski
import numpy as np
import time
from concurrent.futures import ProcessPoolExecutor, as_completed


def query(username: str, password: str, num_queries: int=10, catalogs: tuple=('ZTF_alerts',)):

    # ZTF_alerts ZTF_20180919
    # catalogs = (
    #     "2MASS_PSC",
    #     "2MASS_XSC",
    #     "AMSG_20180302",
    #     "CLU_20180513",
    #     "FIRST_20141217",
    #     "Gaia_DR2",
    #     "Gaia_DR2_WD",
    #     "IPHAS_DR2",
    #     "Known_lenses_20180901",
    #     "NVSS_41",
    #     "PanSTARRS1",
    #     "RFC_2017c",
    #     "TGSS_ADR1",
    #     "TIC_7",
    #     "TNS",
    #     "ZTF_20180919"
    # )

    # with Kowalski(protocol='http', host='127.0.0.1', port=8000,
    #               username=username, password=password) as k:
    with Kowalski(protocol='https', host='kowalski.caltech.edu', port=443,
                  username=username, password=password) as k:
        qs = []
        for nq in range(num_queries):
            ra = np.random.random() * 360.0
            dec = np.random.random() * 180.0 - 90.0
            q = {"query_type": "cone_search",
                 "object_coordinates": {"radec": f"[({ra}, {dec})]",
                                        "cone_search_radius": "60",
                                        "cone_search_unit": "arcsec"},
                 "catalogs": {}}
            for catalog in catalogs:
                q["catalogs"][catalog] = {"filter": "{}", "projection": "{'_id': 1}"}
            qs.append(q)

        times = []
        for q in qs:
            tic = time.time()
            result = k.query(query=q, timeout=2)
            print(result)
            toc = time.time()
            times.append(toc-tic)

    return times


def benchmark_throughput(username, password, num_clients=1, num_queries=10):

    pool = ProcessPoolExecutor(num_clients)

    futures = []
    for ff in range(num_clients):
        futures.append(pool.submit(query, username=username, password=password, num_queries=num_queries))

    # wait for everything to finish
    # pool.shutdown(wait=True)

    for x in as_completed(futures):
        print(x.result())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Benchmark Kowalski')
    parser.add_argument('-u', help='username', default='USERNAME')
    parser.add_argument('-p', help='password', default='PASSWORD')

    args = parser.parse_args()
    print(args)

    benchmark_throughput(args.u, args.p)
