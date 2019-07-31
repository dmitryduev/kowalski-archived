import os
import urllib.request
import re
import random
import string
import pathlib
import gzip
import shutil
# from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing as mp
from tqdm import tqdm
import subprocess


def check_url(_url,
              _gaia_url='http://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source/',
              _path='/_tmp/gaia_dr2'):
    if not os.path.exists(os.path.join(_path, _url)):

        # print(f'fetching {_url}')

        # generate random string
        _tmp = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(8)).lower()
        # save to tmp file:
        save_tmp = os.path.join(_path, _tmp + '.gz')
        # print(f'saving to {save_tmp}')
        urllib.request.urlretrieve(os.path.join(_gaia_url, _url), save_tmp)
        # unzip:
        with gzip.open(save_tmp, 'rb') as f_in:
            tmp_unzipped = os.path.join(_path, pathlib.Path(save_tmp).stem)
            # print(f'unzipping to {tmp_unzipped}')
            with open(tmp_unzipped, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        # unzipped exists already?
        url_unzipped = os.path.join(_path, pathlib.Path(_url).stem)
        if os.path.exists(url_unzipped):
            # check sizes
            # print(os.path.getsize(tmp_unzipped), os.path.getsize(url_unzipped))
            if os.path.getsize(tmp_unzipped) > os.path.getsize(url_unzipped):
                print(f'replace {url_unzipped}')
        else:
            print(f'fetch {url_unzipped}')
        try:
            # print(f'removing {save_tmp}')
            os.remove(save_tmp)
            # print(f'removing {tmp_unzipped}')
            os.remove(tmp_unzipped)
        finally:
            pass


def fetch_url(_url):
    _gaia_url = 'http://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source/csv'
    _path = '/_tmp/gaia_dr2'

    p = os.path.join(_path, _url)
    if not os.path.exists(p):
        subprocess.run(['wget', '-q', '-O', p, os.path.join(_gaia_url, _url)])


if __name__ == '__main__':

    gaia_url = 'http://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source/csv/'

    response = urllib.request.urlopen(gaia_url, timeout=300)
    html = response.read().decode('utf8')
    # print(html)
    urls = [url for url in re.findall(r'href=[\'"]?([^\'" >]+)', html) if '.csv.gz' in url]
    # 61234:
    print(len(urls))
    assert len(urls) == 61234, 'fetching url list failed'

    path = '/_tmp/gaia_dr2'
    if not os.path.exists(path):
        os.makedirs(path)

    with mp.Pool(processes=4) as p:
        list(tqdm(p.imap(fetch_url, urls), total=61234))

    # # init threaded operations
    # pool = ThreadPoolExecutor(50)
    # # pool = ProcessPoolExecutor(24)
    #
    # for url in urls:
    #     pool.submit(fetch_url, _url=url, _gaia_url=gaia_url, _path=path)
    #
    # # wait for everything to finish
    # pool.shutdown(wait=True)

    # for ui, url in enumerate(urls):
    #     print(ui)
    #     fetch_url(_url=url, _gaia_url=gaia_url, _path=path)
