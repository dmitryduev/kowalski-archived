import os
import urllib.request
import re
import random
import string
import pathlib
import gzip
import shutil
import requests
from bs4 import BeautifulSoup
import multiprocessing as mp
from tqdm import tqdm
import subprocess
import json


''' load config and secrets '''
with open('/app/config.json') as cjson:
    config = json.load(cjson)

with open('/app/secrets.json') as sjson:
    secrets = json.load(sjson)

for k_ in secrets:
    config[k_].update(secrets.get(k_, {}))


def fetch_url(_url):
    _gaia_url = 'http://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source/csv'
    _path = '/_tmp/gaia_dr2'

    p = os.path.join(_path, _url)
    if not os.path.exists(p):
        subprocess.run(['wget', '-q', '--timeout=5', '--waitretry=2',
                        '--tries=10', '-O', p, os.path.join(_gaia_url, _url)])


def gunzip(f):
    subprocess.run(['gunzip', f])


if __name__ == '__main__':

    t_tag = '20190718'

    base_url = 'https://ztfweb.ipac.caltech.edu/ztf/ops/srcmatch/'

    # response = urllib.request.urlopen(gaia_url, timeout=300)
    # html = response.read().decode('utf8')
    # # print(html)
    # urls = [url for url in re.findall(r'href=[\'"]?([^\'" >]+)', html) if '.csv.gz' in url]
    # # 61234:
    # print(len(urls))
    # assert len(urls) == 61234, 'fetching url list failed'

    path = f'/_tmp/ztf_matchfiles_{t_tag}/'
    if not os.path.exists(path):
        os.makedirs(path)

    urls = []

    for rc in range(1):
        bu = os.path.join(base_url, f'rc{rc:02d}')

        response = requests.get(bu, auth=(secrets['ztf_depot']['user'], secrets['ztf_depot']['pwd']))
        html = response.text

        # link_list = []
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.findAll('a')

        for link in links:
            txt = link.getText()
            print(txt)

    # download
    # with mp.Pool(processes=10) as p:
    #     list(tqdm(p.imap(fetch_url, urls), total=61234))
