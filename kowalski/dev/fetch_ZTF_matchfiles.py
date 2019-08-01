import os
import requests
from bs4 import BeautifulSoup
import multiprocessing as mp
import subprocess
import json
from tqdm import tqdm


''' load config and secrets '''
with open('/app/config.json') as cjson:
    config = json.load(cjson)

with open('/app/secrets.json') as sjson:
    secrets = json.load(sjson)

for k_ in secrets:
    config[k_].update(secrets.get(k_, {}))


def fetch_url(url):
    p = os.path.join(path, os.path.basename(url))
    if not os.path.exists(p):
        subprocess.run(['wget',
                        f"--http-user={secrets['ztf_depot']['user']}", f"--http-passwd={secrets['ztf_depot']['pwd']}",
                        '-q', '--timeout=300', '--waitretry=2',
                        '--tries=10', '-O', p, url])


def gunzip(f):
    subprocess.run(['gunzip', f])


t_tag = '20190718'

path = f'/_tmp/ztf_matchfiles_{t_tag}/'
if not os.path.exists(path):
    os.makedirs(path)


if __name__ == '__main__':

    base_url = 'https://ztfweb.ipac.caltech.edu/ztf/ops/srcmatch/'

    urls = []

    print('Collecting urls of matchfiles to download:')

    # collect urls of matchfiles to download
    for rc in tqdm(range(64), total=64):
        bu = os.path.join(base_url, f'rc{rc:02d}')

        response = requests.get(bu, auth=(secrets['ztf_depot']['user'], secrets['ztf_depot']['pwd']))
        html = response.text

        # link_list = []
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.findAll('a')

        for link in links:
            txt = link.getText()
            if 'fr' in txt:
                # print(txt)

                bu_fr = os.path.join(bu, txt)

                response_fr = requests.get(bu_fr, auth=(secrets['ztf_depot']['user'], secrets['ztf_depot']['pwd']))
                html_fr = response_fr.text

                soup_fr = BeautifulSoup(html_fr, 'html.parser')
                links_fr = soup_fr.findAll('a')

                for link_fr in links_fr:
                    txt_fr = link_fr.getText()
                    if txt_fr.endswith('.pytable'):
                        # print('\t', txt_fr)
                        urls.append(os.path.join(bu_fr, txt_fr))

    n_matchfiles = len(urls)

    print(f'Downloading {n_matchfiles} matchfiles:')

    # download
    with mp.Pool(processes=4) as p:
        list(tqdm(p.imap(fetch_url, urls), total=n_matchfiles))
