import os
import urllib.request
import re

if __name__ == '__main__':

    twomass_url = 'http://irsa.ipac.caltech.edu/2MASS/download/allsky/'

    response = urllib.request.urlopen(twomass_url)
    html = response.read().decode('utf8')
    urls = [url for url in re.findall(r'href=[\'"]?([^\'" >]+)', html) if '.gz' in url]
    # print(urls)

    path = '/_tmp/twomass'

    if not os.path.exists(path):
        os.makedirs(path)

    for url in urls:
        if not os.path.exists(os.path.join(path, url)):
            print(f'fetching {url}')
            urllib.request.urlretrieve(os.path.join(twomass_url, url), os.path.join(path, url))