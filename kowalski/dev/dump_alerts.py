from astropy.time import Time
import datetime
import json


if __name__ == '__main__':
    for y in (2018, 2019, 2020):
        for m in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12):
            jd_start = Time(datetime.datetime(y, m, 1)).jd
            if m != 12:
                jd_end = Time(datetime.datetime(y, m + 1, 1)).jd
            else:
                jd_end = Time(datetime.datetime(y + 1, 1, 1)).jd
            q = {'candidate.jd': {'$gte': jd_start, '$lt': jd_end}}
            print(y, m, jd_start, jd_end, q)
            with open(f'ZTF_alerts.{y:d}{m:02d}.dump.json', 'w') as f:
                json.dump(q, f)
