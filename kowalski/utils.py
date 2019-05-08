import hashlib
import random
import string

import math
import numpy as np
import pandas as pd
import datetime
import pytz
import base64
import bcrypt
from bson.json_util import dumps

from typing import Union


array_type = Union[list, tuple, set, np.ndarray]


def is_array(arr):
    if isinstance(arr, list) or isinstance(arr, tuple) or isinstance(arr, set) or isinstance(arr, np.ndarray):
        return True
    else:
        return False


def generate_password_hash(password, salt_rounds=12):
    password_bin = password.encode('utf-8')
    hashed = bcrypt.hashpw(password_bin, bcrypt.gensalt(salt_rounds))
    encoded = base64.b64encode(hashed)
    return encoded.decode('utf-8')


def check_password_hash(encoded, password):
    password = password.encode('utf-8')
    encoded = encoded.encode('utf-8')

    hashed = base64.b64decode(encoded)
    is_correct = bcrypt.hashpw(password, hashed) == hashed
    return is_correct


def to_pretty_json(value):
    # return dumps(value, indent=4)  # , separators=(',', ': ')
    return dumps(value, separators=(',', ': '))


def radec_str2rad(_ra_str, _dec_str):
    """
    :param _ra_str: 'H:M:S'
    :param _dec_str: 'D:M:S'
    :return: ra, dec in rad
    """
    # convert to rad:
    _ra = list(map(float, _ra_str.split(':')))
    _ra = (_ra[0] + _ra[1] / 60.0 + _ra[2] / 3600.0) * np.pi / 12.
    _dec = list(map(float, _dec_str.split(':')))
    _sign = -1 if _dec_str.strip()[0] == '-' else 1
    _dec = _sign * (abs(_dec[0]) + abs(_dec[1]) / 60.0 + abs(_dec[2]) / 3600.0) * np.pi / 180.

    return _ra, _dec


def radec_str2geojson(ra_str, dec_str):

    # hms -> ::, dms -> ::
    if isinstance(ra_str, str) and isinstance(dec_str, str):
        if ('h' in ra_str) and ('m' in ra_str) and ('s' in ra_str):
            ra_str = ra_str[:-1]  # strip 's' at the end
            for char in ('h', 'm'):
                ra_str = ra_str.replace(char, ':')
        if ('d' in dec_str) and ('m' in dec_str) and ('s' in dec_str):
            dec_str = dec_str[:-1]  # strip 's' at the end
            for char in ('d', 'm'):
                dec_str = dec_str.replace(char, ':')

        if (':' in ra_str) and (':' in dec_str):
            ra, dec = radec_str2rad(ra_str, dec_str)
            # convert to geojson-friendly degrees:
            ra = ra * 180.0 / np.pi - 180.0
            dec = dec * 180.0 / np.pi
        else:
            raise Exception('Unrecognized string ra/dec format.')
    else:
        # already in degrees?
        ra = float(ra_str)
        # geojson-friendly ra:
        ra -= 180.0
        dec = float(dec_str)

    return ra, dec


def utc_now():
    return datetime.datetime.now(pytz.utc)


def jd(_t):
    """
    Calculate Julian Date
    """
    assert isinstance(_t, datetime.datetime), 'function argument must be a datetime.datetime instance'

    a = np.floor((14 - _t.month) / 12)
    y = _t.year + 4800 - a
    m = _t.month + 12 * a - 3

    jdn = _t.day + np.floor((153 * m + 2) / 5.) + 365 * y + np.floor(y / 4.) - np.floor(y / 100.) + np.floor(
        y / 400.) - 32045

    _jd = jdn + (_t.hour - 12.) / 24. + _t.minute / 1440. + _t.second / 86400. + _t.microsecond / 86400000000.

    return _jd


def mjd(_t):
    """
    Calculate Modified Julian Date
    """
    assert isinstance(_t, datetime.datetime), 'function argument must be a datetime.datetime instance'
    _jd = jd(_t)
    _mjd = _jd - 2400000.5
    return _mjd


def days_to_hmsm(days):
    """
    Convert fractional days to hours, minutes, seconds, and microseconds.
    Precision beyond microseconds is rounded to the nearest microsecond.

    Parameters
    ----------
    days : float
        A fractional number of days. Must be less than 1.

    Returns
    -------
    hour : int
        Hour number.

    min : int
        Minute number.

    sec : int
        Second number.

    micro : int
        Microsecond number.

    Raises
    ------
    ValueError
        If `days` is >= 1.

    Examples
    --------
    >>> days_to_hmsm(0.1)
    (2, 24, 0, 0)

    """
    hours = days * 24.
    hours, hour = math.modf(hours)

    mins = hours * 60.
    mins, min = math.modf(mins)

    secs = mins * 60.
    secs, sec = math.modf(secs)

    micro = round(secs * 1.e6)

    return int(hour), int(min), int(sec), int(micro)


def jd_to_date(jd):
    """
    Convert Julian Day to date.

    Algorithm from 'Practical Astronomy with your Calculator or Spreadsheet',
        4th ed., Duffet-Smith and Zwart, 2011.

    Parameters
    ----------
    jd : float
        Julian Day

    Returns
    -------
    year : int
        Year as integer. Years preceding 1 A.D. should be 0 or negative.
        The year before 1 A.D. is 0, 10 B.C. is year -9.

    month : int
        Month as integer, Jan = 1, Feb. = 2, etc.

    day : float
        Day, may contain fractional part.

    Examples
    --------
    Convert Julian Day 2446113.75 to year, month, and day.

    >>> jd_to_date(2446113.75)
    (1985, 2, 17.25)

    """
    jd = jd + 0.5

    F, I = math.modf(jd)
    I = int(I)

    A = math.trunc((I - 1867216.25) / 36524.25)

    if I > 2299160:
        B = I + 1 + A - math.trunc(A / 4.)
    else:
        B = I

    C = B + 1524

    D = math.trunc((C - 122.1) / 365.25)

    E = math.trunc(365.25 * D)

    G = math.trunc((C - E) / 30.6001)

    day = C - E + F - math.trunc(30.6001 * G)

    if G < 13.5:
        month = G - 1
    else:
        month = G - 13

    if month > 2.5:
        year = D - 4716
    else:
        year = D - 4715

    return year, month, day


def jd_to_datetime(_jd):
    """
    Convert a Julian Day to an `jdutil.datetime` object.

    Parameters
    ----------
    jd : float
        Julian day.

    Returns
    -------
    dt : `jdutil.datetime` object
        `jdutil.datetime` equivalent of Julian day.

    Examples
    --------
    >>> jd_to_datetime(2446113.75)
    datetime(1985, 2, 17, 6, 0)

    """
    year, month, day = jd_to_date(_jd)

    frac_days, day = math.modf(day)
    day = int(day)

    hour, min_, sec, micro = days_to_hmsm(frac_days)

    return datetime.datetime(year, month, day, hour, min_, sec, micro)


def mjd_to_datetime(_mjd):
    _jd = _mjd + 2400000.5

    return jd_to_datetime(_jd)


def compute_hash(_task):
    """
        Compute hash for a hashable task
    :return:
    """
    ht = hashlib.blake2b(digest_size=16)
    ht.update(_task.encode('utf-8'))
    hsh = ht.hexdigest()

    return hsh


def random_alphanumeric_str(length: int = 8):
    return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(length)).lower()


''' ZTF Alert Light Curve helpers '''


def make_dataframe(packets):
    if is_array(packets):
        dfs = []
        for packet in packets:
            df = pd.DataFrame(packet['candidate'], index=[0])
            df_prv = pd.DataFrame(packet['prv_candidates'])
            dfs.append(df)
            dfs.append(df_prv)
        # drop duplicate entries. decide using jd
        return pd.concat(dfs, ignore_index=True, sort=False).drop_duplicates(subset='jd',
                                                                             inplace=True).reset_index(drop=True)
    else:
        # single packet
        df = pd.DataFrame(packets['candidate'], index=[0])
        df_prv = pd.DataFrame(packets['prv_candidates'])
        return pd.concat([df, df_prv], ignore_index=True, sort=False)


def is_star(dflc, match_radius_arcsec=1.5, star_galaxy_threshold=0.4):
    try:
        return (dflc.iloc[-1].distpsnr1 < match_radius_arcsec) & (dflc.iloc[-1].sgscore1 > star_galaxy_threshold)
    except Exception as _e:
        print(_e)
        return False


def ccd_quad_2_rc(ccd: int, quad: int) -> int:
    # assert ccd in range(1, 17)
    # assert quad in range(1, 5)
    b = (ccd - 1) * 4
    rc = b + quad - 1
    return rc


# filters = {'zg': 1, 'zr': 2, 'zi': 3}
