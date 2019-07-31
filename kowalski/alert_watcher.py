import argparse
import os
import sys
import io
import time
import json
from bson.json_util import dumps
import traceback

import confluent_kafka
from ast import literal_eval
import avro.schema
import fastavro
import subprocess
import datetime
import multiprocessing
# import threading

import pymongo
import pytz
from numba import jit
import numpy as np

from tensorflow.keras.models import load_model
import gzip
import io
from astropy.io import fits


''' load config and secrets '''
with open('/app/config.json') as cjson:
    config = json.load(cjson)

with open('/app/secrets.json') as sjson:
    secrets = json.load(sjson)

for k in secrets:
    config[k].update(secrets.get(k, {}))


def utc_now():
    return datetime.datetime.now(pytz.utc)


def time_stamps():
    """

    :return: local time, UTC time
    """
    return datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S'), \
           datetime.datetime.utcnow().strftime('%Y%m%d_%H:%M:%S')


@jit
def deg2hms(x):
    """Transform degrees to *hours:minutes:seconds* strings.

    Parameters
    ----------
    x : float
        The degree value c [0, 360) to be written as a sexagesimal string.

    Returns
    -------
    out : str
        The input angle written as a sexagesimal string, in the
        form, hours:minutes:seconds.

    """
    assert 0.0 <= x < 360.0, 'Bad RA value in degrees'
    # ac = Angle(x, unit='degree')
    # hms = str(ac.to_string(unit='hour', sep=':', pad=True))
    # print(str(hms))
    _h = np.floor(x * 12.0 / 180.)
    _m = np.floor((x * 12.0 / 180. - _h) * 60.0)
    _s = ((x * 12.0 / 180. - _h) * 60.0 - _m) * 60.0
    hms = '{:02.0f}:{:02.0f}:{:07.4f}'.format(_h, _m, _s)
    # print(hms)
    return hms


@jit
def deg2dms(x):
    """Transform degrees to *degrees:arcminutes:arcseconds* strings.

    Parameters
    ----------
    x : float
        The degree value c [-90, 90] to be converted.

    Returns
    -------
    out : str
        The input angle as a string, written as degrees:minutes:seconds.

    """
    assert -90.0 <= x <= 90.0, 'Bad Dec value in degrees'
    # ac = Angle(x, unit='degree')
    # dms = str(ac.to_string(unit='degree', sep=':', pad=True))
    # print(dms)
    _d = np.floor(abs(x)) * np.sign(x)
    _m = np.floor(np.abs(x - _d) * 60.0)
    _s = np.abs(np.abs(x - _d) * 60.0 - _m) * 60.0
    dms = '{:02.0f}:{:02.0f}:{:06.3f}'.format(_d, _m, _s)
    # print(dms)
    return dms


"""Utilities for manipulating Avro data and schemas.
"""


def _loadSingleAvsc(file_path, names):
    """Load a single avsc file.
    """
    with open(file_path) as file_text:
        json_data = json.load(file_text)
    schema = avro.schema.SchemaFromJSONData(json_data, names)
    return schema


def combineSchemas(schema_files):
    """Combine multiple nested schemas into a single schema.

    Parameters
    ----------
    schema_files : `list`
        List of files containing schemas.
        If nested, most internal schema must be first.

    Returns
    -------
    `dict`
        Avro schema
    """
    known_schemas = avro.schema.Names()

    for s in schema_files:
        schema = _loadSingleAvsc(s, known_schemas)
    return schema.to_json()


def writeAvroData(json_data, json_schema):
    """Encode json into Avro format given a schema.

    Parameters
    ----------
    json_data : `dict`
        The JSON data containing message content.
    json_schema : `dict`
        The writer Avro schema for encoding data.

    Returns
    -------
    `_io.BytesIO`
        Encoded data.
    """
    bytes_io = io.BytesIO()
    fastavro.schemaless_writer(bytes_io, json_schema, json_data)
    return bytes_io


def readAvroData(bytes_io, json_schema):
    """Read data and decode with a given Avro schema.

    Parameters
    ----------
    bytes_io : `_io.BytesIO`
        Data to be decoded.
    json_schema : `dict`
        The reader Avro schema for decoding data.

    Returns
    -------
    `dict`
        Decoded data.
    """
    bytes_io.seek(0)
    message = fastavro.schemaless_reader(bytes_io, json_schema)
    return message


def readSchemaData(bytes_io):
    """Read data that already has an Avro schema.

    Parameters
    ----------
    bytes_io : `_io.BytesIO`
        Data to be decoded.

    Returns
    -------
    `dict`
        Decoded data.
    """
    bytes_io.seek(0)
    message = fastavro.reader(bytes_io)
    return message


class AlertError(Exception):
    """Base class for exceptions in this module.
    """
    pass


class EopError(AlertError):
    """Exception raised when reaching end of partition.

    Parameters
    ----------
    msg : Kafka message
        The Kafka message result from consumer.poll().
    """
    def __init__(self, msg):
        message = 'topic:%s, partition:%d, status:end, ' \
                  'offset:%d, key:%s, time:%.3f\n' \
                  % (msg.topic(), msg.partition(),
                     msg.offset(), str(msg.key()), time.time())
        self.message = message

    def __str__(self):
        return self.message


class AlertConsumer(object):
    """Creates an alert stream Kafka consumer for a given topic.

    Parameters
    ----------
    topic : `str`
        Name of the topic to subscribe to.
    schema_files : Avro schema files
        The reader Avro schema files for decoding data. Optional.
    **kwargs
        Keyword arguments for configuring confluent_kafka.Consumer().
    """

    def __init__(self, topic, schema_files=None, **kwargs):

        # keep track of disconnected partitions
        self.num_disconnected_partitions = 0
        self.topic = topic

        def error_cb(err, _self=self):
            print(*time_stamps(), 'error_cb -------->', err)
            # print(err.code())
            if err.code() == -195:
                _self.num_disconnected_partitions += 1
                if _self.num_disconnected_partitions == _self.num_partitions:
                    print(*time_stamps(), 'all partitions got disconnected, killing thread')
                    sys.exit()
                else:
                    print(*time_stamps(), '{:s}: disconnected from partition.'.format(_self.topic),
                          'total:', self.num_disconnected_partitions)

        # 'error_cb': error_cb
        kwargs['error_cb'] = error_cb

        self.consumer = confluent_kafka.Consumer(**kwargs)
        self.num_partitions = 0

        def on_assign(consumer, partitions, _self=self):
            # force-reset offsets when subscribing to a topic:
            for part in partitions:
                # -2 stands for beginning and -1 for end
                part.offset = -2
                # keep number of partitions. when reaching  end of last partition, kill thread and start from beginning
                _self.num_partitions += 1
                print(consumer.get_watermark_offsets(part))

        self.consumer.subscribe([topic], on_assign=on_assign)
        # self.consumer.subscribe([topic])

        if schema_files is not None:
            self.alert_schema = combineSchemas(schema_files)

        # MongoDB:
        self.config = config
        # self.collection_alerts = 'ZTF_alerts'
        self.collection_alerts = 'ZTF_alerts2'
        self.collection_alert_cross_matches = 'ZTF_alerts_cross_match'
        self.db = None
        self.connect_to_db()

        # indexes
        self.db['db'][self.collection_alerts].create_index([('coordinates.radec_geojson', '2dsphere'),
                                                            ('candid', pymongo.DESCENDING)], background=True)
        self.db['db'][self.collection_alerts].create_index([('coordinates.radec_geojson', '2dsphere'),
                                                            ('objectId', pymongo.DESCENDING)], background=True)
        self.db['db'][self.collection_alerts].create_index([('objectId', pymongo.ASCENDING)], background=True)
        self.db['db'][self.collection_alerts].create_index([('candid', pymongo.ASCENDING)], background=True)
        self.db['db'][self.collection_alerts].create_index([('candidate.pid', pymongo.ASCENDING)], background=True)
        self.db['db'][self.collection_alerts].create_index([('objectId', pymongo.DESCENDING),
                                                            ('candidate.pid', pymongo.ASCENDING)], background=True)
        self.db['db'][self.collection_alerts].create_index([('candidate.pdiffimfilename', pymongo.ASCENDING)],
                                                           background=True)
        self.db['db'][self.collection_alerts].create_index([('candidate.jd', pymongo.ASCENDING),
                                                            ('candidate.programid', pymongo.ASCENDING),
                                                            ('candidate.programpi', pymongo.ASCENDING)],
                                                           background=True)
        self.db['db'][self.collection_alerts].create_index([('candidate.jd', pymongo.DESCENDING),
                                                            ('classifications.braai', pymongo.DESCENDING),
                                                            ('candid', pymongo.DESCENDING)],
                                                           background=True)
        self.db['db'][self.collection_alerts].create_index([('candidate.jd', 1),
                                                            ('candidate.field', 1),
                                                            ('candidate.rb', 1),
                                                            ('candidate.drb', 1),
                                                            ('classifications.braai', 1),
                                                            ('candidate.ndethist', 1),
                                                            ('candidate.magpsf', 1),
                                                            ('candidate.isdiffpos', 1),
                                                            ('objectId', 1)],
                                                           name='jd_field_rb_drb_braai_ndethhist_magpsf_isdiffpos',
                                                           background=True)

        # ML models:
        self.ml_models = dict()
        for m in config['ml_models']:
            try:
                m_v = config["ml_models"][m]["version"]
                self.ml_models[m] = {'model': load_model(f'/app/models/{m}_{m_v}.h5'),
                                     'version': m_v}
            except Exception as e:
                print(*time_stamps(), f'Error loading ML model {m}')
                traceback.print_exc()
                print(e)
                continue

    def connect_to_db(self):
        """
            Connect to mongo
        :return:
        """

        _config = self.config

        try:
            # there's only one instance of DB, it's too big to be replicated
            _client = pymongo.MongoClient(host=_config['database']['host'],
                                          port=_config['database']['port'], connect=False)
            # grab main database:
            _db = _client[_config['database']['db']]
        except Exception as _e:
            raise ConnectionRefusedError
        try:
            # authenticate
            _db.authenticate(_config['database']['user'], _config['database']['pwd'])
        except Exception as _e:
            raise ConnectionRefusedError

        self.db = dict()
        self.db['client'] = _client
        self.db['db'] = _db

    def insert_db_entry(self, _collection=None, _db_entry=None):
        """
            Insert a document _doc to collection _collection in DB.
            It is monitored for timeout in case DB connection hangs for some reason
        :param _collection:
        :param _db_entry:
        :return:
        """
        assert _collection is not None, 'Must specify collection'
        assert _db_entry is not None, 'Must specify document'
        try:
            self.db['db'][_collection].insert_one(_db_entry)
        except Exception as _e:
            print(*time_stamps(), 'Error inserting {:s} into {:s}'.format(str(_db_entry['_id']), _collection))
            traceback.print_exc()
            print(_e)

    def insert_multiple_db_entries(self, _collection=None, _db_entries=None):
        """
            Insert a document _doc to collection _collection in DB.
            It is monitored for timeout in case DB connection hangs for some reason
        :param _db:
        :param _collection:
        :param _db_entries:
        :return:
        """
        assert _collection is not None, 'Must specify collection'
        assert _db_entries is not None, 'Must specify documents'
        try:
            # ordered=False ensures that every insert operation will be attempted
            # so that if, e.g., a document already exists, it will be simply skipped
            self.db['db'][_collection].insert_many(_db_entries, ordered=False)
        except pymongo.errors.BulkWriteError as bwe:
            print(*time_stamps(), bwe.details)
        except Exception as _e:
            traceback.print_exc()
            print(_e)

    def replace_db_entry(self, _collection=None, _filter=None, _db_entry=None):
        """
            Insert a document _doc to collection _collection in DB.
            It is monitored for timeout in case DB connection hangs for some reason
        :param _collection:
        :param _filter:
        :param _db_entry:
        :return:
        """
        assert _collection is not None, 'Must specify collection'
        assert _db_entry is not None, 'Must specify document'
        try:
            self.db['db'][_collection].replace_one(_filter, _db_entry, upsert=True)
        except Exception as _e:
            print(*time_stamps(), 'Error replacing {:s} in {:s}'.format(str(_db_entry['_id']), _collection))
            traceback.print_exc()
            print(_e)

    @staticmethod
    def alert_mongify(alert):

        doc = dict(alert)

        # let mongo create a unique id
        # candid+objectId is a unique combination:
        # doc['_id'] = f"{alert['candid']}_{alert['objectId']}"

        # placeholders for cross-matches and classifications
        # doc['cross_matches'] = dict()
        doc['classifications'] = dict()

        # GeoJSON for 2D indexing
        doc['coordinates'] = {}
        _ra = doc['candidate']['ra']
        _dec = doc['candidate']['dec']
        _radec = [_ra, _dec]
        # string format: H:M:S, D:M:S
        # tic = time.time()
        _radec_str = [deg2hms(_ra), deg2dms(_dec)]
        # print(time.time() - tic)
        # print(_radec_str)
        doc['coordinates']['radec_str'] = _radec_str
        # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
        _radec_geojson = [_ra - 180.0, _dec]
        doc['coordinates']['radec_geojson'] = {'type': 'Point',
                                               'coordinates': _radec_geojson}
        # radians and degrees:
        # doc['coordinates']['radec_rad'] = [_ra * np.pi / 180.0, _dec * np.pi / 180.0]
        # doc['coordinates']['radec_deg'] = [_ra, _dec]

        prv_candidates = doc['prv_candidates']
        doc.pop('prv_candidates', None)

        return doc, prv_candidates

    def poll(self, path_alerts=None, datestr=None):
        """
            Polls Kafka broker to consume topic.
        :param path_alerts:
        :param datestr:
        :return:
        """
        # msg = self.consumer.poll(timeout=timeout)
        msg = self.consumer.poll()

        if msg is None:
            print(*time_stamps(), 'Caught error: msg is None')

        if msg.error():
            print('Caught error:', msg.error())
            # if msg.value() is not None:
            #     print(*time_stamps(), msg.value())
            raise EopError(msg)

        elif msg is not None:
            # decode avro packet
            msg_decoded = self.decodeMessage(msg)
            for record in msg_decoded:

                candid = record['candid']
                objectId = record['objectId']

                print(*time_stamps(), self.topic, objectId, candid)

                # candid in db?
                if self.db['db'][self.collection_alerts].count_documents({'candid': candid}, limit=1) == 0:
                    # candid not in db, ingest

                    # ingest decoded avro packet into db
                    alert, prv_candidates = self.alert_mongify(record)

                    # alert filters:

                    # ML models:
                    scores = alert_filter__ml(record, ml_models=self.ml_models)
                    alert['classifications'] = scores

                    # cross-match with external catalogs:
                    if self.db['db'][self.collection_alert_cross_matches].count_documents({'_id': objectId},
                                                                                          limit=1) == 0:
                        # print(alert['candid'], alert['candidate']['programpi'])
                        # if record['candidate']['programpi'].strip() == 'TESS':
                        # tic = time.time()
                        xmatches = alert_filter__xmatch(self.db['db'], alert)
                        # alert['cross_matches'] = xmatches
                        # toc = time.time()
                        # print(f'xmatch for {alert["candid"]} took {toc-tic:.2f} s')

                        alert_xmatches = {'_id': objectId,
                                          'cross_matches': xmatches}

                        self.insert_db_entry(_collection=self.collection_alert_cross_matches, _db_entry=alert_xmatches)

                    print(*time_stamps(), 'ingesting {:s} into db'.format(alert['candid']))
                    self.insert_db_entry(_collection=self.collection_alerts, _db_entry=alert)

                    # dump packet as json to disk if in a public TESS sector
                    if alert['candidate']['programpi'] == 'TESS' and alert['candidate']['programid'] == 1:

                        # get cross-matches
                        xmatches = self.db['db'][self.collection_alert_cross_matches].find_one({'_id': objectId})
                        alert['cross_matches'] = xmatches['cross_matches']

                        path_alert_dir = os.path.join(path_alerts, datestr)
                        # mkdir if does not exist
                        if not os.path.exists(path_alert_dir):
                            os.makedirs(path_alert_dir)

                        print(*time_stamps(), f'saving {alert["candid"]} to disk')
                        try:
                            with open(os.path.join(path_alert_dir, f"{alert['candid']}.json"), 'w') as f:
                                f.write(dumps(alert))
                        except Exception as e:
                            print(time_stamps(), str(e))

                    # iterate over prv_candidates

                    for prv_candidate in prv_candidates:
                        prv_candid = prv_candidate['candid']
                        prv_pid = prv_candidate['candid']
                        # pop nulls
                        prv_candidate = {kk: vv for kk, vv in prv_candidate.items() if vv is not None}
                        if prv_candid is not None:
                            if self.db['db'][self.collection_alerts].count_documents({'candid': prv_candid},
                                                                                     limit=1) == 0:
                                # candid not in db, ingest
                                _a = {'objectId': objectId,
                                      'candid': prv_candid,
                                      'candidate': prv_candidate}

                                # remove possible non-detection with same pid
                                if self.db['db'][self.collection_alerts].count_documents(
                                        {'candidate.pid': prv_pid},
                                        limit=1) == 1:
                                    self.replace_db_entry(_collection=self.collection_alerts,
                                                          _filter={'candidate.pid': prv_pid}, _db_entry=_a)
                                else:
                                    self.insert_db_entry(_collection=self.collection_alerts, _db_entry=_a)
                        else:
                            # candid==None
                            if self.db['db'][self.collection_alerts].count_documents({'candidate.pid': prv_pid},
                                                                                     limit=1) == 0:
                                # pid not in db (non-detection), ingest
                                _a = {'objectId': objectId,
                                      'candidate': prv_candidate}
                                self.insert_db_entry(_collection=self.collection_alerts, _db_entry=_a)

                else:
                    # candid in db
                    tmp = self.db['db'][self.collection_alerts].find({'candid': candid}, {'_id': 0, 'coordinate': 1})
                    if len(tmp) == 0:
                        # saved from prv_candidates of another alert, update entry

                        # ingest decoded avro packet into db
                        alert, prv_candidates = self.alert_mongify(record)

                        # alert filters:

                        # ML models:
                        scores = alert_filter__ml(record, ml_models=self.ml_models)
                        alert['classifications'] = scores

                        # cross-match with external catalogs:
                        if self.db['db'][self.collection_alert_cross_matches].count_documents({'_id': objectId},
                                                                                              limit=1) == 0:
                            # print(alert['candid'], alert['candidate']['programpi'])
                            # if record['candidate']['programpi'].strip() == 'TESS':
                            # tic = time.time()
                            xmatches = alert_filter__xmatch(self.db['db'], alert)
                            # alert['cross_matches'] = xmatches
                            # toc = time.time()
                            # print(f'xmatch for {alert["candid"]} took {toc-tic:.2f} s')

                            alert_xmatches = {'_id': objectId,
                                              'cross_matches': xmatches}

                            self.insert_db_entry(_collection=self.collection_alert_cross_matches,
                                                 _db_entry=alert_xmatches)

                        print(*time_stamps(), 're-ingesting {:s} into db'.format(alert['candid']))
                        self.replace_db_entry(_collection=self.collection_alerts,
                                              _filter={'candid': alert['candid']}, _db_entry=alert)

                        # dump packet as json to disk if in a public TESS sector
                        if alert['candidate']['programpi'] == 'TESS' and alert['candidate']['programid'] == 1:

                            # get cross-matches
                            xmatches = self.db['db'][self.collection_alert_cross_matches].find_one({'_id': objectId})
                            alert['cross_matches'] = xmatches['cross_matches']

                            path_alert_dir = os.path.join(path_alerts, datestr)
                            # mkdir if does not exist
                            if not os.path.exists(path_alert_dir):
                                os.makedirs(path_alert_dir)

                            print(*time_stamps(), f'saving {alert["candid"]} to disk')
                            try:
                                with open(os.path.join(path_alert_dir, f"{alert['candid']}.json"), 'w') as f:
                                    f.write(dumps(alert))
                            except Exception as e:
                                print(time_stamps(), str(e))

                        # iterate over prv_candidates
                        for prv_candidate in prv_candidates:
                            prv_candid = prv_candidate['candid']
                            prv_pid = prv_candidate['candid']
                            # todo: pop nulls
                            prv_candidate = {kk: vv for kk, vv in prv_candidate.items() if vv is not None}
                            if prv_candid is not None:
                                if self.db['db'][self.collection_alerts].count_documents({'candid': prv_candid},
                                                                                         limit=1) == 0:
                                    # candid not in db, ingest
                                    _a = {'objectId': objectId,
                                          'candid': prv_candid,
                                          'candidate': prv_candidate}

                                    # remove possible non-detection with same pid
                                    if self.db['db'][self.collection_alerts].count_documents(
                                            {'candidate.pid': prv_pid},
                                            limit=1) == 1:
                                        self.replace_db_entry(_collection=self.collection_alerts,
                                                              _filter={'candidate.pid': prv_pid}, _db_entry=_a)
                                    else:
                                        self.insert_db_entry(_collection=self.collection_alerts, _db_entry=_a)
                            else:
                                # candid==None
                                if self.db['db'][self.collection_alerts].count_documents(
                                        {'candidate.pid': prv_pid},
                                        limit=1) == 0:
                                    # pid not in db (non-detection), ingest
                                    _a = {'objectId': objectId,
                                          'candidate': prv_candidate}
                                    self.insert_db_entry(_collection=self.collection_alerts, _db_entry=_a)

    def decodeMessage(self, msg):
        """Decode Avro message according to a schema.

        Parameters
        ----------
        msg : Kafka message
            The Kafka message result from consumer.poll().

        Returns
        -------
        `dict`
            Decoded message.
        """
        # print(msg.topic(), msg.offset(), msg.error(), msg.key(), msg.value())
        message = msg.value()
        # print(message)
        try:
            bytes_io = io.BytesIO(message)
            decoded_msg = readSchemaData(bytes_io)
            # print(decoded_msg)
            # decoded_msg = readAvroData(bytes_io, self.alert_schema)
            # print(decoded_msg)
        except AssertionError:
            # FIXME this exception is raised but not sure if it matters yet
            bytes_io = io.BytesIO(message)
            decoded_msg = None
        except IndexError:
            literal_msg = literal_eval(str(message, encoding='utf-8'))  # works to give bytes
            bytes_io = io.BytesIO(literal_msg)  # works to give <class '_io.BytesIO'>
            decoded_msg = readSchemaData(bytes_io)  # yields reader
        except Exception:
            decoded_msg = message
        finally:
            return decoded_msg


def msg_text(message):
    """Remove postage stamp cutouts from an alert message.
    """
    message_text = {k: message[k] for k in message
                    if k not in ['cutoutDifference', 'cutoutTemplate', 'cutoutScience']}
    return message_text


def write_stamp_file(stamp_dict, output_dir):
    """Given a stamp dict that follows the cutout schema,
       write data to a file in a given directory.
    """
    try:
        filename = stamp_dict['fileName']
        try:
            os.makedirs(output_dir)
        except OSError:
            pass
        out_path = os.path.join(output_dir, filename)
        with open(out_path, 'wb') as f:
            f.write(stamp_dict['stampData'])
    except TypeError:
        sys.stderr.write('%% Cannot get stamp\n')
    return


def alert_filter(alert, stampdir=None):
    """Filter to apply to each alert.
       See schemas: https://github.com/ZwickyTransientFacility/ztf-avro-alert
    """
    data = msg_text(alert)
    if data:  # Write your condition statement here
        print(data)  # Print all main alert data to screen
        if stampdir is not None:  # Collect all postage stamps
            write_stamp_file(
                alert.get('cutoutDifference'), stampdir)
            write_stamp_file(
                alert.get('cutoutTemplate'), stampdir)
            write_stamp_file(
                alert.get('cutoutScience'), stampdir)
    return


def make_triplet(alert, to_tpu: bool = False):
    """
        Feed in alert packet
    """
    cutout_dict = dict()

    for cutout in ('science', 'template', 'difference'):
        # cutout_data = loads(dumps([alert[f'cutout{cutout.capitalize()}']['stampData']]))[0]
        cutout_data = alert[f'cutout{cutout.capitalize()}']['stampData']

        # unzip
        with gzip.open(io.BytesIO(cutout_data), 'rb') as f:
            with fits.open(io.BytesIO(f.read())) as hdu:
                data = hdu[0].data
                # replace nans with zeros
                cutout_dict[cutout] = np.nan_to_num(data)
                # L2-normalize
                cutout_dict[cutout] /= np.linalg.norm(cutout_dict[cutout])

        # pad to 63x63 if smaller
        shape = cutout_dict[cutout].shape
        if shape != (63, 63):
            # print(f'Shape of {candid}/{cutout}: {shape}, padding to (63, 63)')
            cutout_dict[cutout] = np.pad(cutout_dict[cutout], [(0, 63 - shape[0]), (0, 63 - shape[1])],
                                         mode='constant', constant_values=1e-9)

    triplet = np.zeros((63, 63, 3))
    triplet[:, :, 0] = cutout_dict['science']
    triplet[:, :, 1] = cutout_dict['template']
    triplet[:, :, 2] = cutout_dict['difference']

    if to_tpu:
        # Edge TPUs require additional processing
        triplet = np.rint(triplet * 128 + 128).astype(np.uint8).flatten()

    return triplet


def alert_filter__ml(alert, ml_models: dict = None):
    """Filter to apply to each alert.
    """

    scores = dict()

    try:
        ''' braai '''
        triplet = make_triplet(alert)
        triplets = np.expand_dims(triplet, axis=0)
        braai = ml_models['braai']['model'].predict(x=triplets)[0]
        # braai = 1.0
        scores['braai'] = float(braai)
        scores['braai_version'] = ml_models['braai']['version']
    except Exception as e:
        print(*time_stamps(), str(e))

    return scores


# cone search radius:
cone_search_radius = float(config['xmatch']['cone_search_radius'])
# convert to rad:
if config['xmatch']['cone_search_unit'] == 'arcsec':
    cone_search_radius *= np.pi / 180.0 / 3600.
elif config['xmatch']['cone_search_unit'] == 'arcmin':
    cone_search_radius *= np.pi / 180.0 / 60.
elif config['xmatch']['cone_search_unit'] == 'deg':
    cone_search_radius *= np.pi / 180.0
elif config['xmatch']['cone_search_unit'] == 'rad':
    cone_search_radius *= 1
else:
    raise Exception('Unknown cone search unit. Must be in [deg, rad, arcsec, arcmin]')


def alert_filter__xmatch(db, alert):
    """Filter to apply to each alert.
    """

    xmatches = dict()

    try:
        ra_geojson = float(alert['candidate']['ra'])
        # geojson-friendly ra:
        ra_geojson -= 180.0
        dec_geojson = float(alert['candidate']['dec'])

        ''' catalogs '''
        for catalog in config['xmatch']['catalogs']:
            catalog_filter = config['xmatch']['catalogs'][catalog]['filter']
            catalog_projection = config['xmatch']['catalogs'][catalog]['projection']

            object_position_query = dict()
            object_position_query['coordinates.radec_geojson'] = {
                '$geoWithin': {'$centerSphere': [[ra_geojson, dec_geojson], cone_search_radius]}}
            s = db[catalog].find({**object_position_query, **catalog_filter},
                                 {**catalog_projection})
            xmatches[catalog] = list(s)

    except Exception as e:
        print(*time_stamps(), str(e))

    return xmatches


def listener(topic, bootstrap_servers='', offset_reset='earliest',
             group=None, path_alerts=None):
    """
        Listen to a topic
    :param topic:
    :param bootstrap_servers:
    :param offset_reset:
    :param group:
    :param path_alerts:
    :return:
    """

    # def error_cb(err):
    #     print(*time_stamps(), 'error_cb -------->', err)
    #     # print(err.code())
    #     if err.code() == -195:
    #         print(*time_stamps(), 'got disconnected, killing thread')
    #         sys.exit()

    # Configure consumer connection to Kafka broker
    conf = {'bootstrap.servers': bootstrap_servers,
            # 'error_cb': error_cb,
            'default.topic.config': {'auto.offset.reset': offset_reset}}
    if group is not None:
        conf['group.id'] = group
    else:
        conf['group.id'] = os.environ['HOSTNAME'] if 'HOSTNAME' in os.environ else 'kowalski.caltech.edu'

    # make it unique:
    conf['group.id'] = '{:s}_{:s}'.format(conf['group.id'], datetime.datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f'))

    # Configure Avro reader schema
    schema_files = ["ztf-avro-alert/schema/candidate.avsc",
                    "ztf-avro-alert/schema/cutout.avsc",
                    "ztf-avro-alert/schema/prv_candidate.avsc",
                    "ztf-avro-alert/schema/alert.avsc"]

    # date string:
    datestr = topic.split('_')[1]

    # Start alert stream consumer
    stream_reader = AlertConsumer(topic, schema_files, **conf)

    # todo: Subscribe alert filters to stream_readers
    # todo: they will be notified when an alert arrived/got x-matched

    while True:
        try:
            # poll!
            # print(*time_stamps(), 'Polling')
            stream_reader.poll(path_alerts=path_alerts, datestr=datestr)

        except EopError as e:
            # Write when reaching end of partition
            # sys.stderr.write(e.message)
            print(*time_stamps(), e.message)
        except IndexError:
            # sys.stderr.write('%% Data cannot be decoded\n')
            print(*time_stamps(), '%% Data cannot be decoded\n')
        except UnicodeDecodeError:
            # sys.stderr.write('%% Unexpected data format received\n')
            print(*time_stamps(), '%% Unexpected data format received\n')
        except KeyboardInterrupt:
            # sys.stderr.write('%% Aborted by user\n')
            print(*time_stamps(), '%% Aborted by user\n')
            sys.exit()
        except Exception as e:
            print(*time_stamps(), str(e))
            sys.exit()


def main(_obs_date=None):

    topics_on_watch = dict()

    while True:

        try:
            if True:
                # get kafka topic names with kafka-topics command
                kafka_cmd = [config['kafka-topics']['cmd'],
                             '--zookeeper', config['kafka-topics']['zookeeper'], '-list']
                # print(kafka_cmd)

                topics = subprocess.run(kafka_cmd, stdout=subprocess.PIPE).stdout.decode('utf-8').split('\n')[:-1]
                # print(topics)

                if _obs_date is None:
                    datestr = datetime.datetime.utcnow().strftime('%Y%m%d')
                else:
                    datestr = _obs_date
                # as of 20180403 naming convention is ztf_%Y%m%d_programidN
                topics_tonight = [t for t in topics if datestr in t and 'programid' in t]
                print(*time_stamps(), topics_tonight)

            if False:
                # for testing
                topics_tonight = ['ztf_20180604_programid3']

            for t in topics_tonight:
                if t not in topics_on_watch:
                    print(*time_stamps(), f'starting listener thread for {t}')
                    offset_reset = config['kafka']['default.topic.config']['auto.offset.reset']
                    bootstrap_servers = config['kafka']['bootstrap.servers']
                    group = '{:s}'.format(config['kafka']['group'])
                    # print(group)
                    path_alerts = config['path']['path_alerts']
                    # topics_on_watch[t] = threading.Thread(target=listener,
                    #                                       args=(t, bootstrap_servers,
                    #                                             offset_reset, group, path_alerts))
                    topics_on_watch[t] = multiprocessing.Process(target=listener,
                                                                 args=(t, bootstrap_servers,
                                                                       offset_reset, group, path_alerts))
                    topics_on_watch[t].daemon = True
                    topics_on_watch[t].start()

                else:
                    print(*time_stamps(), f'performing thread health check for {t}')
                    try:
                        # if not topics_on_watch[t].isAlive():
                        if not topics_on_watch[t].is_alive():
                            print(*time_stamps(), f'{t} died, removing')
                            # topics_on_watch[t].terminate()
                            topics_on_watch.pop(t, None)
                        else:
                            print(*time_stamps(), f'{t} appears normal')
                    except Exception as _e:
                        print(*time_stamps(), 'Failed to perform health check', str(_e))
                        pass

        except Exception as e:
            print(*time_stamps(), str(e))

        if _obs_date is None:
            time.sleep(300)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch AVRO packets from Kafka streams and ingest them into DB')
    parser.add_argument('--obsdate', help='observing date')

    args = parser.parse_args()
    obs_date = args.obsdate
    # print(obs_date)

    main(_obs_date=obs_date)
