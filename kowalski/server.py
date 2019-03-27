from aiohttp import web
from multidict import MultiDict
import jinja2
import aiohttp_jinja2
from aiohttp_session import setup, get_session, session_middleware
from aiohttp_session.cookie_storage import EncryptedCookieStorage
import aiofiles
import json
import jwt
from motor.motor_asyncio import AsyncIOMotorClient
from bson.json_util import loads, dumps
import datetime
import time
from ast import literal_eval
from async_timeout import timeout
import asyncio
# import concurrent.futures
from misaka import Markdown, HtmlRenderer
import os
import pathlib
import shutil
import re
import numpy as np
import string
import random
import traceback
from astropy.io import fits
import io
import gzip

from utils import *


''' markdown rendering '''
rndr = HtmlRenderer()
md = Markdown(rndr, extensions=('fenced-code',))


''' load config and secrets '''
with open('/app/config.json') as cjson:
    config = json.load(cjson)

with open('/app/secrets.json') as sjson:
    secrets = json.load(sjson)

for k in secrets:
    config[k].update(secrets.get(k, {}))
# print(config)


async def init_db():
    _client = AsyncIOMotorClient(username=config['database']['admin'],
                                 password=config['database']['admin_pwd'],
                                 host=config['database']['host'],
                                 port=config['database']['port'])

    # _id: db_name.user_name
    user_ids = []
    async for _u in _client.admin.system.users.find({}, {'_id': 1}):
        user_ids.append(_u['_id'])

    print(user_ids)

    db_name = config['database']['db']
    username = config['database']['user']

    # print(f'{db_name}.{username}')
    # print(user_ids)

    _mongo = _client[db_name]

    if f'{db_name}.{username}' not in user_ids:
        await _mongo.command('createUser', config['database']['user'],
                             pwd=config['database']['pwd'], roles=['readWrite'])
        print('Successfully initialized db')

    _mongo.client.close()


async def add_admin(_mongo):
    """
        Create admin user for the web interface if it does not exists already
    :return:
    """
    ex_admin = await _mongo.users.find_one({'_id': config['server']['admin_username']})
    if ex_admin is None or len(ex_admin) == 0:
        try:
            await _mongo.users.insert_one({'_id': config['server']['admin_username'],
                                           'password': generate_password_hash(config['server']['admin_password']),
                                           'permissions': {},
                                           'last_modified': utc_now()
                                           })
        except Exception as e:
            print(f'Got error: {str(e)}')
            _err = traceback.format_exc()
            print(_err)


routes = web.RouteTableDef()


@web.middleware
async def auth_middleware(request, handler):
    """
        auth middleware
    :param request:
    :param handler:
    :return:
    """
    tic = time.time()
    request.user = None
    jwt_token = request.headers.get('authorization', None)

    if jwt_token:
        try:
            payload = jwt.decode(jwt_token, request.app['JWT']['JWT_SECRET'],
                                 algorithms=[request.app['JWT']['JWT_ALGORITHM']])
            # print('Godny token!')
        except (jwt.DecodeError, jwt.ExpiredSignatureError):
            return web.json_response({'message': 'Token is invalid'}, status=400)

        request.user = payload['user_id']

    response = await handler(request)
    toc = time.time()
    # print(f"Auth middleware took {toc-tic} seconds to execute")

    return response


def auth_required(func):
    """
        Wrapper to ensure successful user authorization to use the API
    :param func:
    :return:
    """
    def wrapper(request):
        if not request.user:
            return web.json_response({'message': 'Auth required'}, status=401)
        return func(request)
    return wrapper


def login_required(func):
    """
        Wrapper to ensure successful user authorization to use the web frontend
    :param func:
    :return:
    """
    async def wrapper(request):
        # get session:
        session = await get_session(request)
        if 'jwt_token' not in session:
            # return web.json_response({'message': 'Auth required'}, status=401)
            # redirect to login page
            location = request.app.router['login'].url_for()
            # location = '/login'
            raise web.HTTPFound(location=location)
        else:
            jwt_token = session['jwt_token']
            if not await token_ok(request, jwt_token):
                # return web.json_response({'message': 'Auth required'}, status=401)
                # redirect to login page
                location = request.app.router['login'].url_for()
                # location = '/login'
                raise web.HTTPFound(location=location)
        return await func(request)
    return wrapper


async def token_ok(request, jwt_token):
    try:
        payload = jwt.decode(jwt_token, request.app['JWT']['JWT_SECRET'],
                             algorithms=[request.app['JWT']['JWT_ALGORITHM']])
        return True
    except (jwt.DecodeError, jwt.ExpiredSignatureError):
        return False


@routes.post('/auth')
async def auth(request):
    try:
        post_data = await request.json()
    except Exception as _e:
        print(f'Cannot extract json() from request, trying post(): {str(_e)}')
        # _err = traceback.format_exc()
        # print(_err)
        post_data = await request.post()

    # print(post_data)

    # must contain 'username' and 'password'

    if ('username' not in post_data) or (len(post_data['username']) == 0):
        return web.json_response({'message': 'Missing "username"'}, status=400)
    if ('password' not in post_data) or (len(post_data['password']) == 0):
        return web.json_response({'message': 'Missing "password"'}, status=400)

    # connecting from penquins: check penquins version
    if 'penquins.__version__' in post_data:
        penquins_version = post_data['penquins.__version__']
        if penquins_version not in config['misc']['supported_penquins_versions']:
            return web.json_response({'message': 'Unsupported version of penquins. ' +
                                                 "Please install the latest version + read current docs on Kowalski!"
                                      }, status=400)

    username = str(post_data['username'])
    password = str(post_data['password'])

    try:
        # user exists and passwords match?
        select = await request.app['mongo'].users.find_one({'_id': username})
        if check_password_hash(select['password'], password):
            payload = {
                'user_id': username,
                'exp': datetime.datetime.utcnow() + datetime.timedelta(
                    seconds=request.app['JWT']['JWT_EXP_DELTA_SECONDS'])
            }
            jwt_token = jwt.encode(payload,
                                   request.app['JWT']['JWT_SECRET'],
                                   request.app['JWT']['JWT_ALGORITHM'])

            return web.json_response({'token': jwt_token.decode('utf-8')})

        else:
            return web.json_response({'message': 'Wrong credentials'}, status=400)

    except Exception as e:
        print(f'Got error: {str(e)}')
        _err = traceback.format_exc()
        print(_err)
        return web.json_response({'message': 'Wrong credentials'}, status=400)


@routes.get('/login')
async def login_get(request):
    """
        Serve login page
    :param request:
    :return:
    """
    context = {'logo': config['server']['logo']}
    response = aiohttp_jinja2.render_template('template-login.html',
                                              request,
                                              context)
    return response


@routes.post('/login', name='login')
async def login_post(request):
    """
        Server login page for the browser
    :param request:
    :return:
    """
    try:
        try:
            post_data = await request.json()
        except Exception as _e:
            print(f'Cannot extract json() from request, trying post(): {str(_e)}')
            # _err = traceback.format_exc()
            # print(_err)
            post_data = await request.post()

        # get session:
        session = await get_session(request)

        if ('username' not in post_data) or (len(post_data['username']) == 0):
            return web.json_response({'message': 'Missing "username"'}, status=400)
        if ('password' not in post_data) or (len(post_data['password']) == 0):
            return web.json_response({'message': 'Missing "password"'}, status=400)

        username = str(post_data['username'])
        password = str(post_data['password'])

        # print(username, password)
        print(f'User {username} logged in.')

        # user exists and passwords match?
        select = await request.app['mongo'].users.find_one({'_id': username})
        if check_password_hash(select['password'], password):
            payload = {
                'user_id': username,
                'exp': datetime.datetime.utcnow() + datetime.timedelta(
                    seconds=request.app['JWT']['JWT_EXP_DELTA_SECONDS'])
            }
            jwt_token = jwt.encode(payload,
                                   request.app['JWT']['JWT_SECRET'],
                                   request.app['JWT']['JWT_ALGORITHM'])

            # store the token, will need it
            session['jwt_token'] = jwt_token.decode('utf-8')
            session['user_id'] = username

            print('LOGIN', session)

            return web.json_response({'message': 'success'}, status=200)

        else:
            raise Exception('Bad credentials')

    except Exception as _e:
        print(f'Got error: {str(_e)}')
        _err = traceback.format_exc()
        print(_err)
        return web.json_response({'message': f'Failed to login user: {_err}'}, status=401)


@routes.get('/logout', name='logout')
async def logout(request):
    """
        Logout web user
    :param request:
    :return:
    """
    # get session:
    session = await get_session(request)

    session.invalidate()

    # redirect to login page
    location = request.app.router['login'].url_for()
    # location = '/login'
    raise web.HTTPFound(location=location)


@routes.get('/test')
@auth_required
async def handler_test(request):
    return web.json_response({'message': 'test ok.'}, status=200)


@routes.get('/test_wrapper')
@login_required
async def wrapper_handler_test(request):
    return web.json_response({'message': 'test ok.'}, status=200)


@routes.get('/', name='root')
@login_required
async def root_handler(request):
    """
        Serve home page for the browser
    :param request:
    :return:
    """
    # get session:
    session = await get_session(request)

    context = {'logo': config['server']['logo'],
               'user': session['user_id']}
    response = aiohttp_jinja2.render_template('template-root.html',
                                              request,
                                              context)
    # response.headers['Content-Language'] = 'ru'
    return response


''' manage users: API '''


@routes.get('/users')
@login_required
async def manage_users(request):
    """
        Serve users page for the browser
    :param request:
    :return:
    """
    # get session:
    session = await get_session(request)

    # only admin can access this
    if session['user_id'] == config['server']['admin_username']:
        users = await request.app['mongo'].users.find({}, {'password': 0}).to_list(length=1000)
        # print(users)

        context = {'logo': config['server']['logo'],
                   'user': session['user_id'],
                   'users': users}
        response = aiohttp_jinja2.render_template('template-users.html',
                                                  request,
                                                  context)
        return response

    else:
        return web.json_response({'message': '403 Forbidden'}, status=403)


@routes.put('/users')
@login_required
async def add_user(request):
    """
        Add new user to DB
    :return:
    """
    # get session:
    session = await get_session(request)

    _data = await request.json()
    # print(_data)

    if session['user_id'] == config['server']['admin_username']:
        try:
            username = _data['user'] if 'user' in _data else None
            password = _data['password'] if 'password' in _data else None
            permissions = _data['permissions'] if 'permissions' in _data else '{}'

            if len(username) == 0 or len(password) == 0:
                return web.json_response({'message': 'username and password must be set'}, status=500)

            if len(permissions) == 0:
                permissions = '{}'

            # add user to coll_usr collection:
            await request.app['mongo'].users.insert_one(
                {'_id': username,
                 'password': generate_password_hash(password),
                 'permissions': literal_eval(str(permissions)),
                 'last_modified': datetime.datetime.now()}
            )

            return web.json_response({'message': 'success'}, status=200)

        except Exception as _e:
            print(f'Got error: {str(_e)}')
            _err = traceback.format_exc()
            print(_err)
            return web.json_response({'message': f'Failed to add user: {_err}'}, status=500)
    else:
        return web.json_response({'message': '403 Forbidden'}, status=403)


@routes.delete('/users')
@login_required
async def remove_user(request):
    """
        Remove user from DB
    :return:
    """
    # get session:
    session = await get_session(request)

    _data = await request.json()
    # print(_data)

    if session['user_id'] == config['server']['admin_username']:
        try:
            # get username from request
            username = _data['user'] if 'user' in _data else None
            if username == config['server']['admin_username']:
                return web.json_response({'message': 'Cannot remove the superuser!'}, status=500)

            # try to remove the user:
            await request.app['mongo'].users.delete_one({'_id': username})

            return web.json_response({'message': 'success'}, status=200)

        except Exception as _e:
            print(f'Got error: {str(_e)}')
            _err = traceback.format_exc()
            print(_err)
            return web.json_response({'message': f'Failed to remove user: {_err}'}, status=500)
    else:
        return web.json_response({'message': '403 Forbidden'}, status=403)


@routes.post('/users')
@login_required
async def edit_user(request):
    """
        Edit user info
    :return:
    """
    # get session:
    session = await get_session(request)

    _data = await request.json()
    # print(_data)

    if session['user_id'] == config['server']['admin_username']:
        try:
            _id = _data['_user'] if '_user' in _data else None
            username = _data['edit-user'] if 'edit-user' in _data else None
            password = _data['edit-password'] if 'edit-password' in _data else None
            # permissions = _data['edit-permissions'] if 'edit-permissions' in _data else '{}'

            if _id == config['server']['admin_username'] and username != config['server']['admin_username']:
                return web.json_response({'message': 'Cannot change the admin username!'}, status=500)

            if len(username) == 0:
                return web.json_response({'message': 'username must be set'}, status=500)

            # change username:
            if _id != username:
                select = await request.app['mongo'].users.find_one({'_id': _id})
                select['_id'] = username
                await request.app['mongo'].users.insert_one(select)
                await request.app['mongo'].users.delete_one({'_id': _id})

            # change password:
            if len(password) != 0:
                await request.app['mongo'].users.update_one(
                    {'_id': username},
                    {
                        '$set': {
                            'password': generate_password_hash(password)
                        },
                        '$currentDate': {'last_modified': True}
                    }
                )

            # change permissions:
            # if len(permissions) != 0:
            #     select = await request.app['mongo'].users.find_one({'_id': username}, {'_id': 0, 'permissions': 1})
            #     # print(select)
            #     # print(permissions)
            #     _p = literal_eval(str(permissions))
            #     # print(_p)
            #     if str(permissions) != str(select['permissions']):
            #         result = await request.app['mongo'].users.update_one(
            #             {'_id': _id},
            #             {
            #                 '$set': {
            #                     'permissions': _p
            #                 },
            #                 '$currentDate': {'last_modified': True}
            #             }
            #         )

            return web.json_response({'message': 'success'}, status=200)

        except Exception as _e:
            print(f'Got error: {str(_e)}')
            _err = traceback.format_exc()
            print(_err)
            return web.json_response({'message': f'Failed to remove user: {_err}'}, status=500)
    else:
        return web.json_response({'message': '403 Forbidden'}, status=403)


''' ZTF alert stats'''


@routes.get('/lab/ztf-alert-stats', name='ztf-alert-stats')
@login_required
async def ztf_alert_stats_get_handler(request):
    """
        Serve docs page for the browser
    :param request:
    :return:
    """
    # get session:
    session = await get_session(request)

    q = request.query
    if 'date' in q:
        utc = datetime.datetime.strptime(q['date'], '%Y%m%d')
    else:
        utc = utc_now()

    utc_sod = datetime.datetime(utc.year, utc.month, utc.day)
    jd_sod = jd(utc_sod)

    date = utc.strftime('%B %-d, %Y')

    total = await request.app['mongo'].ZTF_alerts.estimated_document_count({})

    last_date = await request.app['mongo'].ZTF_alerts.find({}, {'_id': 0,
                                                                'candidate.jd': 1}
                                                           ).sort([('candidate.jd', -1)]).limit(1).to_list(length=1)
    last_date = jd_to_datetime(last_date[0]['candidate']['jd']) if len(last_date) > 0 else datetime.datetime(2018, 2, 9)
    last_date = last_date.strftime('%B %-d, %Y, %H:%M:%S')
    # last_date = None
    # print(last_date)

    total_date = await request.app['mongo'].ZTF_alerts.count_documents({'candidate.jd': {'$gt': jd_sod,
                                                                                         '$lt': jd_sod + 1}})

    fields = await request.app['mongo'].ZTF_alerts.distinct('candidate.field', {'candidate.jd': {'$gt': jd_sod,
                                                                                                 '$lt': jd_sod + 1}})
    fields = sorted(list(map(int, fields)))

    stats = dict()
    stats['total'] = total
    stats['total_date'] = total_date
    stats['total_date_programid'] = {pid: await request.app['mongo'].ZTF_alerts.count_documents({
        'candidate.jd': {'$gt': jd_sod, '$lt': jd_sod + 1}, 'candidate.programid': pid}) for pid in range(4)}
    stats['fields'] = fields

    context = {'logo': config['server']['logo'],
               'user': session['user_id'],
               'date': date,
               'stats': stats,
               'last_date': last_date}

    response = aiohttp_jinja2.render_template('template-lab-ztf-alert-stats.html',
                                              request,
                                              context)
    return response


''' query API '''

regex = dict()
regex['collection_main'] = re.compile(r"db\[['\"](.*?)['\"]\]")
regex['aggregate'] = re.compile(r"aggregate\((\[(?s:.*)\])")


def parse_query(task, save: bool=False):
    # save auxiliary stuff
    kwargs = task['kwargs'] if 'kwargs' in task else {}

    # reduce!
    task_reduced = {'user': task['user'], 'query': {}, 'kwargs': kwargs}

    # fixme: this is for testing api from cl
    # if '_id' not in task_reduced['kwargs']:
    #     task_reduced['kwargs']['_id'] = ''.join(random.choices(string.ascii_letters + string.digits, k=32))

    if task['query_type'] == 'general_search':
        # specify task type:
        task_reduced['query_type'] = 'general_search'
        # nothing dubious to start with?
        if task['user'] != config['server']['admin_username']:
            go_on = True in [s in str(task['query']) for s in ['.aggregate(',
                                                               '.map_reduce(',
                                                               '.distinct(',
                                                               '.count_documents(',
                                                               '.index_information(',
                                                               '.find_one(',
                                                               '.find(']] and \
                    True not in [s in str(task['query']) for s in ['import',
                                                                   'pymongo.',
                                                                   'shutil.',
                                                                   'command(',
                                                                   'bulk_write(',
                                                                   'exec(',
                                                                   'spawn(',
                                                                   'subprocess(',
                                                                   'call(',
                                                                   'insert(',
                                                                   'update(',
                                                                   'delete(',
                                                                   'create_index(',
                                                                   'create_collection(',
                                                                   'run(',
                                                                   'popen(',
                                                                   'Popen(']] and \
                    str(task['query']).strip()[0] not in ('"', "'", '[', '(', '{', '\\')
        else:
            go_on = True

        # TODO: check access permissions:
        # TODO: for now, only check on admin stuff
        if task['user'] != config['server']['admin_username']:
            prohibited_collections = ('users', 'stats', 'queries')

            # get the main collection that is being queried:
            main_collection = regex['collection_main'].search(str(task['query'])).group(1)
            # print(main_collection)

            if main_collection in prohibited_collections:
                go_on = False

            # aggregating?
            if '.aggregate(' in str(task['query']):
                pipeline = literal_eval(regex['aggregate'].search(str(task['query'])).group(1))
                # pipeline = literal_eval(self.regex['aggregate'].search(str(task['query'])).group(1))
                lookups = [_ip for (_ip, _pp) in enumerate(pipeline) if '$lookup' in _pp]
                for _l in lookups:
                    if pipeline[_l]['$lookup']['from'] in prohibited_collections:
                        go_on = False

        if go_on:
            task_reduced['query'] = task['query']
        else:
            raise Exception('Atata!')

    elif task['query_type'] == 'cone_search':
        # specify task type:
        task_reduced['query_type'] = 'cone_search'
        # cone search radius:
        cone_search_radius = float(task['object_coordinates']['cone_search_radius'])
        # convert to rad:
        if task['object_coordinates']['cone_search_unit'] == 'arcsec':
            cone_search_radius *= np.pi / 180.0 / 3600.
        elif task['object_coordinates']['cone_search_unit'] == 'arcmin':
            cone_search_radius *= np.pi / 180.0 / 60.
        elif task['object_coordinates']['cone_search_unit'] == 'deg':
            cone_search_radius *= np.pi / 180.0
        elif task['object_coordinates']['cone_search_unit'] == 'rad':
            cone_search_radius *= 1
        else:
            raise Exception('Unknown cone search unit. Must be in [deg, rad, arcsec, arcmin]')

        for catalog in task['catalogs']:
            # TODO: check that not trying to query what's not allowed!
            task_reduced['query'][catalog] = dict()
            # parse catalog query:
            # construct filter
            _filter = task['catalogs'][catalog]['filter']
            if isinstance(_filter, str):
                # passed string? evaluate:
                catalog_query = literal_eval(_filter.strip())
            elif isinstance(_filter, dict):
                # passed dict?
                catalog_query = _filter
            else:
                raise ValueError('Unsupported filter specification')

            # construct projection
            # catalog_projection = dict()
            # FIXME: always return standardized coordinates?
            # catalog_projection = {'coordinates.epoch': 1, 'coordinates.radec_str': 1, 'coordinates.radec': 1}
            _projection = task['catalogs'][catalog]['projection']
            if isinstance(_projection, str):
                # passed string? evaluate:
                catalog_projection = literal_eval(_projection.strip())
            elif isinstance(_filter, dict):
                # passed dict?
                catalog_projection = _projection
            else:
                raise ValueError('Unsupported projection specification')

            # parse coordinate list

            # comb radecs for single sources as per Tom's request:
            radec = task['object_coordinates']['radec'].strip()
            if radec[0] not in ('[', '(', '{'):
                ra, dec = radec.split()
                if ('s' in radec) or (':' in radec):
                    radec = f"[('{ra}', '{dec}')]"
                else:
                    radec = f"[({ra}, {dec})]"

            # print(task['object_coordinates']['radec'])
            objects = literal_eval(radec)
            # print(type(objects), isinstance(objects, dict), isinstance(objects, list))

            # this could either be list [(ra1, dec1), (ra2, dec2), ..] or dict {'name': (ra1, dec1), ...}
            if isinstance(objects, list):
                object_coordinates = objects
                object_names = [str(obj_crd) for obj_crd in object_coordinates]
            elif isinstance(objects, dict):
                object_names, object_coordinates = zip(*objects.items())
                object_names = list(map(str, object_names))
            else:
                raise ValueError('Unsupported type of object coordinates')

            # print(object_names, object_coordinates)

            for oi, obj_crd in enumerate(object_coordinates):
                # convert ra/dec into GeoJSON-friendly format
                # print(obj_crd)
                _ra, _dec = radec_str2geojson(*obj_crd)
                # print(str(obj_crd), _ra, _dec)
                object_position_query = dict()
                object_position_query['coordinates.radec_geojson'] = {
                    '$geoWithin': {'$centerSphere': [[_ra, _dec], cone_search_radius]}}
                # use stringified object coordinates as dict keys and merge dicts with cat/obj queries:
                task_reduced['query'][catalog][object_names[oi]] = ({**object_position_query, **catalog_query},
                                                                    {**catalog_projection})

    if save:
        # print(task_reduced)
        # task_hashable = dumps(task)
        task_hashable = dumps(task_reduced)
        # compute hash for task. this is used as key in DB
        task_hash = compute_hash(task_hashable)

        # print({'user': task['user'], 'task_id': task_hash})

        # mark as enqueued in DB:
        t_stamp = utc_now()
        if 'query_expiration_interval' not in kwargs:
            # default expiration interval:
            t_expires = t_stamp + datetime.timedelta(days=int(config['misc']['query_expiration_interval']))
        else:
            # custom expiration interval:
            t_expires = t_stamp + datetime.timedelta(days=int(kwargs['query_expiration_interval']))

        # dump task_hashable to file, as potentially too big to store in mongo
        # save task:
        user_tmp_path = os.path.join(config['path']['path_queries'], task['user'])
        # print(user_tmp_path)
        # mkdir if necessary
        if not os.path.exists(user_tmp_path):
            os.makedirs(user_tmp_path)
        task_file = os.path.join(user_tmp_path, f'{task_hash}.task.json')

        with open(task_file, 'w') as f_task_file:
            f_task_file.write(dumps(task))

        task_doc = {'task_id': task_hash,
                    'user': task['user'],
                    'task': task_file,
                    'result': None,
                    'status': 'enqueued',
                    'created': t_stamp,
                    'expires': t_expires,
                    'last_modified': t_stamp}

        return task_hash, task_reduced, task_doc

    else:
        return '', task_reduced, {}


async def execute_query(mongo, task_hash, task_reduced, task_doc, save: bool=False):

    db = mongo

    if save:
        # mark query as enqueued:
        await db.queries.insert_one(task_doc)

    result = dict()
    query_result = dict()

    query = task_reduced

    try:

        # cone search:
        if query['query_type'] == 'cone_search':
            # iterate over catalogs as they represent
            for catalog in query['query']:
                query_result[catalog] = dict()
                # iterate over objects:
                for obj in query['query'][catalog]:
                    # project?
                    if len(query['query'][catalog][obj][1]) > 0:
                        _select = db[catalog].find(query['query'][catalog][obj][0],
                                                   query['query'][catalog][obj][1])
                    # return the whole documents by default
                    else:
                        _select = db[catalog].find(query['query'][catalog][obj][0])
                    # unfortunately, mongoDB does not allow to have dots in field names,
                    # thus replace with underscores
                    query_result[catalog][obj.replace('.', '_')] = await _select.to_list(length=None)

        elif query['query_type'] == 'general_search':
            # just evaluate. I know that's dangerous, but I'm checking things in broker.py
            qq = bytes(query['query'], 'utf-8').decode('unicode_escape')

            _select = eval(qq)
            # _select = eval(query['query'])
            # _select = literal_eval(qq)

            if ('.find_one(' in qq) or ('.count_documents(' in qq) or ('.index_information(' in qq):
                _select = await _select

            # make it look like json
            # print(list(_select))
            if isinstance(_select, int) or isinstance(_select, float) or \
                    isinstance(_select, list) or isinstance(_select, dict):
                query_result['query_result'] = _select
            else:
                query_result['query_result'] = await _select.to_list(length=None)

        result['user'] = query['user']
        result['status'] = 'done'
        result['kwargs'] = query['kwargs'] if 'kwargs' in query else {}

        if not save:
            # dump result back
            result['result_data'] = query_result

        else:
            # save task result:
            user_tmp_path = os.path.join(config['path']['path_queries'], query['user'])
            # print(user_tmp_path)
            # mkdir if necessary
            if not os.path.exists(user_tmp_path):
                os.makedirs(user_tmp_path)
            task_result_file = os.path.join(user_tmp_path, f'{task_hash}.result.json')

            # save location in db:
            result['result'] = task_result_file

            async with aiofiles.open(task_result_file, 'w') as f_task_result_file:
                task_result = dumps(query_result)
                await f_task_result_file.write(task_result)

        # print(task_hash, result)

        # db book-keeping:
        if save:
            # mark query as done:
            await db.queries.update_one({'user': query['user'], 'task_id': task_hash},
                                        {'$set': {'status': result['status'],
                                                  'last_modified': utc_now(),
                                                  'result': result['result']}}
                                        )

        # return task_hash, dumps(result)
        return task_hash, result

    except Exception as e:
        print(f'Got error: {str(e)}')
        _err = traceback.format_exc()
        print(_err)

        # book-keeping:
        if save:
            # save task result with error message:
            user_tmp_path = os.path.join(config['path']['path_queries'], query['user'])
            # print(user_tmp_path)
            # mkdir if necessary
            if not os.path.exists(user_tmp_path):
                os.makedirs(user_tmp_path)
            task_result_file = os.path.join(user_tmp_path, f'{task_hash}.result.json')

            # save location in db:
            result['user'] = query['user']
            result['status'] = 'failed'

            query_result = dict()
            query_result['msg'] = _err

            async with aiofiles.open(task_result_file, 'w') as f_task_result_file:
                task_result = dumps(query_result)
                await f_task_result_file.write(task_result)

            # mark query as failed:
            await db.queries.update_one({'user': query['user'], 'task_id': task_hash},
                                        {'$set': {'status': result['status'],
                                                  'last_modified': utc_now(),
                                                  'result': None}}
                                        )

        raise Exception('Query failed')


@routes.put('/query')
@auth_required
async def query(request):
    """
        Query DB programmatic API

    :return:
    """

    _query = await request.json()
    # print(_query)

    try:
        # parse query
        known_query_types = ('cone_search', 'general_search')

        assert _query['query_type'] in known_query_types, \
            f'query_type {_query["query_type"]} not in {str(known_query_types)}'

        _query['user'] = request.user

        # by default, [unless enqueue_only is requested]
        # all queries are not registered in the db and the task/results are stored on disk as json files
        # giving a significant execution speed up. this behaviour can be overridden.
        if ('kwargs' in _query) and ('enqueue_only' in _query['kwargs']) and _query['kwargs']['enqueue_only']:
            save = True
        else:
            save = _query['kwargs']['save'] if (('kwargs' in _query) and ('save' in _query['kwargs'])) else False

        # tic = time.time()
        task_hash, task_reduced, task_doc = parse_query(_query, save=save)
        # toc = time.time()
        # print(f'parsing task took {toc-tic} seconds')
        # print(task_hash, task_reduced, task_doc)

        # schedule query execution:
        if ('enqueue_only' in task_reduced['kwargs']) and task_reduced['kwargs']['enqueue_only']:
            # only schedule query execution. store query and results, return query id to user
            asyncio.ensure_future(execute_query(request.app['mongo'], task_hash, task_reduced, task_doc, save))
            return web.json_response({'status': 'enqueued', 'query_id': task_hash}, status=200, dumps=dumps)
        else:
            if save:
                # db book-keeping and saving to disk? result['result'] will contain result json location
                task_hash, result = await execute_query(request.app['mongo'], task_hash, task_reduced, task_doc, save)

                # with open(result['result'], 'rb') as f_task_result_file:
                #     response = web.Response(body=f_task_result_file, headers={
                #             'CONTENT-DISPOSITION': f'attachment; filename={f_task_result_file}'
                #         })
                #     response.content_type = 'text/json'
                #     # response = web.StreamResponse(body=f_task_result_file, headers={
                #     #     'CONTENT-DISPOSITION': f'attachment; filename={f_task_result_file}'
                #     # })
                #
                #     return response

                return web.json_response(result, status=200, dumps=dumps)
            else:
                # result['result_data'] will contain query result
                task_hash, result = await execute_query(request.app['mongo'], task_hash, task_reduced, task_doc, save)

                return web.json_response(result, status=200, dumps=dumps)

    except Exception as _e:
        print(f'Got error: {str(_e)}')
        _err = traceback.format_exc()
        print(_err)
        return web.json_response({'message': f'Failure: {_err}'}, status=500)


@routes.patch('/query')
@auth_required
async def query_abort(request):
    # todo: abort an enqueued query
    pass


@routes.delete('/query')
@auth_required
async def query_delete(request):
    """
        Delete Query from DB programmatically.

    :return:
    """

    # get user:
    user = request.user

    # get query
    _data = await request.json()
    # print(_data)

    try:
        if _data['task_id'] != 'all':
            await request.app['mongo'].queries.delete_one({'user': user, 'task_id': {'$eq': _data['task_id']}})

            # remove files containing task and result
            for p in pathlib.Path(os.path.join(config['path']['path_queries'], user)).glob(f'{_data["task_id"]}*'):
                p.unlink()

        else:
            await request.app['mongo'].queries.delete_many({'user': user})

            # remove all files containing task and result
            if os.path.exists(os.path.join(config['path']['path_queries'], user)):
                shutil.rmtree(os.path.join(config['path']['path_queries'], user))

        return web.json_response({'message': 'success'}, status=200)

    except Exception as _e:
        print(f'Got error: {str(_e)}')
        _err = traceback.format_exc()
        print(_err)
        return web.json_response({'message': f'Failure: {_err}'}, status=500)


@routes.post('/query')
@auth_required
async def query_grab(request):
    """
        Grab Query / result from DB from the browser.

    :return:
    """

    # get user:
    user = request.user

    # get query
    _data = await request.json()
    # print(_data)

    try:
        task_id = _data['task_id']
        part = _data['part']

        query = await request.app['mongo'].queries.find_one({'user': user, 'task_id': {'$eq': task_id}}, {'status': 1})
        # print(query)

        if part == 'task':
            task_file = os.path.join(config['path']['path_queries'], user, f'{task_id}.task.json')
            # with open(task_file, 'r') as f_task_file:
            async with aiofiles.open(task_file, 'r') as f_task_file:
                return web.json_response(await f_task_file.read(), status=200)

        elif part == 'result':
            if query['status'] == 'enqueued':
                return web.json_response({'message': f'query not finished yet'}, status=200)

            task_result_file = os.path.join(config['path']['path_queries'], user, f'{task_id}.result.json')

            async with aiofiles.open(task_result_file, 'r') as f_task_result_file:
                return web.json_response(await f_task_result_file.read(), status=200)

        else:
            return web.json_response({'message': f'Failure: part not recognized'}, status=500)

    except Exception as _e:
        print(f'Got error: {str(_e)}')
        _err = traceback.format_exc()
        print(_err)
        return web.json_response({'message': f'Failure: {_err}'}, status=500)


''' useful async stuff 

tasks = [
    asyncio.ensure_future(long_computation(request.app['mongo'], 1)),
    asyncio.ensure_future(long_computation(request.app['mongo'], 2)),
]
loop = asyncio.get_event_loop()
if long_computation is blocking, need to hand it off to a separate thread using run_in_executor:
1. Run in the default loop's executor:
tasks = [
    loop.run_in_executor(None, long_computation, request.app['mongo'], 1),
    loop.run_in_executor(None, long_computation, request.app['mongo'], 2),
]
2. Run in a custom thread pool:
with concurrent.futures.ThreadPoolExecutor() as pool:
    tasks = [
        loop.run_in_executor(pool, long_computation, request.app['mongo'], 1),
        loop.run_in_executor(pool, long_computation, request.app['mongo'], 2),
    ]
done, _ = await asyncio.wait(tasks)

# for f in done:
        #     print(f"{f.result()}")

'''


''' API for the browser '''
# Uses sessions => needs additional middleware => slightly slower if were used together with @auth_required


@routes.put('/web-query')
@login_required
async def web_query_put(request):
    """
        Query DB from the browser.

    :return:
    """

    # get session:
    session = await get_session(request)
    user = session['user_id']

    # get query
    _query = await request.json()
    # print(_query)

    try:
        # parse query
        known_query_types = ('cone_search', 'general_search')

        assert _query['query_type'] in known_query_types, \
            f'query_type {_query["query_type"]} not in {str(known_query_types)}'

        _query['user'] = user
        save = True  # always save to db when querying from the browser

        # tic = time.time()
        task_hash, task_reduced, task_doc = parse_query(_query, save=save)
        # toc = time.time()
        # print(f'parsing task took {toc-tic} seconds')
        # print(task_hash, task_reduced, task_doc)

        # schedule query execution:
        asyncio.ensure_future(execute_query(request.app['mongo'], task_hash, task_reduced, task_doc, save))

        return web.json_response({'message': 'success'}, status=200)

    except Exception as _e:
        print(f'Got error: {str(_e)}')
        _err = traceback.format_exc()
        print(_err)
        return web.json_response({'message': f'Failure: {_err}'}, status=500)


@routes.delete('/web-query')
@login_required
async def web_query_delete(request):
    """
        Delete Query from DB from the browser.

    :return:
    """

    # get session:
    session = await get_session(request)
    user = session['user_id']

    # get query
    _data = await request.json()
    # print(_data)

    try:
        if _data['task_id'] != 'all':
            await request.app['mongo'].queries.delete_one({'user': user, 'task_id': {'$eq': _data['task_id']}})

            # remove files containing task and result
            for p in pathlib.Path(os.path.join(config['path']['path_queries'], user)).glob(f'{_data["task_id"]}*'):
                p.unlink()

        else:
            await request.app['mongo'].queries.delete_many({'user': user})

            # remove all files containing task and result
            if os.path.exists(os.path.join(config['path']['path_queries'], user)):
                shutil.rmtree(os.path.join(config['path']['path_queries'], user))

        return web.json_response({'message': 'success'}, status=200)

    except Exception as _e:
        print(f'Got error: {str(_e)}')
        _err = traceback.format_exc()
        print(_err)
        return web.json_response({'message': f'Failure: {_err}'}, status=500)


@routes.patch('/web-query')
@login_required
async def web_query_abort(request):
    # todo: abort an enqueued query
    pass


@routes.post('/web-query')
@login_required
async def web_query_grab(request):
    """
        Grab Query / result from DB from the browser.

    :return:
    """

    # get session:
    session = await get_session(request)
    user = session['user_id']

    # get query
    _data = await request.json()
    # print(_data)

    try:
        task_id = _data['task_id']
        part = _data['part']
        save = _data['save']

        # query = await request.app['mongo'].queries.find_one({'user': user, 'task_id': {'$eq': task_id}})
        # print(query)

        if part == 'task':
            task_file = os.path.join(config['path']['path_queries'], user, f'{task_id}.task.json')

            # check result file size in bytes:
            task_file_size = os.path.getsize(task_file)

            if save or (task_file_size / 1e6 < 10):
                # with open(task_file, 'r') as f_task_file:
                async with aiofiles.open(task_file, 'r') as f_task_file:
                    return web.json_response(await f_task_file.read(), status=200)
            else:
                return web.json_response({'message': f'query {task_id} size of ' +
                                                     f'{task_file_size / 1e6} MB too large for browser'},
                                         status=200)

        elif part == 'result':
            task_result_file = os.path.join(config['path']['path_queries'], user, f'{task_id}.result.json')

            # check result file size in bytes:
            task_result_file_size = os.path.getsize(task_result_file)

            if save or (task_result_file_size / 1e6 < 10):
                async with aiofiles.open(task_result_file, 'r') as f_task_result_file:
                    return web.json_response(await f_task_result_file.read(), status=200)
            else:
                return web.json_response({'message': f'query {task_id} result size of ' +
                                                     f'{task_result_file_size/1e6} MB too large for browser'},
                                         status=200)

        else:
            return web.json_response({'message': f'Failure: part not recognized'}, status=500)

    except Exception as _e:
        print(f'Got error: {str(_e)}')
        _err = traceback.format_exc()
        print(_err)
        return web.json_response({'message': f'Failure: {_err}'}, status=500)


''' ZTF Alert Lab API '''


@routes.get('/lab/ztf-alerts', name='ztf-alerts')
@login_required
async def ztf_alert_get_handler(request):
    """
        Serve docs page for the browser
    :param request:
    :return:
    """
    # get session:
    session = await get_session(request)

    context = {'logo': config['server']['logo'],
               'user': session['user_id']}
    response = aiohttp_jinja2.render_template('template-lab-ztf-alerts.html',
                                              request,
                                              context)
    return response


def assemble_lc(dflc, objectId, composite=False, match_radius_arcsec=1.5, star_galaxy_threshold=0.4):
    # mjds:
    dflc['mjd'] = dflc.jd - 2400000.5

    dflc['datetime'] = dflc['mjd'].apply(lambda x: mjd_to_datetime(x))
    # strings for plotly:
    dflc['dt'] = dflc['datetime'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    dflc.sort_values(by=['mjd'], inplace=True)

    # fractional days ago
    dflc['days_ago'] = dflc['datetime'].apply(lambda x:
                                              (datetime.datetime.utcnow() - x).total_seconds() / 86400.)

    if is_star(dflc, match_radius_arcsec=match_radius_arcsec, star_galaxy_threshold=star_galaxy_threshold):
        # print('It is a star!')
        # variable object/star? take into account flux in ref images:
        lc = []

        # fix old alerts:
        dflc.replace('None', np.nan, inplace=True)

        # prior to 2018-11-12, non-detections don't have field and rcid in the alert packet,
        # which makes inferring upper limits more difficult
        # fix using pdiffimfilename:
        w = dflc.rcid.isnull()
        if np.sum(w):
            dflc.loc[w, 'rcid'] = dflc.loc[w, 'pdiffimfilename'].apply(lambda x:
                                                      ccd_quad_2_rc(ccd=int(os.path.basename(x).split('_')[4][1:]),
                                                                    quad=int(os.path.basename(x).split('_')[6][1:])))
            dflc.loc[w, 'field'] = dflc.loc[w, 'pdiffimfilename'].apply(lambda x:
                                                                        int(os.path.basename(x).split('_')[2][1:]))

        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        #     print(dflc[['fid', 'field', 'rcid', 'magnr']])

        grp = dflc.groupby(['fid', 'field', 'rcid'])
        impute_magnr = grp['magnr'].agg(lambda x: np.median(x[np.isfinite(x)]))
        # print(impute_magnr)
        impute_sigmagnr = grp['sigmagnr'].agg(lambda x: np.median(x[np.isfinite(x)]))
        # print(impute_sigmagnr)

        for idx, grpi in grp:
            w = np.isnan(grpi['magnr'])
            w2 = grpi[w].index
            dflc.loc[w2, 'magnr'] = impute_magnr[idx]
            dflc.loc[w2, 'sigmagnr'] = impute_sigmagnr[idx]

        # print(dflc)

        dflc['sign'] = 2 * (dflc['isdiffpos'] == 't') - 1

        # from ztf_pipelines_deliverables, reference image zps are fixed

        ref_zps = {1: 26.325, 2: 26.275, 3: 25.660}

        dflc['magzpref'] = dflc['fid'].apply(lambda x: ref_zps[x])

        # 'magzpsci' was not there in older alerts
        if 'magzpsci' in dflc.columns:
            w = dflc.magzpsci.isnull()
            dflc.loc[w, 'magzpsci'] = dflc.loc[w, 'magzpref']
        else:
            dflc['magzpsci'] = dflc['magzpref']

        # fixme?: see email from Frank Masci from Feb 7, 2019
        dflc['ref_flux'] = 10 ** (0.4 * (dflc['magzpsci'] - dflc['magnr']))
        # dflc['ref_flux'] = 10 ** (0.4 * (dflc['magzpref'] - dflc['magnr']))

        dflc['ref_sigflux'] = dflc['sigmagnr'] / 1.0857 * dflc['ref_flux']

        dflc['difference_flux'] = 10 ** (0.4 * (dflc['magzpsci'] - dflc['magpsf']))
        dflc['difference_sigflux'] = dflc['sigmapsf'] / 1.0857 * dflc['difference_flux']

        dflc['dc_flux'] = dflc['ref_flux'] + dflc['sign'] * dflc['difference_flux']
        # errors are correlated, so these are conservative choices
        w = dflc['difference_sigflux'] > dflc['ref_sigflux']
        dflc.loc[w, 'dc_sigflux'] = np.sqrt(dflc.loc[w, 'difference_sigflux'] ** 2 - dflc.loc[w, 'ref_sigflux'] ** 2)
        dflc.loc[~w, 'dc_sigflux'] = np.sqrt(dflc.loc[~w, 'difference_sigflux'] ** 2 + dflc.loc[~w, 'ref_sigflux'] ** 2)

        w_dc_flux_good = dflc['dc_flux'] > 0
        dflc.loc[w_dc_flux_good, 'dc_mag'] = dflc.loc[w_dc_flux_good, 'magzpsci'] - \
                                             2.5 * np.log10(dflc.loc[w_dc_flux_good, 'dc_flux'])
        dflc.loc[w_dc_flux_good, 'dc_sigmag'] = dflc.loc[w_dc_flux_good, 'dc_sigflux'] / \
                                                dflc.loc[w_dc_flux_good, 'dc_flux'] * 1.0857
        # print(dflc[['dc_mag', 'difference_flux', 'dc_flux', 'ref_flux', 'sign']])

        # if we have a nondetection that means that there's no flux +/- 5 sigma from the ref flux
        # (unless it's a bad subtraction)
        dflc['difference_fluxlim'] = 10 ** (0.4 * (dflc['magzpsci'] - dflc['diffmaglim']))
        dflc['dc_flux_ulim'] = dflc['ref_flux'] + dflc['difference_fluxlim']
        dflc['dc_flux_llim'] = dflc['ref_flux'] - dflc['difference_fluxlim']
        w_dc_flux_ulim_good = dflc['dc_flux_ulim'] > 0
        w_dc_flux_llim_good = dflc['dc_flux_llim'] > 0
        dflc.loc[w_dc_flux_ulim_good, 'dc_mag_ulim'] = dflc.loc[w_dc_flux_ulim_good, 'magzpsci'] - \
                                                       2.5 * np.log10(dflc.loc[w_dc_flux_ulim_good, 'dc_flux_ulim'])
        dflc.loc[w_dc_flux_llim_good, 'dc_mag_llim'] = dflc.loc[w_dc_flux_llim_good, 'magzpsci'] - \
                                                       2.5 * np.log10(dflc.loc[w_dc_flux_llim_good, 'dc_flux_llim'])
        # print(dflc['dc_mag_ulim'])

        # print(dflc[['magzpref', 'magzpsci', 'ref_flux', 'ref_sigflux', 'difference_flux', 'difference_sigflux']])

        # if some of the above produces NaNs for some reason, try fixing it sloppy way:
        for fid in (1, 2, 3):
            if fid in dflc.fid.values:
                ref_flux = None
                w = (dflc.fid == fid) & ~dflc.magpsf.isnull() & (dflc.distnr <= match_radius_arcsec)
                if np.sum(w):
                    ref_mag = np.float64(dflc.loc[w].iloc[0]['magnr'])
                    ref_flux = np.float64(10 ** (0.4 * (27 - ref_mag)))
                    # print(fid, ref_mag, ref_flux)

                wnodet_old = (dflc.fid == fid) & dflc.magpsf.isnull() & \
                             dflc.dc_mag_ulim.isnull() & (dflc.diffmaglim > 0)

                if np.sum(wnodet_old) and (ref_flux is not None):
                    # if we have a non-detection that means that there's no flux +/- 5 sigma from
                    # the ref flux (unless it's a bad subtraction)
                    dflc.loc[wnodet_old, 'difference_fluxlim'] = 10 ** (0.4 * (27 - dflc.loc[wnodet_old, 'diffmaglim']))
                    dflc.loc[wnodet_old, 'dc_flux_ulim'] = ref_flux + dflc.loc[wnodet_old, 'difference_fluxlim']
                    dflc.loc[wnodet_old, 'dc_flux_llim'] = ref_flux - dflc.loc[wnodet_old, 'difference_fluxlim']

                    # mask bad values:
                    w_u_good = (dflc.fid == fid) & dflc.magpsf.isnull() & \
                               dflc.dc_mag_ulim.isnull() & (dflc.diffmaglim > 0) & (dflc.dc_flux_ulim > 0)
                    w_l_good = (dflc.fid == fid) & dflc.magpsf.isnull() & \
                               dflc.dc_mag_ulim.isnull() & (dflc.diffmaglim > 0) & (dflc.dc_flux_llim > 0)

                    dflc.loc[w_u_good, 'dc_mag_ulim'] = 27 - 2.5 * np.log10(dflc.loc[w_u_good, 'dc_flux_ulim'])
                    dflc.loc[w_l_good, 'dc_mag_llim'] = 27 - 2.5 * np.log10(dflc.loc[w_l_good, 'dc_flux_llim'])

        # corrections done, now proceed with assembly
        for fid in (1, 2, 3):
            # print(fid)
            # get detections in this filter:
            w = (dflc.fid == fid) & ~dflc.magpsf.isnull()
            lc_dets = pd.concat([dflc.loc[w, 'jd'], dflc.loc[w, 'dt'], dflc.loc[w, 'days_ago'],
                                 dflc.loc[w, 'mjd'], dflc.loc[w, 'dc_mag'], dflc.loc[w, 'dc_sigmag']],
                                axis=1, ignore_index=True, sort=False) if np.sum(w) else None
            if lc_dets is not None:
                lc_dets.columns = ['jd', 'dt', 'days_ago', 'mjd', 'mag', 'magerr']

            wnodet = (dflc.fid == fid) & dflc.magpsf.isnull()
            # print(wnodet)

            lc_non_dets = pd.concat([dflc.loc[wnodet, 'jd'], dflc.loc[wnodet, 'dt'], dflc.loc[wnodet, 'days_ago'],
                                     dflc.loc[wnodet, 'mjd'], dflc.loc[wnodet, 'dc_mag_llim'],
                                     dflc.loc[wnodet, 'dc_mag_ulim']],
                                    axis=1, ignore_index=True, sort=False) if np.sum(wnodet) else None
            if lc_non_dets is not None:
                lc_non_dets.columns = ['jd', 'dt', 'days_ago', 'mjd', 'mag_llim', 'mag_ulim']

            if lc_dets is None and lc_non_dets is None:
                continue

            lc_joint = None

            if lc_dets is not None:
                # print(lc_dets)
                # print(lc_dets.to_dict('records'))
                lc_joint = lc_dets
            if lc_non_dets is not None:
                # print(lc_non_dets.to_dict('records'))
                lc_joint = lc_non_dets if lc_joint is None else pd.concat([lc_joint, lc_non_dets],
                                                                          axis=0, ignore_index=True, sort=False)

            # sort by date and fill NaNs with zeros
            lc_joint.sort_values(by=['mjd'], inplace=True)
            # print(lc_joint)
            lc_joint = lc_joint.fillna(0)

            # single or multiple alert packets used?
            lc_id = f"{objectId}_composite_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}" \
                if composite else f"{objectId}_{int(dflc.loc[0, 'candid'])}"
            # print(lc_id)

            lc_save = {"telescope": "PO:1.2m",
                       "instrument": "ZTF",
                       "filter": fid,
                       "source": "alert_stream",
                       "comment": "corrected for flux in reference image",
                       "id": lc_id,
                       "lc_type": "temporal",
                       "data": lc_joint.to_dict('records')
                       }
            lc.append(lc_save)

    else:
        # print('Not a star!')
        # not a star (transient): up to three individual lcs
        lc = []

        for fid in (1, 2, 3):
            # print(fid)
            # get detections in this filter:
            w = (dflc.fid == fid) & ~dflc.magpsf.isnull()
            lc_dets = pd.concat([dflc.loc[w, 'jd'], dflc.loc[w, 'dt'], dflc.loc[w, 'days_ago'],
                                 dflc.loc[w, 'mjd'], dflc.loc[w, 'magpsf'], dflc.loc[w, 'sigmapsf']],
                                axis=1, ignore_index=True, sort=False) if np.sum(w) else None
            if lc_dets is not None:
                lc_dets.columns = ['jd', 'dt', 'days_ago', 'mjd', 'mag', 'magerr']

            wnodet = (dflc.fid == fid) & dflc.magpsf.isnull()

            lc_non_dets = pd.concat([dflc.loc[wnodet, 'jd'], dflc.loc[wnodet, 'dt'], dflc.loc[wnodet, 'days_ago'],
                                     dflc.loc[wnodet, 'mjd'], dflc.loc[wnodet, 'diffmaglim']],
                                    axis=1, ignore_index=True, sort=False) if np.sum(wnodet) else None
            if lc_non_dets is not None:
                lc_non_dets.columns = ['jd', 'dt', 'days_ago', 'mjd', 'mag_ulim']

            if lc_dets is None and lc_non_dets is None:
                continue

            lc_joint = None

            if lc_dets is not None:
                # print(lc_dets)
                # print(lc_dets.to_dict('records'))
                lc_joint = lc_dets
            if lc_non_dets is not None:
                # print(lc_non_dets.to_dict('records'))
                lc_joint = lc_non_dets if lc_joint is None else pd.concat([lc_joint, lc_non_dets],
                                                                          axis=0, ignore_index=True, sort=False)

            # sort by date and fill NaNs with zeros
            lc_joint.sort_values(by=['mjd'], inplace=True)
            # print(lc_joint)
            lc_joint = lc_joint.fillna(0)

            # single or multiple alert packets used?
            lc_id = f"{objectId}_composite_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}" \
                if composite else f"{objectId}_{int(dflc.loc[0, 'candid'])}"
            # print(lc_id)

            lc_save = {"telescope": "PO:1.2m",
                       "instrument": "ZTF",
                       "filter": fid,
                       "source": "alert_stream",
                       "comment": "no corrections applied. using raw magpsf, sigmapsf, and diffmaglim",
                       "id": lc_id,
                       "lc_type": "temporal",
                       "data": lc_joint.to_dict('records')
                       }
            lc.append(lc_save)

    # print(lc)
    return lc


@routes.get('/lab/ztf-alerts/{candid}')
@login_required
async def ztf_alert_get_handler(request):
    """
        Serve alert page for the browser
    :param request:
    :return:
    """
    # get session:
    session = await get_session(request)

    candid = int(request.match_info['candid'])

    # pass as optional get params to control is_star
    match_radius_arcsec = float(request.query.get('match_radius_arcsec', 1.5))
    star_galaxy_threshold = float(request.query.get('star_galaxy_threshold', 0.4))

    # alert = await request.app['mongo']['ZTF_alerts'].find_one({'candid': candid},
    #                                                           {'cutoutScience': 0,
    #                                                            'cutoutTemplate': 0,
    #                                                            'cutoutDifference': 0})
    alert = await request.app['mongo']['ZTF_alerts'].find_one({'candid': candid})

    download = request.query.get('download', None)
    # frmt = request.query.get('format', 'web')
    # print(frmt)

    if download is not None:

        if download == 'alert':
            return web.json_response(alert, status=200, dumps=dumps)

        elif download == 'lc_alert':
            dflc = make_dataframe(alert)
            lc_candid = assemble_lc(dflc, objectId=alert['objectId'], composite=False,
                                    match_radius_arcsec=match_radius_arcsec,
                                    star_galaxy_threshold=star_galaxy_threshold)
            return web.json_response(lc_candid, status=200, dumps=dumps)

        elif download == 'lc_object':
            obj = await request.app['mongo']['ZTF_alerts'].find({'objectId': alert['objectId']},
                                                                {'cutoutScience': 0,
                                                                 'cutoutTemplate': 0,
                                                                 'cutoutDifference': 0}).to_list(length=None)
            dflc = make_dataframe(obj)
            lc_object = assemble_lc(dflc, objectId=alert['objectId'], composite=True,
                                    match_radius_arcsec=match_radius_arcsec,
                                    star_galaxy_threshold=star_galaxy_threshold)
            return web.json_response(lc_object, status=200, dumps=dumps)

    if alert is not None and (len(alert) > 0):
        # make packet light curve
        dflc = make_dataframe(alert)
        lc_alert = assemble_lc(dflc, objectId=alert['objectId'], composite=False,
                               match_radius_arcsec=match_radius_arcsec,
                               star_galaxy_threshold=star_galaxy_threshold)

        # pre-process for plotly:
        lc_candid = []
        for lc_ in lc_alert:
            lc__ = {'lc_det': {'dt': [], 'days_ago': [], 'jd': [], 'mjd': [], 'mag': [], 'magerr': []},
                    'lc_nodet_u': {'dt': [], 'days_ago': [], 'jd': [], 'mjd': [], 'mag_ulim': []},
                    'lc_nodet_l': {'dt': [], 'days_ago': [], 'jd': [], 'mjd': [], 'mag_llim': []}}
            for dp in lc_['data']:
                if ('mag_ulim' in dp) and (dp['mag_ulim'] > 0.01):
                    for kk in ('dt', 'days_ago', 'jd', 'mjd', 'mag_ulim'):
                        lc__['lc_nodet_u'][kk].append(dp[kk])
                if ('mag_llim' in dp) and (dp['mag_llim'] > 0.01):
                    for kk in ('dt', 'days_ago', 'jd', 'mjd', 'mag_llim'):
                        lc__['lc_nodet_l'][kk].append(dp[kk])
                if ('mag' in dp) and (dp['mag'] > 0.01):
                    for kk in ('dt', 'days_ago', 'jd', 'mjd', 'mag', 'magerr'):
                        lc__['lc_det'][kk].append(dp[kk])
            lc_['data'] = lc__
            lc_candid.append(lc_)

        # todo: make composite light curve from all packets for alert['objectId']
        obj = await request.app['mongo']['ZTF_alerts'].find({'objectId': alert['objectId']},
                                                            {'cutoutScience': 0,
                                                             'cutoutTemplate': 0,
                                                             'cutoutDifference': 0}).to_list(length=None)
        # print([o['_id'] for o in obj])
        dflc = make_dataframe(obj)
        lc_obj = assemble_lc(dflc, objectId=alert['objectId'], composite=True,
                             match_radius_arcsec=match_radius_arcsec,
                             star_galaxy_threshold=star_galaxy_threshold)

        # pre-process for plotly:
        lc_object = []
        for lc_ in lc_obj:
            lc__ = {'lc_det': {'dt': [], 'days_ago': [], 'jd': [], 'mjd': [], 'mag': [], 'magerr': []},
                    'lc_nodet_u': {'dt': [], 'days_ago': [], 'jd': [], 'mjd': [], 'mag_ulim': []},
                    'lc_nodet_l': {'dt': [], 'days_ago': [], 'jd': [], 'mjd': [], 'mag_llim': []}}
            for dp in lc_['data']:
                if ('mag_ulim' in dp) and (dp['mag_ulim'] > 0.01):
                    for kk in ('dt', 'days_ago', 'jd', 'mjd', 'mag_ulim'):
                        lc__['lc_nodet_u'][kk].append(dp[kk])
                if ('mag_llim' in dp) and (dp['mag_llim'] > 0.01):
                    for kk in ('dt', 'days_ago', 'jd', 'mjd', 'mag_llim'):
                        lc__['lc_nodet_l'][kk].append(dp[kk])
                if ('mag' in dp) and (dp['mag'] > 0.01):
                    for kk in ('dt', 'days_ago', 'jd', 'mjd', 'mag', 'magerr'):
                        lc__['lc_det'][kk].append(dp[kk])
            lc_['data'] = lc__
            lc_object.append(lc_)

        # print(lc_candid)

        context = {'logo': config['server']['logo'],
                   'user': session['user_id'],
                   'match_radius_arcsec': match_radius_arcsec,
                   'star_galaxy_threshold': star_galaxy_threshold,
                   'alert': alert,
                   'lc_candid': lc_candid,
                   'lc_object': lc_object}
        response = aiohttp_jinja2.render_template('template-lab-ztf-alert.html',
                                                  request,
                                                  context)
        return response

    else:
        # redirect to alerts lab page
        location = request.app.router['ztf-alerts'].url_for()
        raise web.HTTPFound(location=location)


@routes.get('/lab/ztf-alerts/{candid}/cutout/{cutout}')
@login_required
async def ztf_alert_get_cutout_handler(request):
    """
        Serve docs page for the browser
    :param request:
    :return:
    """
    # get session:
    session = await get_session(request)

    candid = int(request.match_info['candid'])
    cutout = request.match_info['cutout'].capitalize()

    assert cutout in ['Science', 'Template', 'Difference']

    alert = await request.app['mongo']['ZTF_alerts'].find_one({'candid': candid},
                                                              {f'cutout{cutout}': 1})

    cutout_data = loads(dumps([alert[f'cutout{cutout}']['stampData']]))[0]

    # unzipped fits name
    fits_name = pathlib.Path(alert[f"cutout{cutout}"]["fileName"]).with_suffix('')

    # unzip and flip about y axis on the server side
    with gzip.open(io.BytesIO(cutout_data), 'rb') as f:
        with fits.open(io.BytesIO(f.read())) as hdu:
            header = hdu[0].header
            data_flipped_y = np.flipud(hdu[0].data)

    hdu = fits.PrimaryHDU(data_flipped_y, header=header)
    # hdu = fits.PrimaryHDU(data_flipped_y)
    hdul = fits.HDUList([hdu])

    stamp_fits = io.BytesIO()
    hdul.writeto(fileobj=stamp_fits)

    return web.Response(body=stamp_fits.getvalue(), content_type='image/fits',
                        headers=MultiDict({'Content-Disposition': f'Attachment;filename={fits_name}'}), )


@routes.post('/lab/ztf-alerts')
@login_required
async def ztf_alert_post_handler(request):
    """
        Process query to own ZTF_alerts db from browser

        714104325815015060
        [('22:08:59.5326', '57:23:54.941')]
    :param request:
    :return:
    """
    # get session:
    session = await get_session(request)

    try:
        _query = await request.json()
    except Exception as _e:
        print(f'Cannot extract json() from request, trying post(): {str(_e)}')
        # _err = traceback.format_exc()
        # print(_err)
        _query = await request.post()
    # print(_query)

    try:
        # parse query
        q = dict()

        # filter set?
        if len(_query['filter']) > 2:
            # construct filter
            _filter = _query['filter']
            if isinstance(_filter, str):
                # passed string? evaluate:
                _filter = literal_eval(_filter.strip())
            elif isinstance(_filter, dict):
                # passed dict?
                _filter = _filter
            else:
                raise ValueError('Unsupported filter specification')

            q = {**q, **_filter}

        # cone search?
        if len(_query['cone_search_radius']) > 0 and len(_query['radec']) > 8:
            cone_search_radius = float(_query['cone_search_radius'])
            # convert to rad:
            if _query['cone_search_unit'] == 'arcsec':
                cone_search_radius *= np.pi / 180.0 / 3600.
            elif _query['cone_search_unit'] == 'arcmin':
                cone_search_radius *= np.pi / 180.0 / 60.
            elif _query['cone_search_unit'] == 'deg':
                cone_search_radius *= np.pi / 180.0
            elif _query['cone_search_unit'] == 'rad':
                cone_search_radius *= 1
            else:
                raise Exception('Unknown cone search unit. Must be in [deg, rad, arcsec, arcmin]')

            # parse coordinate list

            # comb radecs for single sources as per Tom's request:
            radec = _query['radec'].strip()
            if radec[0] not in ('[', '(', '{'):
                ra, dec = radec.split()
                if ('s' in radec) or (':' in radec):
                    radec = f"[('{ra}', '{dec}')]"
                else:
                    radec = f"[({ra}, {dec})]"

            # print(task['object_coordinates']['radec'])
            objects = literal_eval(radec)
            # print(type(objects), isinstance(objects, dict), isinstance(objects, list))

            # this could either be list [(ra1, dec1), (ra2, dec2), ..] or dict {'name': (ra1, dec1), ...}
            if isinstance(objects, list):
                object_coordinates = objects
                object_names = [str(obj_crd) for obj_crd in object_coordinates]
            elif isinstance(objects, dict):
                object_names, object_coordinates = zip(*objects.items())
                object_names = list(map(str, object_names))
            else:
                raise ValueError('Unsupported type of object coordinates')

            # print(object_names, object_coordinates)

            object_position_query = dict()
            object_position_query['$or'] = []

            for oi, obj_crd in enumerate(object_coordinates):
                # convert ra/dec into GeoJSON-friendly format
                # print(obj_crd)
                _ra, _dec = radec_str2geojson(*obj_crd)
                # print(str(obj_crd), _ra, _dec)

                object_position_query['$or'].append({'coordinates.radec_geojson':
                                                         {'$geoWithin': {'$centerSphere': [[_ra, _dec],
                                                                                           cone_search_radius]}}})

            q = {**q, **object_position_query}
            q = {'$and': [q]}

        # print(q)
        if len(q) == 0:
            context = {'logo': config['server']['logo'],
                       'user': session['user_id'],
                       'data': [],
                       'form': _query,
                       'messages': [[f'Empty query', 'danger']]}

        else:

            # do not fetch/pass cutouts in bulk
            # alerts = await request.app['mongo']['ZTF_alerts'].find(q, {'cutoutScience': 0,
            #                                                            'cutoutTemplate': 0,
            #                                                            'cutoutDifference': 0}). \
            #     sort([('candidate.jd', -1)]).to_list(length=None)
            alerts = await request.app['mongo']['ZTF_alerts'].find(q, {'prv_candidates': 0,
                                                                       'cutoutScience': 0,
                                                                       'cutoutTemplate': 0,
                                                                       'cutoutDifference': 0}).to_list(length=None)

            context = {'logo': config['server']['logo'],
                       'user': session['user_id'],
                       'alerts': alerts,
                       'form': _query}

            if len(alerts) == 0:
                context['messages'] = [['No alerts found', 'info']]

        response = aiohttp_jinja2.render_template('template-lab-ztf-alerts.html',
                                                  request,
                                                  context)
        return response

    except Exception as _e:

        print(f'Error: {str(_e)}')

        context = {'logo': config['server']['logo'],
                   'user': session['user_id'],
                   'alerts': [],
                   'form': _query,
                   'messages': [[f'Error: {str(_e)}', 'danger']]}

        response = aiohttp_jinja2.render_template('template-lab-ztf-alerts.html',
                                                  request,
                                                  context)

        return response


''' web endpoints '''


@routes.get('/my-queries')
@login_required
async def my_queries_handler(request):
    """
        Serve my-queries page for the browser
    :param request:
    :return:
    """
    # get session:
    session = await get_session(request)

    # grab user tasks:
    user_tasks = await request.app['mongo'].queries.find({'user': session['user_id']},
                                                         {'task_id': 1, 'status': 1, 'created': 1, 'expires': 1,
                                                          'last_modified': 1}).sort('created', -1).to_list(length=1000)

    context = {'logo': config['server']['logo'],
               'user': session['user_id'],
               'user_tasks': user_tasks}
    response = aiohttp_jinja2.render_template('template-my-queries.html',
                                              request,
                                              context)
    return response


@routes.get('/query-cone-search')
@login_required
async def query_cone_search_handler(request):
    """
        Serve CS page for the browser
    :param request:
    :return:
    """
    # get session:
    session = await get_session(request)

    # get available catalog names
    catalogs = await request.app['mongo'].list_collection_names()
    # exclude system collections and collections without a 2dsphere index
    catalogs_system = (config['database']['collection_users'],
                       config['database']['collection_queries'],
                       config['database']['collection_stats'])
    catalogs_no_2d_index = ('ZTF_exposures_20181220',
                            'ZTF_sources_20181220',
                            'Gaia_DR2_light_curves')
    catalogs = [c for c in sorted(catalogs)[::-1] if c not in catalogs_system + catalogs_no_2d_index]

    context = {'logo': config['server']['logo'],
               'user': session['user_id'],
               'catalogs': catalogs}
    response = aiohttp_jinja2.render_template('template-query-cone-search.html',
                                              request,
                                              context)
    return response


@routes.get('/query-general-search')
@login_required
async def query_general_search_handler(request):
    """
        Serve GS page for the browser
    :param request:
    :return:
    """
    # get session:
    session = await get_session(request)

    context = {'logo': config['server']['logo'],
               'user': session['user_id']}
    response = aiohttp_jinja2.render_template('template-query-general-search.html',
                                              request,
                                              context)
    return response


@routes.get('/docs')
@login_required
async def docs_handler(request):
    """
        Serve docs page for the browser
    :param request:
    :return:
    """
    # get session:
    session = await get_session(request)

    # todo?

    context = {'logo': config['server']['logo'],
               'user': session['user_id']}
    response = aiohttp_jinja2.render_template('template-docs.html',
                                              request,
                                              context)
    return response


@routes.get('/docs/{doc}')
@login_required
async def doc_handler(request):
    """
        Serve doc page for the browser
    :param request:
    :return:
    """
    # get session:
    session = await get_session(request)

    doc = request.match_info['doc']

    title = doc.replace('_', ' ').capitalize()

    # render doc with misaka
    with open(os.path.join(config['path']['path_docs'],
                           doc + '.md'), 'r') as f:
        tut = f.read()

    content = md(tut)

    context = {'logo': config['server']['logo'],
               'user': session['user_id'],
               'title': title,
               'content': content}
    response = aiohttp_jinja2.render_template('template-doc.html',
                                              request,
                                              context)
    return response


'''
# how to use async timeout
async with timeout(1.5) as cm:
    await inner()
print(cm.expired)
'''


async def app_factory():
    """
        App Factory
    :return:
    """

    # init db if necessary
    await init_db()

    # Database connection
    client = AsyncIOMotorClient(f"mongodb://{config['database']['user']}:{config['database']['pwd']}@" +
                                f"{config['database']['host']}:{config['database']['port']}/{config['database']['db']}")
    mongo = client[config['database']['db']]

    # add site admin if necessary
    await add_admin(mongo)

    # init app with auth middleware
    app = web.Application(middlewares=[auth_middleware])

    # store mongo connection
    app['mongo'] = mongo

    # mark all enqueued tasks failed on startup
    await app['mongo'].queries.update_many({'status': 'enqueued'},
                                           {'$set': {'status': 'failed', 'last_modified': utc_now()}})

    # graciously close mongo client on shutdown
    async def close_mongo(app):
        app['mongo'].client.close()

    app.on_cleanup.append(close_mongo)

    # set up JWT for user authentication/authorization
    app['JWT'] = {'JWT_SECRET': config['server']['JWT_SECRET_KEY'],
                  'JWT_ALGORITHM': 'HS256',
                  'JWT_EXP_DELTA_SECONDS': 30 * 86400 * 3}

    # render templates with jinja2
    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader('./templates'))

    # set up browser sessions
    fernet_key = config['misc']['fernet_key'].encode()
    secret_key = base64.urlsafe_b64decode(fernet_key)
    setup(app, EncryptedCookieStorage(secret_key))

    # route table
    # app.add_routes([web.get('/', hello)])
    app.add_routes(routes)

    # static files
    app.add_routes([web.static('/static', './static')])

    # data files
    app.add_routes([web.static('/data', '/data')])

    return app


''' Tests '''


class TestAPIs(object):
    # python -m pytest -s server.py
    # python -m pytest server.py

    # test user management API for admin
    async def test_users(self, aiohttp_client):
        client = await aiohttp_client(await app_factory())

        login = await client.post('/login', json={"username": config['server']['admin_username'],
                                                  "password": config['server']['admin_password']})
        # print(login)
        assert login.status == 200

        # test = await client.get('/lab/ztf-alerts')
        # print(test)

        # adding a user
        resp = await client.put('/users', json={'user': 'test_user', 'password': random_alphanumeric_str(6)})
        assert resp.status == 200
        # text = await resp.text()
        # text = await resp.json()

        # editing user credentials
        resp = await client.post('/users', json={'_user': 'test_user',
                                                 'edit-user': 'test_user',
                                                 'edit-password': random_alphanumeric_str(6)})
        assert resp.status == 200
        resp = await client.post('/users', json={'_user': 'test_user',
                                                 'edit-user': 'test_user_edited',
                                                 'edit-password': ''})
        assert resp.status == 200

        # deleting a user
        resp = await client.delete('/users', json={'user': 'test_user_edited'})
        assert resp.status == 200

    # test programmatic query API
    async def test_query(self, aiohttp_client):
        client = await aiohttp_client(await app_factory())

        # check JWT authorization
        auth = await client.post(f'/auth',
                                 json={"username": config['server']['admin_username'],
                                       "password": config['server']['admin_password']})
        assert auth.status == 200
        # print(await auth.text())
        # print(await auth.json())
        credentials = await auth.json()
        assert 'token' in credentials

        access_token = credentials['token']

        headers = {'Authorization': access_token}

        collection = 'ZTF_alerts'

        # check query without book-keeping
        qu = {"query_type": "general_search",
              "query": f"db['{collection}'].find_one({{}}, {{'_id': 1}})",
              "kwargs": {"save": False}
              }
        # print(qu)
        resp = await client.put('/query', json=qu, headers=headers, timeout=1)
        assert resp.status == 200
        result = await resp.json()
        assert result['status'] == 'done'

        # check query with book-keeping
        qu = {"query_type": "general_search",
              "query": f"db['{collection}'].find_one({{}}, {{'_id': 1}})",
              "kwargs": {"enqueue_only": True, "_id": random_alphanumeric_str(32)}
              }
        # print(qu)
        resp = await client.put('/query', json=qu, headers=headers, timeout=0.15)
        assert resp.status == 200
        result = await resp.json()
        # print(result)
        assert result['status'] == 'enqueued'

        # remove enqueued query
        resp = await client.delete('/query', json={'task_id': result['query_id']}, headers=headers, timeout=1)
        assert resp.status == 200
        result = await resp.json()
        assert result['message'] == 'success'


if __name__ == '__main__':

    web.run_app(app_factory(), port=config['server']['port'])
