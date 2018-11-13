from aiohttp import web
import jinja2
import aiohttp_jinja2
from aiohttp_session import setup, get_session, session_middleware
from aiohttp_session.cookie_storage import EncryptedCookieStorage
import json
import base64
from cryptography import fernet
import jwt
from motor.motor_asyncio import AsyncIOMotorClient
import datetime
import time
# import functools

# from kowalski.utils import utc_now, jd, radec_str2geojson, generate_password_hash, check_password_hash
# from .utils import utc_now, jd, radec_str2geojson, generate_password_hash, check_password_hash
from utils import utc_now, jd, radec_str2geojson, generate_password_hash, check_password_hash


''' load config and secrets '''
with open('/app/config.json') as cjson:
    config = json.load(cjson)

with open('/app/secrets.json') as sjson:
    secrets = json.load(sjson)

for k in secrets:
    config[k].update(secrets.get(k, {}))
# print(config)


def json_response(body: dict, **kwargs):
    kwargs['body'] = json.dumps(body or kwargs['body']).encode('utf-8')
    kwargs['content_type'] = 'text/json'
    return web.Response(**kwargs)


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
        _mongo.command('createUser', config['database']['user'],
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
            print(e)


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
            return json_response({'message': 'Token is invalid'}, status=400)

        request.user = payload['user_id']

    response = await handler(request)
    toc = time.time()
    print(f"Auth middleware took {toc-tic} seconds to execute")

    return response


def auth_required(func):
    """
        Wrapper to ensure successful user authorization to use the API
    :param func:
    :return:
    """
    def wrapper(request):
        if not request.user:
            return json_response({'message': 'Auth required'}, status=401)
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
            # return json_response({'message': 'Auth required'}, status=401)
            # redirect to login page
            # location = request.app.router['login'].url_for()
            location = '/login'
            raise web.HTTPFound(location=location)
        else:
            jwt_token = session['jwt_token']
            if not await token_ok(request, jwt_token):
                # return json_response({'message': 'Auth required'}, status=401)
                # redirect to login page
                # location = request.app.router['login'].url_for()
                location = '/login'
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
    post_data = await request.post()
    # post_data = dict(post_data)
    # print(post_data)

    # must contain 'username' and 'password'

    if ('username' not in post_data) or (len(post_data['username']) == 0):
        return json_response({'message': 'Missing "username"'}, status=400)
    if ('password' not in post_data) or (len(post_data['password']) == 0):
        return json_response({'message': 'Missing "password"'}, status=400)

    # # todo: check penquins version
    # penquins_version = flask.request.json.get('penquins.__version__', None)
    # if penquins_version is None or penquins_version not in config['misc']['supported_penquins_versions']:
    #     return flask.jsonify({"msg": "Unsupported version of penquins. " +
    #                                  "Please install the latest version + read current tutorial on Kowalski!"}), 400

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

            return json_response({'token': jwt_token.decode('utf-8')})

        else:
            return json_response({'message': 'Wrong credentials'}, status=400)

    except Exception as e:
        print(str(e))
        return json_response({'message': 'Wrong credentials'}, status=400)


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


@routes.post('/login')
async def login_post(request):
    """
        Server login page for the browser
    :param request:
    :return:
    """
    post_data = await request.post()

    # get session:
    session = await get_session(request)

    if ('username' not in post_data) or (len(post_data['username']) == 0):
        return json_response({'message': 'Missing "username"'}, status=400)
    if ('password' not in post_data) or (len(post_data['password']) == 0):
        return json_response({'message': 'Missing "password"'}, status=400)

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

        location = '/'
        raise web.HTTPFound(location=location)

    else:
        context = {'logo': config['server']['logo'],
                   'messages': [(u'Failed to log in.', u'danger')]}
        response = aiohttp_jinja2.render_template('template-login.html',
                                                  request,
                                                  context)
        return response


@routes.get('/logout')
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
    # location = request.app.router['login'].url_for()
    location = '/login'
    raise web.HTTPFound(location=location)


@routes.get('/test')
@auth_required
async def test_handler(request):
    return json_response({'message': 'test ok.'}, status=200)


@routes.get('/test_wrapper')
@login_required
async def test_wrapper_handler(request):
    return json_response({'message': 'test ok.'}, status=200)


@routes.get('/')
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


# manage users
@routes.get('/users')
@login_required
async def users_get(request):
    """
        Serve home page for the browser
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
        json_response({'message': '403 Forbidden'}, status=403)


async def app_factory(_config):
    """
        App Factory
    :param _config:
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

    # todo: mark all enqueued tasks failed on shutdown
    async def mark_enqueued_tasks_failed(app):
        pass

    app.on_cleanup.append(mark_enqueued_tasks_failed)

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
    fernet_key = fernet.Fernet.generate_key()
    secret_key = base64.urlsafe_b64decode(fernet_key)
    setup(app, EncryptedCookieStorage(secret_key))

    # route table
    # app.add_routes([web.get('/', hello)])
    app.add_routes(routes)

    # static files
    app.add_routes([web.static('/static', './static')])

    return app


if __name__ == '__main__':

    web.run_app(app_factory(config), port=config['server']['port'])
