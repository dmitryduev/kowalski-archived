from aiohttp import web
import jinja2
import aiohttp_jinja2
import json
import jwt
from motor.motor_asyncio import AsyncIOMotorClient
import datetime

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


async def auth_middleware(app, handler):
    async def middleware(request):
        request.user = None
        jwt_token = request.headers.get('authorization', None)
        if jwt_token:
            try:
                payload = jwt.decode(jwt_token, request.app['JWT']['JWT_SECRET'],
                                     algorithms=[request.app['JWT']['JWT_ALGORITHM']])
            except (jwt.DecodeError, jwt.ExpiredSignatureError):
                return json_response({'message': 'Token is invalid'}, status=400)

            request.user = payload['user_id']
        return await handler(request)
    return middleware


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


@routes.get('/hw')
async def hello(request):
    return web.Response(text="Hello, world")


@routes.get('/')
async def handler(request):
    context = {'first_name': 'Dmitry', 'last_name': 'Duev'}
    response = aiohttp_jinja2.render_template('template-root.html',
                                              request,
                                              context)
    # response.headers['Content-Language'] = 'ru'
    return response


async def app_factory(_config):
    """

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

    # init app
    app = web.Application()

    # store mongo connection
    app['mongo'] = mongo

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

    # route table
    # app.add_routes([web.get('/', hello)])
    app.add_routes(routes)

    return app


if __name__ == '__main__':

    web.run_app(app_factory(config), port=config['server']['port'])
