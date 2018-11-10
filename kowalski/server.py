from aiohttp import web
import jinja2
import aiohttp_jinja2
import json
from motor.motor_asyncio import AsyncIOMotorClient

# from kowalski.utils import utc_now, jd, radec_str2geojson
from utils import utc_now, jd, radec_str2geojson, generate_password_hash


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

    ''' init db if necessary '''
    await init_db()

    ''' Database connection '''
    client = AsyncIOMotorClient(f"mongodb://{config['database']['user']}:{config['database']['pwd']}@" +
                                f"{config['database']['host']}:{config['database']['port']}/{config['database']['db']}")
    mongo = client[config['database']['db']]

    # add site admin if necessary
    await add_admin(mongo)

    app = web.Application()

    async def close_mongo(app):
        mongo.client.close()

    app['mongo'] = mongo

    app.on_cleanup.append(close_mongo)

    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader('./templates'))

    # app.add_routes([web.get('/', hello)])
    app.add_routes(routes)

    return app


if __name__ == '__main__':

    web.run_app(app_factory(config), port=config['server']['port'])
