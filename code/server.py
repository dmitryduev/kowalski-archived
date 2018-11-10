from aiohttp import web
import jinja2
import aiohttp_jinja2

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


async def app_factory():
    app = web.Application()

    aiohttp_jinja2.setup(app,
                         loader=jinja2.FileSystemLoader('/Users/dmitryduev/_caltech/python/aiohttp-train/templates'))

    # app.add_routes([web.get('/', hello)])
    app.add_routes(routes)
    return app


if __name__ == '__main__':
    web.run_app(app_factory(), port=4000)
