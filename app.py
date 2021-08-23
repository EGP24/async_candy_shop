from aiohttp import web
from os import environ

from data.db_functions import client_session_initializer, db_engine_initializer, db_session_initializer
from services.courier_service import CourierService
from services.order_service import OrderService
from handlers.courier_handler import CourierHandler
from handlers.order_handler import OrderHandler


def create_app() -> web.Application:
    app = web.Application()

    order_handler = OrderHandler(OrderService())
    courier_handler = CourierHandler(CourierService())

    app.cleanup_ctx.extend([
        client_session_initializer,
        db_engine_initializer,
        db_session_initializer
    ])

    app.add_routes([
        web.post('/orders', order_handler.create_orders),
        web.post('/orders/assign', order_handler.assign_order),
        web.post('/orders/complete', order_handler.complete_order),
        web.post('/couriers', courier_handler.create_couriers),
        web.patch('/couriers/{courier_id}', courier_handler.patch_courier),
        web.get('/couriers/{courier_id}', courier_handler.get_courier_info)
    ])

    return app


if __name__ == '__main__':
    app = create_app()
    web.run_app(app, host=environ.get('HOST'), port=environ.get('PORT'))
