from marshmallow import ValidationError
from aiohttp import web

from validators.validate_schemes import CreateOrdersSchema, AssignOrderSchema, CompleteOrderSchema
from data.db_functions import get_order_by_id, get_courier_by_id


class OrderValidator:
    @staticmethod
    def validate_create_orders_service(func):
        async def wrapper(_, request: web.Request):
            async_session = request.app['async_session']
            schema = CreateOrdersSchema()
            request_data = await request.json()
            not_validated_ids = []

            for i, order_data in enumerate(request_data['data']):
                try:
                    result = schema.load(order_data)
                    order = await get_order_by_id(async_session, result['order_id'])

                    if order:
                        raise ValidationError('')
                except ValidationError:
                    not_validated_ids.append(order_data['order_id'])
                    continue

                for key in result:
                    request_data['data'][i][key] = result[key]
            if not_validated_ids:
                return {'validation_error': {'orders': [{'id': id} for id in not_validated_ids]}}, 400

            return await func(_, async_session, request_data)
        return wrapper

    @staticmethod
    def validate_assign_order_service(func):
        async def wrapper(_, request: web.Request):
            async_session = request.app['async_session']
            schema = AssignOrderSchema()
            request_data = await request.json()

            try:
                result = schema.load(request_data)
                courier = result['courier'] = await get_courier_by_id(async_session, result['courier_id'])
                if not courier:
                    raise ValidationError('')
            except ValidationError:
                return None, 400

            for key in result:
                request_data[key] = result[key]
            del request_data['courier_id']

            return await func(_, async_session, request_data)
        return wrapper

    @staticmethod
    def validate_complete_order_service(func):
        async def wrapper(_, request: web.Request):
            async_session = request.app['async_session']
            schema = CompleteOrderSchema()
            request_data = await request.json()

            try:
                result = schema.load(request_data)
                courier = result['courier'] = await get_courier_by_id(async_session, result['courier_id'])
                order = result['order'] = await get_order_by_id(async_session, result['order_id'])
                complete_time = result['complete_time'] = result['complete_time'].replace(tzinfo=None)
                if not courier or not order or order.courier != courier or not order.is_assign:
                    raise ValidationError('')
                if courier.time_last_complete_order is None and complete_time <= order.assign_time:
                    raise ValidationError('')
                if courier.time_last_complete_order is not None and complete_time <= courier.time_last_complete_order:
                    raise ValidationError('')
            except ValidationError:
                return None, 400

            for key in result:
                request_data[key] = result[key]
            del request_data['courier_id'], request_data['order_id']

            return await func(_, async_session, request_data)
        return wrapper
