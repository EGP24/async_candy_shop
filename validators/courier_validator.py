from marshmallow import ValidationError
from sqlalchemy.future import select
from aiohttp import web

from validators.validate_schemes import CreateCouriersSchema, PatchCourierSchema, GetCourierInfoSchema
from data.db_functions import query_result, get_courier_by_id
from data.models import CourierType


class CourierValidator:
    @staticmethod
    def validate_create_couriers_service(func):
        async def wrapper(_, request: web.Request):
            async_session = request.app['async_session']
            schema = CreateCouriersSchema()
            request_data = await request.json()
            not_validated_ids = []

            for i, courier_data in enumerate(request_data['data']):
                try:
                    result = schema.load(courier_data)
                    courier = await get_courier_by_id(async_session, result['courier_id'])
                    result['courier_type'] = await query_result(
                        async_session, select(CourierType).where(CourierType.title == result['courier_type']))

                    if not result['courier_type'] or courier:
                        raise ValidationError('')
                except ValidationError:
                    not_validated_ids.append(courier_data['courier_id'])
                    continue

                for key in result:
                    request_data['data'][i][key] = result[key]
            if not_validated_ids:
                return {'validation_error': {'couriers': [{'id': id} for id in not_validated_ids]}}, 400

            return await func(_, async_session, request_data)
        return wrapper

    @staticmethod
    def validate_patch_courier_service(func):
        async def wrapper(_, request: web.Request):
            async_session = request.app['async_session']
            schema = PatchCourierSchema()
            request_data = await request.json()
            request_data['courier_id'] = request.match_info.get('courier_id', None)

            try:
                result = schema.load(request_data)
                courier = result['courier'] = await get_courier_by_id(async_session, result['courier_id'])
                if 'courier_type' in result:
                    courier_type = result['courier_type'] = await query_result(
                        async_session, select(CourierType).where(CourierType.title == result['courier_type']))
                    if not courier_type:
                        raise ValidationError('')
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
    def validate_get_courier_info_service(func):
        async def wrapper(_, request: web.Request):
            async_session = request.app['async_session']
            schema = GetCourierInfoSchema()
            request_data = {}

            try:
                result = schema.load(request.match_info)
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
