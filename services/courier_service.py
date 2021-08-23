from aiohttp import web
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from datetime import datetime
from pprint import pprint

from data.models import Courier, CourierInterval, CourierType, Region
from data.db_functions import save_base, query_result, get_courier_by_id


class CourierService:
    async def create_couriers(self, request: web.Request):
        async_session = request.app['async_session']
        request_data = await request.json()
        validated, not_validated = [], []

        for courier_data in request_data['data']:
            try:
                courier_id = courier_data['courier_id']
                courier_type = courier_data['courier_type']
                regions = courier_data['regions']
                working_hours = courier_data['working_hours']
            except KeyError:
                not_validated.append(courier_id)
                continue

            courier_type = await query_result(async_session, select(CourierType).where(
                CourierType.title == courier_type))
            courier = Courier(id=courier_id, type=courier_type)
            regions = [Region(number_region=region, courier=courier) for region in regions]
            intervals = [CourierInterval(courier=courier, time_start=time, time_end=time) for time in working_hours]
            validated.extend([courier] + intervals + regions)

        if not_validated:
            return {'validation_error': {'couriers': [{'id': id} for id in not_validated]}}, 400

        async_session.add_all(validated)
        await save_base(async_session)
        return {'couriers': [{'id': order.id} for order in filter(lambda x: isinstance(x, Courier), validated)]}, 201

    async def patch_courier(self, request: web.Request):
        async_session = request.app['async_session']
        request_data = await request.json()
        courier_id = int(request.match_info['courier_id'])
        courier = await get_courier_by_id(async_session, courier_id)

        if not courier:
            return None, 400
        if 'courier_type' in request_data:
            courier_type = await query_result(async_session, select(CourierType).where(
                CourierType.title == request_data['courier_type']))
            courier.type = courier_type
        if 'regions' in request_data:
            regions = request_data['regions']
            to_delete_regions = [region for region in courier.regions if region.number_region not in regions]
            to_add_regions_numbers = [region for region in regions if region not in [
                i.number_region for i in courier.regions]]
            to_add_regions = [Region(number_region=region, courier=courier) for region in to_add_regions_numbers]
            for region in to_delete_regions:
                await async_session.delete(region)
                courier.regions.remove(region)
            async_session.add_all(to_add_regions)
            courier.regions.extend(to_add_regions)
        if 'working_hours' in request_data:
            intervals = request_data['working_hours']
            to_delete_intervals = [interval for interval in courier.courier_intervals if str(interval) not in intervals]
            to_add_intervals_strs = [interval for interval in intervals if interval not in [
                str(i) for i in courier.courier_intervals]]
            to_add_intervals = [CourierInterval(courier=courier, time_start=time, time_end=time)
                                for time in to_add_intervals_strs]
            for interval in to_delete_intervals:
                await async_session.delete(interval)
                courier.courier_intervals.remove(interval)
            async_session.add_all(to_add_intervals)
            courier.courier_intervals.extend(to_add_intervals)

        await save_base(async_session)
        return courier.to_dict(), 200

    async def get_courier_info(self, request: web.Request):
        async_session = request.app['async_session']
        courier_id = int(request.match_info['courier_id'])
        courier = await get_courier_by_id(async_session, courier_id)
        if not courier:
            return None, 400

        courier_data = courier.to_dict()
        courier_data['earnings'] = courier.earning
        if courier.earning:
            courier_data['rating'] = courier.rating
        return courier_data, 200
