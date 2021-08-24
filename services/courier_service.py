from validators.courier_validator import CourierValidator
from data.models import Courier, CourierInterval, Region
from data.db_functions import save_base


class CourierService:
    @CourierValidator.validate_create_couriers_service
    async def create_couriers(self, async_session, request_data):
        for courier_data in request_data['data']:
            courier_id = courier_data['courier_id']
            courier_type = courier_data['courier_type']
            regions = courier_data['regions']
            working_hours = courier_data['working_hours']

            courier = Courier(id=courier_id, type=courier_type)
            regions = [Region(number_region=region, courier=courier) for region in regions]
            for time_start, time_end in working_hours:
                async_session.add(CourierInterval(courier=courier, time_start=time_start, time_end=time_end))
            async_session.add_all([courier] + regions)

        await save_base(async_session)
        return {'couriers': [{'id': courier['courier_id']} for courier in request_data['data']]}, 201

    @CourierValidator.validate_patch_courier_service
    async def patch_courier(self, async_session, request_data):
        courier = request_data['courier']

        if 'courier_type' in request_data:
            courier.type = request_data['courier_type']
        if 'regions' in request_data:
            regions = request_data['regions']
            courier_region_numbers = {region.number_region: region for region in courier.regions}
            to_delete_regions = [region for number, region in courier_region_numbers.items() if number not in regions]
            for region in regions:
                if region not in courier_region_numbers:
                    region = Region(number_region=region, courier=courier)
                    async_session.add(region)
                    courier.regions.append(region)

            for region in to_delete_regions:
                await async_session.delete(region)
                courier.regions.remove(region)
        if 'working_hours' in request_data:
            intervals = request_data['working_hours']
            for interval in courier.courier_intervals:
                await async_session.delete(interval)
            courier.courier_intervals.clear()
            for time_start, time_end in intervals:
                interval = CourierInterval(courier=courier, time_start=time_start, time_end=time_end)
                async_session.add(interval)
                courier.courier_intervals.append(interval)

        await save_base(async_session)
        return courier.to_dict(), 200

    @CourierValidator.validate_get_courier_info_service
    async def get_courier_info(self, async_session, request_data):
        courier = request_data['courier']

        courier_data = courier.to_dict(full_info=courier.earning != 0)
        return courier_data, 200
