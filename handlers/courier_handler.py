from handlers.simple_handler import SimpleHandler


class CourierHandler(SimpleHandler):
    async def create_couriers(self, request):
        json_data, status = await self.service.create_couriers(request)
        return self.create_response(json_data, status)

    async def patch_courier(self, request):
        json_data, status = await self.service.patch_courier(request)
        return self.create_response(json_data, status)

    async def get_courier_info(self, request):
        json_data, status = await self.service.get_courier_info(request)
        return self.create_response(json_data, status)

