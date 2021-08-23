from handlers.simple_handler import SimpleHandler


class OrderHandler(SimpleHandler):
    async def create_orders(self, request):
        json_data, status = await self.service.create_orders(request)
        return self.create_response(json_data, status)

    async def assign_order(self, request):
        json_data, status = await self.service.assign_order(request)
        return self.create_response(json_data, status)

    async def complete_order(self, request):
        json_data, status = await self.service.complete_order(request)
        return self.create_response(json_data, status)

