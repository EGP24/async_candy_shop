from aiohttp import web


class SimpleHandler:
    def __init__(self, service):
        self.service = service

    def create_response(self, json_data, status):
        if json_data is not None:
            return web.json_response(data=json_data, status=status)
        return web.Response(status=status)
