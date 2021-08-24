from datetime import datetime, timedelta
from sqlalchemy.future import select

from data.db_functions import save_base, query_results
from data.models import Order, OrderInterval
from validators.order_validator import OrderValidator


class OrderService:
    def _check_intervals(self, order_intervals, courier_intervals):
        time_attrs = ['hour', 'minute']
        for courier_interval in courier_intervals:
            end_courier = timedelta(**{f'{key}s': getattr(courier_interval.time_end, key) for key in time_attrs})
            start_courier = timedelta(**{f'{key}s': getattr(courier_interval.time_start, key) for key in time_attrs})
            for order_interval in order_intervals:
                end_order = timedelta(**{f'{key}s': getattr(order_interval.time_end, key) for key in time_attrs})
                start_order = timedelta(**{f'{key}s': getattr(order_interval.time_start, key) for key in time_attrs})
                if start_courier <= end_order and start_order <= end_courier:
                    return True
            return False

    async def _check_order(self, async_session, order, courier_data):
        order_intervals = await query_results(async_session, select(OrderInterval).where(OrderInterval.order == order))
        region_flag = order.region_number in [region.number_region for region in courier_data['regions']]
        weight_flag = courier_data['sum_weight'] + order.weight <= courier_data['carrying']
        interval_flag = self._check_intervals(order_intervals, courier_data['intervals'])
        return region_flag and weight_flag and interval_flag

    @OrderValidator.validate_create_orders_service
    async def create_orders(self, async_session, request_data):
        for order_data in request_data['data']:
            order_id = order_data['order_id']
            weight = order_data['weight']
            region = order_data['region']
            delivery_hours = order_data['delivery_hours']

            order = Order(id=order_id, weight=weight, region_number=region)
            for time_start, time_end in delivery_hours:
                async_session.add(OrderInterval(order_id=order_id, time_start=time_start, time_end=time_end))
            async_session.add(order)

        await save_base(async_session)
        return {'orders': [{'id': order['order_id']} for order in request_data['data']]}, 201

    @OrderValidator.validate_assign_order_service
    async def assign_order(self, async_session, request_data):
        courier = request_data['courier']
        assign_time = datetime.now()

        courier_orders = list(filter(lambda order: order.is_assign and not order.is_complete, courier.orders))
        if courier_orders:
            assign_time = courier_orders[0].assign_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            return {'orders': [{'id': order.id} for order in courier_orders], 'assign_time': assign_time}, 200

        orders = sorted(await query_results(async_session, select(Order).where(Order.is_assign == False)),
                        key=lambda order: order.weight)
        courier_data = {'regions': courier.regions, 'intervals': courier.courier_intervals,
                        'carrying': courier.type.carrying, 'sum_weight': 0}
        orders_assign = []
        for order in orders:
            if await self._check_order(async_session, order, courier_data):
                order.assign_time = assign_time
                order.courier_id = courier.id
                order.is_assign = True
                courier_data['sum_weight'] += order.weight
                orders_assign.append(order)
        await save_base(async_session)
        if orders_assign:
            assign_time = assign_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            return {'orders': [{'id': order.id} for order in orders_assign], 'assign_time': assign_time}, 200
        return {'orders': []}, 200

    @OrderValidator.validate_complete_order_service
    async def complete_order(self, async_session, request_data):
        complete_time = request_data['complete_time']
        courier = request_data['courier']
        order = request_data['order']

        if not order.is_complete:
            if courier.time_last_complete_order is None:
                time_on_delivery = (complete_time - order.assign_time).total_seconds()
            else:
                time_on_delivery = (complete_time - courier.time_last_complete_order).total_seconds()
            order.is_complete = True
            region = {region.number_region: region for region in courier.regions}[order.region_number]
            region.orders_count += 1
            region.sum_time += time_on_delivery
            courier.time_last_complete_order = complete_time
            await save_base(async_session)

            not_complete_orders = list(filter(lambda order: not order.is_complete, courier.orders))
            if not not_complete_orders:
                courier.earning += 500 * courier.type.coefficient
                courier.time_last_complete_order = None
                await save_base(async_session)

        return {'order_id': order.id}, 200
