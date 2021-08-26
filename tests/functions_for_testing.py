from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.future import select
from json import loads
from os import path

from data.models import Courier, Order, CourierInterval, OrderInterval, Region, CourierType
from validators.validate_schemes import CreateCouriersSchema, CreateOrdersSchema


def get_stub(name):
    with open(path.join(f'./tests/stubs/', name), encoding="utf8") as f:
        stub = f.read()
    return loads(stub)


def interval_to_str(interval):
    return f'{str(interval.time_start)[:-3]}-{str(interval.time_end)[:-3]}'


def create_regions(conn, regions, courier_id):
    for region in regions:
        conn.execute(insert(Region.__table__).values(number_region=region, courier_id=courier_id))


def create_intervals(conn, interval_type, intervals, foreign_key):
    for interval in intervals:
        values = {'time_start': interval[0], 'time_end': interval[1]}
        if interval_type == 'c':
            interval_class = CourierInterval
            values['courier_id'] = foreign_key
        if interval_type == 'o':
            interval_class = OrderInterval
            values['order_id'] = foreign_key
        conn.execute(insert(interval_class.__table__).values(**values))


def create_couriers(conn, data_courier):
    for courier in data_courier['data']:
        result = CreateCouriersSchema().load(courier)
        courier_id = result['courier_id']
        courier_type = conn.execute(select(CourierType).where(CourierType.title == result['courier_type'])).fetchone()
        conn.execute(insert(Courier.__table__).values(id=courier_id, type_id=courier_type.id))
        create_regions(conn, result['regions'], courier_id)
        create_intervals(conn, 'c', result['working_hours'], courier_id)


def create_orders(conn, data_order):
    for order in data_order['data']:
        result = CreateOrdersSchema().load(order)
        order_id = result['order_id']
        weight = result['weight']
        region = result['region']
        conn.execute(insert(Order.__table__).values(id=order_id, weight=weight, region_number=region))
        create_intervals(conn, 'o', result['delivery_hours'], order_id)


def get_courier_rating(courier_regions):
    regions = list(filter(lambda region: region.orders_count != 0, courier_regions))
    min_delivery_time = min([60 * 60] + [region.sum_time / region.orders_count for region in regions])
    rating = (60 * 60 - min_delivery_time) / 60 / 60 * 5
    return round(rating, 2)
