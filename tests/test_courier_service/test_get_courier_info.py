import pytest
from datetime import timedelta, datetime
from sqlalchemy.future import select
from sqlalchemy import update

from tests.functions_for_testing import get_stub, create_couriers, create_orders, interval_to_str, get_courier_rating
from data.models import Courier, Order, CourierInterval, Region, CourierType


@pytest.mark.asyncio
async def test_success_get_courier_info(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_complete = get_stub('success_complete_order.json')
    td = timedelta(minutes=15)
    assign_time = datetime.strptime(data_complete['complete_time'], '%Y-%m-%dT%H:%M:%S.%fZ') - td

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    order = pg_connection.execute(update(Order.__table__).where(Order.id == data_complete['order_id']).values(
        courier_id=data_complete['courier_id'], is_assign=True, is_complete=True, assign_time=assign_time).returning(
        Order.region_number)).fetchone()
    pg_connection.execute(update(Region.__table__).where(
        Region.courier_id == data_complete['courier_id'], Region.number_region == order.region_number).values(
        orders_count=1, sum_time=td.total_seconds()))
    pg_connection.execute(update(Courier).where(Courier.id == data_complete['courier_id']).values(earning=1000))

    response = await client.get('/couriers/1')
    courier = pg_connection.execute(select(Courier).where(Courier.id == 1)).fetchone()
    courier_type = pg_connection.execute(select(CourierType).where(CourierType.id == courier.type_id)).fetchone().title
    intervals = pg_connection.execute(select(CourierInterval).where(
        CourierInterval.courier_id == courier.id)).fetchall()
    regions = pg_connection.execute(select(Region).where(Region.courier_id == courier.id)).fetchall()
    courier_data_from_db = {'courier_id': courier.id, 'courier_type': courier_type,
                            'regions': [region.number_region for region in regions],
                            'working_hours': [interval_to_str(interval) for interval in intervals],
                            'earnings': courier.earning, 'rating': get_courier_rating(regions)}
    courier_data = {'courier_id': 1, 'courier_type': 'foot', 'regions': [1, 22, 12],
                    'working_hours': ["11:35-14:05", "09:00-11:00"],
                    'earnings': 1000, 'rating': 3.75}

    body = await response.json()
    assert response.status == 200
    assert body == courier_data_from_db == courier_data


@pytest.mark.asyncio
async def test_validation_courier_id(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_complete = get_stub('success_complete_order.json')
    td = timedelta(minutes=15)
    assign_time = datetime.strptime(data_complete['complete_time'], '%Y-%m-%dT%H:%M:%S.%fZ') - td

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    order = pg_connection.execute(update(Order.__table__).where(Order.id == data_complete['order_id']).values(
        courier_id=data_complete['courier_id'], is_assign=True, is_complete=True, assign_time=assign_time).returning(
        Order.region_number)).fetchone()
    pg_connection.execute(update(Region.__table__).where(
        Region.courier_id == data_complete['courier_id'], Region.number_region == order.region_number).values(
        orders_count=1, sum_time=td.total_seconds()))
    pg_connection.execute(update(Courier).where(Courier.id == data_complete['courier_id']).values(earning=1000))

    response = await client.get('/couriers/kek')
    assert response.status == 400
