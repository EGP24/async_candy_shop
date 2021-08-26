import pytest
from datetime import timedelta, datetime
from sqlalchemy.future import select
from sqlalchemy import update

from tests.functions_for_testing import get_stub, create_couriers, create_orders, interval_to_str
from data.models import Courier, Order, CourierInterval, Region, CourierType


@pytest.mark.asyncio
async def test_success_patch_courier(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_complete = get_stub('success_complete_order.json')
    data_patch = get_stub('success_patch_courier.json')
    td = timedelta(hours=1)
    assign_time = datetime.fromisoformat(data_complete['complete_time'][:-1]) - td

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    order = pg_connection.execute(update(Order.__table__).where(Order.id == data_complete['order_id']).values(
        courier_id=data_complete['courier_id'], is_assign=True, is_complete=True, assign_time=assign_time).returning(
        Order.region_number)).fetchone()
    pg_connection.execute(update(Region.__table__).where(
        Region.courier_id == data_complete['courier_id'], Region.number_region == order.region_number).values(
        orders_count=1, sum_time=td.total_seconds()))

    response = await client.patch('/couriers/1', json=data_patch)
    courier = pg_connection.execute(select(Courier).where(Courier.id == 1)).fetchone()
    courier_type = pg_connection.execute(select(CourierType).where(CourierType.id == courier.type_id)).fetchone().title
    intervals = pg_connection.execute(select(CourierInterval).where(
        CourierInterval.courier_id == courier.id)).fetchall()
    regions = pg_connection.execute(select(Region).where(Region.courier_id == courier.id)).fetchall()
    region = pg_connection.execute(select(Region).where(
        Region.courier_id == courier.id, Region.number_region == order.region_number)).fetchone()
    courier_data_from_db = {'courier_id': courier.id, 'courier_type': courier_type,
                            'regions': [region.number_region for region in regions],
                            'working_hours': [interval_to_str(interval) for interval in intervals]}
    courier_data = {'courier_id': 1, 'courier_type': 'car', 'regions': [22, 12, 5, 6, 7],
                    'working_hours': ["11:35-14:05", "09:00-11:00", "07:00-23:00"]}

    body = await response.json()
    assert response.status == 200
    assert body == courier_data_from_db == courier_data
    assert region.orders_count == 1
    assert region.sum_time == td.total_seconds()


@pytest.mark.asyncio
async def test_not_specified_field(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_complete = get_stub('success_complete_order.json')
    data_patch = get_stub('success_patch_courier.json')
    data_patch['kek'] = 'a'
    td = timedelta(hours=1)
    assign_time = datetime.fromisoformat(data_complete['complete_time'][:-1]) - td

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    order = pg_connection.execute(update(Order.__table__).where(Order.id == data_complete['order_id']).values(
        courier_id=data_complete['courier_id'], is_assign=True, is_complete=True, assign_time=assign_time).returning(
        Order.region_number)).fetchone()
    pg_connection.execute(update(Region.__table__).where(
        Region.courier_id == data_complete['courier_id'], Region.number_region == order.region_number).values(
        orders_count=1, sum_time=td.total_seconds()))

    response = await client.patch('/couriers/1', json=data_patch)
    courier = pg_connection.execute(select(Courier).where(Courier.id == 1)).fetchone()
    courier_type = pg_connection.execute(select(CourierType).where(CourierType.id == courier.type_id)).fetchone().title
    intervals = pg_connection.execute(select(CourierInterval).where(
        CourierInterval.courier_id == courier.id)).fetchall()
    regions = pg_connection.execute(select(Region).where(Region.courier_id == courier.id)).fetchall()
    region = pg_connection.execute(select(Region).where(
        Region.courier_id == courier.id, Region.number_region == order.region_number)).fetchone()
    courier_data_from_db = {'courier_id': courier.id, 'courier_type': courier_type,
                            'regions': sorted(region.number_region for region in regions),
                            'working_hours': [interval_to_str(interval) for interval in intervals]}

    assert response.status == 400
    assert courier_data_from_db == data_couriers['data'][0]
    assert region.orders_count == 1
    assert region.sum_time == td.total_seconds()


@pytest.mark.asyncio
async def test_validation_courier_id(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_complete = get_stub('success_complete_order.json')
    data_patch = get_stub('success_patch_courier.json')
    td = timedelta(hours=1)
    assign_time = datetime.fromisoformat(data_complete['complete_time'][:-1]) - td

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    order = pg_connection.execute(update(Order.__table__).where(Order.id == data_complete['order_id']).values(
        courier_id=data_complete['courier_id'], is_assign=True, is_complete=True, assign_time=assign_time).returning(
        Order.region_number)).fetchone()
    pg_connection.execute(update(Region.__table__).where(
        Region.courier_id == data_complete['courier_id'], Region.number_region == order.region_number).values(
        orders_count=1, sum_time=td.total_seconds()))

    response = await client.patch('/couriers/kek', json=data_patch)
    courier = pg_connection.execute(select(Courier).where(Courier.id == 1)).fetchone()
    courier_type = pg_connection.execute(select(CourierType).where(CourierType.id == courier.type_id)).fetchone().title
    intervals = pg_connection.execute(select(CourierInterval).where(
        CourierInterval.courier_id == courier.id)).fetchall()
    regions = pg_connection.execute(select(Region).where(Region.courier_id == courier.id)).fetchall()
    region = pg_connection.execute(select(Region).where(
        Region.courier_id == courier.id, Region.number_region == order.region_number)).fetchone()
    courier_data_from_db = {'courier_id': courier.id, 'courier_type': courier_type,
                            'regions': sorted(region.number_region for region in regions),
                            'working_hours': [interval_to_str(interval) for interval in intervals]}

    assert response.status == 400
    assert courier_data_from_db == data_couriers['data'][0]
    assert region.orders_count == 1
    assert region.sum_time == td.total_seconds()


@pytest.mark.asyncio
async def test_validation_courier_type(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_complete = get_stub('success_complete_order.json')
    data_patch = get_stub('success_patch_courier.json')
    data_patch['courier_type'] = 'a'
    td = timedelta(hours=1)
    assign_time = datetime.fromisoformat(data_complete['complete_time'][:-1]) - td

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    order = pg_connection.execute(update(Order.__table__).where(Order.id == data_complete['order_id']).values(
        courier_id=data_complete['courier_id'], is_assign=True, is_complete=True, assign_time=assign_time).returning(
        Order.region_number)).fetchone()
    pg_connection.execute(update(Region.__table__).where(
        Region.courier_id == data_complete['courier_id'], Region.number_region == order.region_number).values(
        orders_count=1, sum_time=td.total_seconds()))

    response = await client.patch('/couriers/1', json=data_patch)
    courier = pg_connection.execute(select(Courier).where(Courier.id == 1)).fetchone()
    courier_type = pg_connection.execute(select(CourierType).where(CourierType.id == courier.type_id)).fetchone().title
    intervals = pg_connection.execute(select(CourierInterval).where(
        CourierInterval.courier_id == courier.id)).fetchall()
    regions = pg_connection.execute(select(Region).where(Region.courier_id == courier.id)).fetchall()
    region = pg_connection.execute(select(Region).where(
        Region.courier_id == courier.id, Region.number_region == order.region_number)).fetchone()
    courier_data_from_db = {'courier_id': courier.id, 'courier_type': courier_type,
                            'regions': sorted(region.number_region for region in regions),
                            'working_hours': [interval_to_str(interval) for interval in intervals]}

    assert response.status == 400
    assert courier_data_from_db == data_couriers['data'][0]
    assert region.orders_count == 1
    assert region.sum_time == td.total_seconds()


@pytest.mark.asyncio
async def test_validation_regions(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_complete = get_stub('success_complete_order.json')
    data_patch = get_stub('success_patch_courier.json')
    data_patch['regions'] = ['a', 1, 2, 3]
    td = timedelta(hours=1)
    assign_time = datetime.fromisoformat(data_complete['complete_time'][:-1]) - td

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    order = pg_connection.execute(update(Order.__table__).where(Order.id == data_complete['order_id']).values(
        courier_id=data_complete['courier_id'], is_assign=True, is_complete=True, assign_time=assign_time).returning(
        Order.region_number)).fetchone()
    pg_connection.execute(update(Region.__table__).where(
        Region.courier_id == data_complete['courier_id'], Region.number_region == order.region_number).values(
        orders_count=1, sum_time=td.total_seconds()))

    response = await client.patch('/couriers/1', json=data_patch)
    courier = pg_connection.execute(select(Courier).where(Courier.id == 1)).fetchone()
    courier_type = pg_connection.execute(select(CourierType).where(CourierType.id == courier.type_id)).fetchone().title
    intervals = pg_connection.execute(select(CourierInterval).where(
        CourierInterval.courier_id == courier.id)).fetchall()
    regions = pg_connection.execute(select(Region).where(Region.courier_id == courier.id)).fetchall()
    region = pg_connection.execute(select(Region).where(
        Region.courier_id == courier.id, Region.number_region == order.region_number)).fetchone()
    courier_data_from_db = {'courier_id': courier.id, 'courier_type': courier_type,
                            'regions': sorted(region.number_region for region in regions),
                            'working_hours': [interval_to_str(interval) for interval in intervals]}

    assert response.status == 400
    assert courier_data_from_db == data_couriers['data'][0]
    assert region.orders_count == 1
    assert region.sum_time == td.total_seconds()


@pytest.mark.asyncio
async def test_validation_working_hours(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_complete = get_stub('success_complete_order.json')
    data_patch = get_stub('success_patch_courier.json')
    data_patch['regions'] = ["11:35-14:05", 'a', "07:00-23:00"]
    td = timedelta(hours=1)
    assign_time = datetime.fromisoformat(data_complete['complete_time'][:-1]) - td

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    order = pg_connection.execute(update(Order.__table__).where(Order.id == data_complete['order_id']).values(
        courier_id=data_complete['courier_id'], is_assign=True, is_complete=True, assign_time=assign_time).returning(
        Order.region_number)).fetchone()
    pg_connection.execute(update(Region.__table__).where(
        Region.courier_id == data_complete['courier_id'], Region.number_region == order.region_number).values(
        orders_count=1, sum_time=td.total_seconds()))

    response = await client.patch('/couriers/1', json=data_patch)
    courier = pg_connection.execute(select(Courier).where(Courier.id == 1)).fetchone()
    courier_type = pg_connection.execute(select(CourierType).where(CourierType.id == courier.type_id)).fetchone().title
    intervals = pg_connection.execute(select(CourierInterval).where(
        CourierInterval.courier_id == courier.id)).fetchall()
    regions = pg_connection.execute(select(Region).where(Region.courier_id == courier.id)).fetchall()
    region = pg_connection.execute(select(Region).where(
        Region.courier_id == courier.id, Region.number_region == order.region_number)).fetchone()
    courier_data_from_db = {'courier_id': courier.id, 'courier_type': courier_type,
                            'regions': sorted(region.number_region for region in regions),
                            'working_hours': [interval_to_str(interval) for interval in intervals]}

    assert response.status == 400
    assert courier_data_from_db == data_couriers['data'][0]
    assert region.orders_count == 1
    assert region.sum_time == td.total_seconds()