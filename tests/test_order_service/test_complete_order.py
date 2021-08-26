import pytest
from datetime import timedelta, datetime
from sqlalchemy.future import select
from sqlalchemy import update

from tests.functions_for_testing import get_stub, create_couriers, create_orders
from data.models import Order, Region


@pytest.mark.asyncio
async def test_success_complete(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_complete = get_stub('success_complete_order.json')
    td = timedelta(hours=1)
    assign_time = datetime.strptime(data_complete['complete_time'], '%Y-%m-%dT%H:%M:%S.%fZ') - td

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    pg_connection.execute(update(Order.__table__).where(Order.id == data_complete['order_id']).values(
        courier_id=data_complete['courier_id'], is_assign=True, assign_time=assign_time))

    response = await client.post('/orders/complete', json=data_complete)
    body = await response.json()
    order = pg_connection.execute(select(Order).where(Order.id == data_complete['order_id'])).fetchone()
    region = pg_connection.execute(select(Region).where(
        Region.courier_id == data_complete['courier_id'], Region.number_region == order.region_number)).fetchone()

    assert response.status == 200
    assert body == {'order_id': 1}
    assert region.orders_count == 1
    assert region.sum_time == td.total_seconds()
    assert order.is_complete


@pytest.mark.asyncio
async def test_double_success_complete(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_complete = get_stub('success_complete_order.json')
    td = timedelta(hours=1)
    assign_time = datetime.strptime(data_complete['complete_time'], '%Y-%m-%dT%H:%M:%S.%fZ') - td

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    pg_connection.execute(update(Order.__table__).where(Order.id == data_complete['order_id']).values(
        courier_id=data_complete['courier_id'], is_assign=True, assign_time=assign_time))

    response1 = await client.post('/orders/complete', json=data_complete)
    response2 = await client.post('/orders/complete', json=data_complete)
    body1 = await response1.json()
    body2 = await response2.json()
    order = pg_connection.execute(select(Order).where(Order.id == data_complete['order_id'])).fetchone()
    region = pg_connection.execute(select(Region).where(
        Region.courier_id == data_complete['courier_id'], Region.number_region == order.region_number)).fetchone()

    assert response1.status == response2.status == 200
    assert body1 == body2 == {'order_id': 1}
    assert region.orders_count == 1
    assert region.sum_time == td.total_seconds()
    assert order.is_complete


@pytest.mark.asyncio
async def test_not_specified_field(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_complete = get_stub('success_complete_order.json')
    data_complete['kek'] = 'a'
    assign_time = datetime.strptime(data_complete['complete_time'], '%Y-%m-%dT%H:%M:%S.%fZ') - timedelta(hours=1)

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    pg_connection.execute(update(Order.__table__).where(Order.id == data_complete['order_id']).values(
        courier_id=data_complete['courier_id'], is_assign=True, assign_time=assign_time))

    response = await client.post('/orders/complete', json=data_complete)
    order = pg_connection.execute(select(Order).where(Order.id == data_complete['order_id'])).fetchone()
    region = pg_connection.execute(select(Region).where(
        Region.courier_id == data_complete['courier_id'], Region.number_region == order.region_number)).fetchone()

    assert response.status == 400
    assert region.orders_count == 0
    assert region.sum_time == 0
    assert not order.is_complete


@pytest.mark.asyncio
async def test_validation_courier_id(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_complete = get_stub('success_complete_order.json')
    assign_time = datetime.strptime(data_complete['complete_time'], '%Y-%m-%dT%H:%M:%S.%fZ') - timedelta(hours=1)
    data_complete['courier_id'] = 'a'

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    pg_connection.execute(update(Order.__table__).where(Order.id == data_complete['order_id']).values(
        courier_id=1, is_assign=True, assign_time=assign_time))

    response = await client.post('/orders/complete', json=data_complete)
    order = pg_connection.execute(select(Order).where(Order.id == data_complete['order_id'])).fetchone()
    region = pg_connection.execute(select(Region).where(
        Region.courier_id == 1, Region.number_region == order.region_number)).fetchone()

    assert response.status == 400
    assert region.orders_count == 0
    assert region.sum_time == 0
    assert not order.is_complete


@pytest.mark.asyncio
async def test_validation_order_id(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_complete = get_stub('success_complete_order.json')
    data_complete['order_id'] = 'a'
    assign_time = datetime.strptime(data_complete['complete_time'], '%Y-%m-%dT%H:%M:%S.%fZ') - timedelta(hours=1)

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    pg_connection.execute(update(Order.__table__).where(Order.id == 1).values(
        courier_id=data_complete['courier_id'], is_assign=True, assign_time=assign_time))

    response = await client.post('/orders/complete', json=data_complete)
    order = pg_connection.execute(select(Order).where(Order.id == 1)).fetchone()
    region = pg_connection.execute(select(Region).where(
        Region.courier_id == data_complete['courier_id'], Region.number_region == order.region_number)).fetchone()

    assert response.status == 400
    assert region.orders_count == 0
    assert region.sum_time == 0
    assert not order.is_complete


@pytest.mark.asyncio
async def test_validation_complete_time(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_complete = get_stub('success_complete_order.json')
    assign_time = datetime.strptime(data_complete['complete_time'], '%Y-%m-%dT%H:%M:%S.%fZ') - timedelta(hours=1)
    data_complete['complete_time'] = 'a'

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    pg_connection.execute(update(Order.__table__).where(Order.id == data_complete['order_id']).values(
        courier_id=data_complete['courier_id'], is_assign=True, assign_time=assign_time))

    response = await client.post('/orders/complete', json=data_complete)
    order = pg_connection.execute(select(Order).where(Order.id == data_complete['order_id'])).fetchone()
    region = pg_connection.execute(select(Region).where(
        Region.courier_id == data_complete['courier_id'], Region.number_region == order.region_number)).fetchone()

    assert response.status == 400
    assert region.orders_count == 0
    assert region.sum_time == 0
    assert not order.is_complete


@pytest.mark.asyncio
async def test_missing_courier_id(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_complete = get_stub('success_complete_order.json')
    assign_time = datetime.strptime(data_complete['complete_time'], '%Y-%m-%dT%H:%M:%S.%fZ') - timedelta(hours=1)
    del data_complete['courier_id']

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    pg_connection.execute(update(Order.__table__).where(Order.id == data_complete['order_id']).values(
        courier_id=1, is_assign=True, assign_time=assign_time))

    response = await client.post('/orders/complete', json=data_complete)
    order = pg_connection.execute(select(Order).where(Order.id == data_complete['order_id'])).fetchone()
    region = pg_connection.execute(select(Region).where(
        Region.courier_id == 1, Region.number_region == order.region_number)).fetchone()

    assert response.status == 400
    assert region.orders_count == 0
    assert region.sum_time == 0
    assert not order.is_complete


@pytest.mark.asyncio
async def test_missing_order_id(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_complete = get_stub('success_complete_order.json')
    del data_complete['order_id']
    assign_time = datetime.strptime(data_complete['complete_time'], '%Y-%m-%dT%H:%M:%S.%fZ') - timedelta(hours=1)

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    pg_connection.execute(update(Order.__table__).where(Order.id == 1).values(
        courier_id=data_complete['courier_id'], is_assign=True, assign_time=assign_time))

    response = await client.post('/orders/complete', json=data_complete)
    order = pg_connection.execute(select(Order).where(Order.id == 1)).fetchone()
    region = pg_connection.execute(select(Region).where(
        Region.courier_id == data_complete['courier_id'], Region.number_region == order.region_number)).fetchone()

    assert response.status == 400
    assert region.orders_count == 0
    assert region.sum_time == 0
    assert not order.is_complete


@pytest.mark.asyncio
async def test_missing_complete_time(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_complete = get_stub('success_complete_order.json')
    assign_time = datetime.strptime(data_complete['complete_time'], '%Y-%m-%dT%H:%M:%S.%fZ') - timedelta(hours=1)
    del data_complete['complete_time']

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    pg_connection.execute(update(Order.__table__).where(Order.id == data_complete['order_id']).values(
        courier_id=data_complete['courier_id'], is_assign=True, assign_time=assign_time))

    response = await client.post('/orders/complete', json=data_complete)
    order = pg_connection.execute(select(Order).where(Order.id == data_complete['order_id'])).fetchone()
    region = pg_connection.execute(select(Region).where(
        Region.courier_id == data_complete['courier_id'], Region.number_region == order.region_number)).fetchone()

    assert response.status == 400
    assert region.orders_count == 0
    assert region.sum_time == 0
    assert not order.is_complete