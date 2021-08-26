import pytest
from sqlalchemy.future import select
from sqlalchemy import update

from tests.functions_for_testing import get_stub, create_couriers, create_orders
from data.models import Order


@pytest.mark.asyncio
async def test_success_assign(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_assign = get_stub('success_assign_order.json')

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)

    response = await client.post('/orders/assign', json=data_assign)
    orders = pg_connection.execute(select(Order)).fetchall()
    body = await response.json()
    assign_time = body['assign_time']

    assert response.status == 200
    assert body['orders'] == [{'id': 3}, {'id': 1}]
    for order in orders:
        if order.id in {1, 3}:
            assert order.is_assign
            assert order.assign_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ') == assign_time
            assert order.courier_id == data_assign['courier_id']
        else:
            assert not order.is_assign
            assert order.assign_time is None
            assert order.courier_id is None


@pytest.mark.asyncio
async def test_double_success_assign(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_assign = get_stub('success_assign_order.json')

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)

    response1 = await client.post('/orders/assign', json=data_assign)
    response2 = await client.post('/orders/assign', json=data_assign)
    orders = pg_connection.execute(select(Order)).fetchall()
    body1 = await response1.json()
    body2 = await response2.json()
    assign_time1, assign_time2 = body1['assign_time'], body2['assign_time']

    assert response1.status == response2.status == 200
    assert body1['orders'] == body2['orders'] == [{'id': 3}, {'id': 1}]
    for order in orders:
        if order.id in {1, 3}:
            assert order.is_assign
            assert order.assign_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ') == assign_time1 == assign_time2
            assert order.courier_id == data_assign['courier_id']
        else:
            assert not order.is_assign
            assert order.assign_time is None
            assert order.courier_id is None


@pytest.mark.asyncio
async def test_assign_completed_order(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_assign = get_stub('success_assign_order.json')

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)
    pg_connection.execute(update(Order.__table__).where(Order.id == 1).values(is_complete=True))

    response = await client.post('/orders/assign', json=data_assign)
    orders = pg_connection.execute(select(Order)).fetchall()
    body = await response.json()
    assign_time = body['assign_time']

    assert response.status == 200
    assert body['orders'] == [{'id': 3}]
    for order in orders:
        if order.id == 3:
            assert order.is_assign
            assert order.assign_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ') == assign_time
            assert order.courier_id == data_assign['courier_id']
        else:
            assert not order.is_assign
            assert order.assign_time is None
            assert order.courier_id is None


@pytest.mark.asyncio
async def test_not_specified_field(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_assign = get_stub('success_assign_order.json')
    data_assign['kek'] = 'a'

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)

    response = await client.post('/orders/assign', json=data_assign)
    orders = pg_connection.execute(select(Order)).fetchall()

    assert response.status == 400
    for order in orders:
        assert not order.is_assign
        assert order.assign_time is None
        assert order.courier_id is None


@pytest.mark.asyncio
async def test_validation_courier_id(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_assign = get_stub('success_assign_order.json')
    data_assign['courier_id'] = 666

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)

    response = await client.post('/orders/assign', json=data_assign)
    orders = pg_connection.execute(select(Order)).fetchall()

    assert response.status == 400
    for order in orders:
        assert not order.is_assign
        assert order.assign_time is None
        assert order.courier_id is None


@pytest.mark.asyncio
async def test_missing_courier_id(client, pg_connection):
    data_couriers = get_stub('success_create_couriers.json')
    data_orders = get_stub('success_create_orders.json')
    data_assign = get_stub('success_assign_order.json')
    del data_assign['courier_id']

    create_couriers(pg_connection, data_couriers)
    create_orders(pg_connection, data_orders)

    response = await client.post('/orders/assign', json=data_assign)
    orders = pg_connection.execute(select(Order)).fetchall()

    assert response.status == 400
    for order in orders:
        assert not order.is_assign
        assert order.assign_time is None
        assert order.courier_id is None
