import pytest
from sqlalchemy.future import select

from tests.functions_for_testing import get_stub, interval_to_str
from data.models import Order, OrderInterval


@pytest.mark.asyncio
async def test_success_create_orders(client, pg_connection):
    data = get_stub('success_create_orders.json')
    response = await client.post('/orders', json=data)
    body = await response.json()

    assert response.status == 201
    assert body == {'orders': [{'id': 1}, {'id': 2}, {'id': 3}]}
    for order_data in data['data']:
        order = pg_connection.execute(select(Order).where(Order.id == order_data['order_id'])).fetchone()
        intervals = pg_connection.execute(select(OrderInterval).where(
            OrderInterval.order_id == order_data['order_id'])).fetchall()
        assert order is not None
        assert order.weight == order_data['weight']
        assert order.region_number == order_data['region']
        assert sorted([interval_to_str(interval) for interval in intervals]) == sorted(order_data['delivery_hours'])


@pytest.mark.asyncio
async def test_not_specified_field(client, pg_connection):
    data = get_stub('success_create_orders.json')
    data['data'][0]['kek'] = 'a'

    response = await client.post('/orders', json=data)
    order_intervals = pg_connection.execute(select(OrderInterval)).fetchall()
    orders = pg_connection.execute(select(Order)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'orders': [{'id': 1}]}}
    assert set(map(len, (order_intervals, orders))) == {0}


@pytest.mark.asyncio
async def test_unique_order_id(client, pg_connection):
    data = get_stub('success_create_orders.json')
    del data['data'][1:]

    response = await client.post('/orders', json=data)
    response = await client.post('/orders', json=data)
    orders = pg_connection.execute(select(Order)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'orders': [{'id': 1}]}}
    assert len(orders) == 1


@pytest.mark.asyncio
async def test_validation_order_id(client, pg_connection):
    data = get_stub('success_create_orders.json')
    del data['data'][1:]
    data['data'][0]['order_id'] = 'a'

    response = await client.post('/orders', json=data)
    order_intervals = pg_connection.execute(select(OrderInterval)).fetchall()
    orders = pg_connection.execute(select(Order)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'orders': [{'id': 'a'}]}}
    assert set(map(len, (order_intervals, orders))) == {0}


@pytest.mark.asyncio
async def test_validation_weight(client, pg_connection):
    data = get_stub('success_create_orders.json')
    data['data'][0]['weight'] = 'a'
    data['data'][1]['weight'] = 50.1
    data['data'][2]['weight'] = 0.009

    response = await client.post('/orders', json=data)
    order_intervals = pg_connection.execute(select(OrderInterval)).fetchall()
    orders = pg_connection.execute(select(Order)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'orders': [{'id': 1}, {'id': 2}, {'id': 3}]}}
    assert set(map(len, (order_intervals, orders))) == {0}


@pytest.mark.asyncio
async def test_validation_region(client, pg_connection):
    data = get_stub('success_create_orders.json')
    del data['data'][1:]
    data['data'][0]['region'] = [1, 2, 3, 'a']

    response = await client.post('/orders', json=data)
    order_intervals = pg_connection.execute(select(OrderInterval)).fetchall()
    orders = pg_connection.execute(select(Order)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'orders': [{'id': 1}]}}
    assert set(map(len, (order_intervals, orders))) == {0}


@pytest.mark.asyncio
async def test_validation_delivery_hours(client, pg_connection):
    data = get_stub('success_create_orders.json')
    del data['data'][1:]
    data['data'][0]['delivery_hours'] = ['09:00-18:00', 'a']

    response = await client.post('/orders', json=data)
    order_intervals = pg_connection.execute(select(OrderInterval)).fetchall()
    orders = pg_connection.execute(select(Order)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'orders': [{'id': 1}]}}
    assert set(map(len, (order_intervals, orders))) == {0}


@pytest.mark.asyncio
async def test_missing_weight(client, pg_connection):
    data = get_stub('success_create_orders.json')
    del data['data'][1:]
    del data['data'][0]['weight']

    response = await client.post('/orders', json=data)
    order_intervals = pg_connection.execute(select(OrderInterval)).fetchall()
    orders = pg_connection.execute(select(Order)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'orders': [{'id': 1}]}}
    assert set(map(len, (order_intervals, orders))) == {0}


@pytest.mark.asyncio
async def test_missing_region(client, pg_connection):
    data = get_stub('success_create_orders.json')
    del data['data'][1:]
    del data['data'][0]['region']

    response = await client.post('/orders', json=data)
    order_intervals = pg_connection.execute(select(OrderInterval)).fetchall()
    orders = pg_connection.execute(select(Order)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'orders': [{'id': 1}]}}
    assert set(map(len, (order_intervals, orders))) == {0}


@pytest.mark.asyncio
async def test_missing_delivery_hours(client, pg_connection):
    data = get_stub('success_create_orders.json')
    del data['data'][1:]
    del data['data'][0]['delivery_hours']

    response = await client.post('/orders', json=data)
    order_intervals = pg_connection.execute(select(OrderInterval)).fetchall()
    orders = pg_connection.execute(select(Order)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'orders': [{'id': 1}]}}
    assert set(map(len, (order_intervals, orders))) == {0}
