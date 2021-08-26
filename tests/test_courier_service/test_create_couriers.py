import pytest
from sqlalchemy.future import select

from data.models import Courier, CourierType, CourierInterval, Region
from tests.functions_for_testing import get_stub, interval_to_str


@pytest.mark.asyncio
async def test_success_create_couriers(client, pg_connection):
    data = get_stub('success_create_couriers.json')
    response = await client.post('/couriers', json=data)
    body = await response.json()

    assert response.status == 201
    assert body == {'couriers': [{'id': 1}, {'id': 2}, {'id': 3}, {'id': 4}]}
    for courier_data in data['data']:
        courier = pg_connection.execute(select(Courier).where(Courier.id == courier_data['courier_id'])).fetchone()
        courier_type = pg_connection.execute(select(CourierType).where(
            CourierType.title == courier_data['courier_type'])).fetchone()
        regions = pg_connection.execute(select(Region).where(
            Region.courier_id == courier_data['courier_id'])).fetchall()
        intervals = pg_connection.execute(select(CourierInterval).where(
            CourierInterval.courier_id == courier_data['courier_id'])).fetchall()
        assert courier is not None
        assert courier.type_id == courier_type.id
        assert sorted([region.number_region for region in regions]) == sorted(courier_data['regions'])
        assert sorted([interval_to_str(interval) for interval in intervals]) == sorted(courier_data['working_hours'])


@pytest.mark.asyncio
async def test_not_specified_field(client, pg_connection):
    data = get_stub('success_create_couriers.json')
    data['data'][0]['kek'] = 'a'

    response = await client.post('/couriers', json=data)
    courier_intervals = pg_connection.execute(select(CourierInterval)).fetchall()
    couriers = pg_connection.execute(select(Courier)).fetchall()
    regions = pg_connection.execute(select(Region)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'couriers': [{'id': 1}]}}
    assert set(map(len, (courier_intervals, couriers, regions))) == {0}


@pytest.mark.asyncio
async def test_unique_courier_id(client, pg_connection):
    data = get_stub('success_create_couriers.json')
    del data['data'][1:]

    response = await client.post('/couriers', json=data)
    response = await client.post('/couriers', json=data)
    couriers = pg_connection.execute(select(Courier)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'couriers': [{'id': 1}]}}
    assert len(couriers) == 1


@pytest.mark.asyncio
async def test_validation_courier_id(client, pg_connection):
    data = get_stub('success_create_couriers.json')
    del data['data'][1:]
    data['data'][0]['courier_id'] = 'a'

    response = await client.post('/couriers', json=data)
    courier_intervals = pg_connection.execute(select(CourierInterval)).fetchall()
    couriers = pg_connection.execute(select(Courier)).fetchall()
    regions = pg_connection.execute(select(Region)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'couriers': [{'id': 'a'}]}}
    assert set(map(len, (courier_intervals, couriers, regions))) == {0}


@pytest.mark.asyncio
async def test_validation_courier_type(client, pg_connection):
    data = get_stub('success_create_couriers.json')
    del data['data'][1:]
    data['data'][0]['courier_type'] = 'a'

    response = await client.post('/couriers', json=data)
    courier_intervals = pg_connection.execute(select(CourierInterval)).fetchall()
    couriers = pg_connection.execute(select(Courier)).fetchall()
    regions = pg_connection.execute(select(Region)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'couriers': [{'id': 1}]}}
    assert set(map(len, (courier_intervals, couriers, regions))) == {0}


@pytest.mark.asyncio
async def test_validation_regions(client, pg_connection):
    data = get_stub('success_create_couriers.json')
    del data['data'][1:]
    data['data'][0]['regions'] = [1, 2, 3, 'a']

    response = await client.post('/couriers', json=data)
    courier_intervals = pg_connection.execute(select(CourierInterval)).fetchall()
    couriers = pg_connection.execute(select(Courier)).fetchall()
    regions = pg_connection.execute(select(Region)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'couriers': [{'id': 1}]}}
    assert set(map(len, (courier_intervals, couriers, regions))) == {0}


@pytest.mark.asyncio
async def test_validation_working_hours(client, pg_connection):
    data = get_stub('success_create_couriers.json')
    del data['data'][1:]
    data['data'][0]['working_hours'] = ['09:00-18:00', 'a']

    response = await client.post('/couriers', json=data)
    courier_intervals = pg_connection.execute(select(CourierInterval)).fetchall()
    couriers = pg_connection.execute(select(Courier)).fetchall()
    regions = pg_connection.execute(select(Region)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'couriers': [{'id': 1}]}}
    assert set(map(len, (courier_intervals, couriers, regions))) == {0}


@pytest.mark.asyncio
async def test_missing_courier_type(client, pg_connection):
    data = get_stub('success_create_couriers.json')
    del data['data'][1:]
    del data['data'][0]['courier_type']

    response = await client.post('/couriers', json=data)
    courier_intervals = pg_connection.execute(select(CourierInterval)).fetchall()
    couriers = pg_connection.execute(select(Courier)).fetchall()
    regions = pg_connection.execute(select(Region)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'couriers': [{'id': 1}]}}
    assert set(map(len, (courier_intervals, couriers, regions))) == {0}


@pytest.mark.asyncio
async def test_missing_regions(client, pg_connection):
    data = get_stub('success_create_couriers.json')
    del data['data'][1:]
    del data['data'][0]['regions']

    response = await client.post('/couriers', json=data)
    courier_intervals = pg_connection.execute(select(CourierInterval)).fetchall()
    couriers = pg_connection.execute(select(Courier)).fetchall()
    regions = pg_connection.execute(select(Region)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'couriers': [{'id': 1}]}}
    assert set(map(len, (courier_intervals, couriers, regions))) == {0}


@pytest.mark.asyncio
async def test_missing_working_hours(client, pg_connection):
    data = get_stub('success_create_couriers.json')
    del data['data'][1:]
    del data['data'][0]['working_hours']

    response = await client.post('/couriers', json=data)
    courier_intervals = pg_connection.execute(select(CourierInterval)).fetchall()
    couriers = pg_connection.execute(select(Courier)).fetchall()
    regions = pg_connection.execute(select(Region)).fetchall()
    body = await response.json()

    assert response.status == 400
    assert body == {'validation_error': {'couriers': [{'id': 1}]}}
    assert set(map(len, (courier_intervals, couriers, regions))) == {0}
