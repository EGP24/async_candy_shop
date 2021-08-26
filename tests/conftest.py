import pytest
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv
from uuid import uuid4
from os import environ

from app import create_app
from data.models import Base, CourierType


def create_database(url: str, name: str):
    engine = create_engine(url, isolation_level='AUTOCOMMIT', echo=False)
    with engine.connect() as conn:
        conn.execute(text(f'CREATE DATABASE {name} ENCODING utf8'))


def drop_database(url: str, name: str):
    engine = create_engine(url, isolation_level='AUTOCOMMIT')
    with engine.connect() as conn:
        conn.execute(text(f'DROP DATABASE {name}'))


@pytest.fixture
def loop(event_loop):
    """replace aiohttp loop fixture with pytest-asyncio fixture"""
    return event_loop


@pytest.fixture
def tmp_db_name():
    yield f'db_{uuid4().hex}_test'


@pytest.fixture
def pg_connection(tmp_db_name):
    load_dotenv()
    pg_url = str(URL.create(
        drivername='postgresql',
        host=environ['PG_HOST'],
        port=environ['PG_PORT'],
        username=environ['PG_USERNAME'],
        password=environ['PG_PASSWORD'],
        database=''
    ))

    create_database(str(pg_url), tmp_db_name)
    dsn = pg_url + tmp_db_name
    engine = create_engine(dsn, isolation_level='AUTOCOMMIT')
    Base.metadata.create_all(engine)

    conn = engine.connect()
    conn.execute(insert(CourierType.__table__).values(title='foot', carrying=10, coefficient=2))
    conn.execute(insert(CourierType.__table__).values(title='bike', carrying=15, coefficient=5))
    conn.execute(insert(CourierType.__table__).values(title='car', carrying=50, coefficient=9))
    try:
        yield conn
    finally:
        conn.close()
        engine.dispose()
        drop_database(pg_url, tmp_db_name)


@pytest.fixture
async def client(aiohttp_client, tmp_db_name, pg_connection):
    app = create_app(db_name=tmp_db_name)
    client = await aiohttp_client(app)

    try:
        yield client
    finally:
        await client.close()
