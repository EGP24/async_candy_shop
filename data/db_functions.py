from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession
from sqlalchemy.orm import sessionmaker, selectinload
from sqlalchemy.future import select
from sqlalchemy.engine import URL
from aiohttp.web_app import Application
from os import environ

from data.models import Base, Courier, Order


async def db_engine_initializer(app: Application):
    url = URL.create(
        drivername='postgresql+asyncpg',
        host=environ.get('PG_HOST'),
        port=environ.get('PG_PORT'),
        database=environ.get('PG_DATABASE'),
        username=environ.get('PG_USERNAME'),
        password=environ.get('PG_PASSWORD')
    )
    app['db_engine']: AsyncEngine = create_async_engine(url, echo=False)
    yield
    await app['db_engine'].dispose()


async def db_session_initializer(app: Application):
    engine: AsyncEngine = app['db_engine']
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    app['async_session']: AsyncSession = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)()

    yield
    await app['async_session'].close()


async def save_base(session):
    try:
        await session.commit()
    except:
        await session.rollback()


async def query_results(session, query):
    result = await session.execute(query)
    return result.scalars().all()


async def query_result(session, query):
    result = await session.execute(query)
    return result.scalars().first()


async def get_courier_by_id(session, courier_id):
    courier = await query_result(session, select(Courier).where(Courier.id == courier_id).options(
        *map(selectinload, (
            Courier.regions,
            Courier.courier_intervals,
            Courier.orders,
            Courier.type))
    ))
    return courier


async def get_order_by_id(session, order_id, courier_id=None):
    if courier_id is None:
        query = select(Order).where(Order.id == order_id).options(selectinload(Order.delivery_hours))
    else:
        query = select(Order).where(Order.id == order_id, Order.courier_id == courier_id).options(
            selectinload(Order.delivery_hours)
        )
    order = await query_result(session, query)
    return order