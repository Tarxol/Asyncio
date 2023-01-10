import asyncio
from sqlalchemy import Integer, String, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

import config


engine = create_async_engine(config.PG_DSN_ALC, echo=True)
Base = declarative_base()


class Heros(Base):

    __tablename__ = 'heros'

    id = Column(Integer, primary_key=True)
    name = Column(String(128), index=True)
    birth_year = Column(String(32), index=True)
    eye_color = Column(String(32), index=True)
    films = Column(String, index=True)
    gender = Column(String(16), index=True)
    hair_color = Column(String(32), index=True)
    height = Column(String(32), index=True)
    homeworld = Column(String(128), index=True)
    mass = Column(String(8), index=True)
    skin_color = Column(String(32), index=True)
    species = Column(String, index=True)
    starships = Column(String, index=True)
    vehicles = Column(String, index=True)

async def get_async_session(
    drop: bool = False, create: bool = False
):

    async with engine.begin() as conn:
        if drop:
            print('Start Drop')
            await conn.run_sync(Base.metadata.drop_all)
        if create:
            print('Start Create')
            await conn.run_sync(Base.metadata.create_all)
    async_session_maker = sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )

    return async_session_maker


async def main():
    await get_async_session(True, True)


if __name__ == '__main__':
    asyncio.run(main())
