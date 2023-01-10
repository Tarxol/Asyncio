import asyncio
import asyncpg
import aiohttp #асинхронный аналог requests
import time
from more_itertools import chunked #генератор
import config
from typing import Iterable

BOTTLE_NECK = 10


async def get_persons(session: aiohttp.client.ClientSession, range_person_id: Iterable[int], pool: asyncpg.Pool):
    for person_id_chunk in chunked(range_person_id, BOTTLE_NECK):
        get_person_tasks = [asyncio.create_task(get_person_in_db(session, person_id, pool)) for person_id in person_id_chunk]
        persons = await asyncio.gather(*get_person_tasks)
        for person in persons:
            yield person


async def get_person_in_db(session: aiohttp.client.ClientSession, person_id: int, pool: asyncpg.Pool) -> dict:
    async with session.get(f'https://swapi.dev/api/people/{person_id}') as response:
        response_json = await response.json()
        print(person_id, response_json)
        query = 'INSERT INTO heros (id, name, birth_year, eye_color, films, gender, hair_color, height, homeworld, ' \
                'mass, skin_color, species, starships, vehicles) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,' \
                ' $11, $12, $13, $14)'
        value_films = ','.join(response_json.get('films')) if response_json.get('films') else ''
        value_species = ','.join(response_json.get('species')) if response_json.get('species') else ''
        value_starships = ','.join(response_json.get('starships')) if response_json.get('starships') else ''
        value_vehicles = ','.join(response_json.get('vehicles')) if response_json.get('vehicles') else ''
        values_tuple = (person_id,
                        response_json.get('name', ''),
                        response_json.get('birth_year', ''),
                        response_json.get('eye_color', ''),
                        value_films,    #строка с названиями типов через запятую
                        response_json.get('gender', ''),
                        response_json.get('hair_color', ''),
                        response_json.get('height', ''),
                        response_json.get('homeworld', ''),
                        response_json.get('mass', ''),
                        response_json.get('skin_color', ''),
                        value_species,  #строка с названиями типов через запятую
                        value_starships,#строка с названиями типов через запятую
                        value_vehicles) #строка с названиями типов через запятую
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(query, [values_tuple])
                print(person_id, 'downloaded')
        return response_json



async def main():
    pool = await asyncpg.create_pool(config.PG_DSN, min_size=20, max_size=20)
    async with aiohttp.client.ClientSession() as session:
        async for _ in get_persons(session, range(1, 100), pool):
            pass
    await pool.close()


if __name__ == '__main__':
    start = time.time()
    asyncio.run(main())
    print(f'Время работы {time.time() - start}')