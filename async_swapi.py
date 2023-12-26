import asyncio
import aiohttp
from more_itertools import chunked
from models import init_db, close_db, SwapiPeople, Session

CHUNK_SIZE = 10

async def extract_names(list_href:str|list, type_objects:str) -> str:
    list_names = []
    if isinstance(list_href, str):
        list_href = [list_href]
        for href in list_href:
            async with Session() as session:
                response = await session.get(href)
                response = await response.json()
                list_names.append(response[type_objects])
                return ','.join(list_names)

async def insert_people(people_list):
    people_list = [SwapiPeople(
                               birth_year=person['birth_year'],
                               eye_color=person['eye_color'],
                               films=await extract_names(person['films'], 'name'),
                               gender=person['gender'],
                               hair_color=person['hair_color'],
                               height=person['height'],
                               homeworld=person['homeworld'],
                               mass=person['mass'],
                               name=person['name'],
                               skin_color=person['skin_color'],
                               species=await extract_names(person['species'], 'name'),
                               starships=await extract_names(person['starships'], 'name'),
                               vehicles=await extract_names(person['vehicles'], 'name')) for person in people_list if person.get('birth_year')]
    async with Session() as session:
       session.add_all(people_list)
       await session.commit()



async def get_person(person_id):
    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.dev/api/people/{person_id}/")
    json_response = await response.json()
    await session.close()
    return json_response


async def main():
    await init_db()
    for person_id_chunk in chunked(range(1, 100), CHUNK_SIZE):
        coros = [get_person(person_id) for person_id in person_id_chunk]
        result = await asyncio.gather(*coros)
        asyncio.create_task(insert_people(result))
    tasks = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*tasks)
    await close_db()

asyncio.run(main())