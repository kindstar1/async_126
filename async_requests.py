import asyncio

import aiohttp

from migrate_db import DbSession, SwapiPeople, close_orm, init_orm

MAX_REQUESTS = 10
TIMEOUT = aiohttp.ClientTimeout(total=120)


async def get_people(session: aiohttp.ClientSession):
    uids_list = []
    url = "https://swapi.tech/api/people?page=1&limit=10"
    while url is not None:
        response = await session.get(url)
        json_data = await response.json()
        for i in json_data.get("results", []):
            uids_list.append(i.get("uid"))
        url = json_data.get("next")
        print(url)

    async def get_person(uid: str):
        response = await session.get(f"https://swapi.tech/api/people/{uid}")
        json_data = await response.json()
        result = json_data.get("result")
        if result:
            return result
        return None

    people_list = await asyncio.gather(*[get_person(uid) for uid in uids_list])
    return people_list


async def insert_peoples(people_list: list[dict]):
    people_list = [p for p in people_list if p is not None]
    async with DbSession() as session:
        for item in people_list:
            person = SwapiPeople(
                id=int(item["uid"]),
                birth_year=item["properties"]["birth_year"],
                eye_color=item["properties"]["eye_color"],
                gender=item["properties"]["gender"],
                hair_color=item["properties"]["hair_color"],
                homeworld=item["properties"]["homeworld"],
                mass=item["properties"]["mass"],
                name=item["properties"]["name"],
                skin_color=item["properties"]["skin_color"],
            )
            session.add(person)
        await session.commit()


async def main():
    # Не переиспользовать соединения — за VPN часто «висят» keep-alive
    connector = aiohttp.TCPConnector(force_close=True)
    async with aiohttp.ClientSession(timeout=TIMEOUT, connector=connector) as http_session:
        await init_orm()
        people_list = await get_people(http_session)
        await insert_peoples(people_list)
        await close_orm()


if __name__ == "__main__":
    asyncio.run(main())
