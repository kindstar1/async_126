import asyncio

import aiohttp

from migrate_db import DbSession, SwapiPeople, close_orm, init_orm

BASE_URL = "https://swapi.tech/api"
PAGE_LIMIT = 10

MAX_CONCURRENT_HTTP = 10
MAX_RETRIES = 5

TIMEOUT = aiohttp.ClientTimeout(total=180)


async def fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
    max_retries: int = MAX_RETRIES,
) -> dict | None:
    for attempt in range(1, max_retries + 1):
        try:
            async with semaphore:
                response = await session.get(url)
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientResponseError as exc:

            if exc.status == 429:
                retry_after_raw = exc.headers.get("Retry-After")
                retry_after: float | None = None
                if retry_after_raw is not None:
                    try:
                        retry_after = float(retry_after_raw)
                    except ValueError:
                        retry_after = None

                if attempt == max_retries:
                    print(f"[FAIL] {url} (429, attempt {attempt}/{max_retries})")
                    return None

                delay = retry_after if retry_after is not None else (5.0 * attempt)
                await asyncio.sleep(delay)
                continue

            if attempt == max_retries:
                print(f"[FAIL] {url} (HTTP {exc.status}, attempt {attempt}/{max_retries}): {exc.message}")
                return None
            await asyncio.sleep(0.5 * attempt)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
    
            if attempt == max_retries:
                print(f"[FAIL] {url} (attempt {attempt}/{max_retries}): {exc}")
                return None
            await asyncio.sleep(0.5 * attempt)

    return None


async def get_json_cached(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
    json_cache: dict[str, dict | None],
    json_inflight: dict[str, asyncio.Future[dict | None]],
) -> dict | None:
    if url in json_cache:
        return json_cache[url]

    existing = json_inflight.get(url)
    if existing is not None:
        return await existing

    loop = asyncio.get_running_loop()
    fut: asyncio.Future[dict | None] = loop.create_future()
    json_inflight[url] = fut

    try:
        data = await fetch_json(session, url, semaphore)
        json_cache[url] = data
        fut.set_result(data)
        return data
    except Exception as exc:
        fut.set_result(None)
        print(f"[FAIL] json for {url}: {exc}")
        return None
    finally:
        json_inflight.pop(url, None)


def _extract_title_from_result(data: dict | None) -> str | None:

    if not data:
        return None
    props = data.get("result", {}).get("properties", {}) or {}
    return props.get("title") or props.get("name")


async def fetch_related_titles(
    session: aiohttp.ClientSession,
    urls: list[str],
    semaphore: asyncio.Semaphore,
    title_cache: dict[str, str],
    inflight: dict[str, asyncio.Future[str]],
    json_cache: dict[str, dict | None],
    json_inflight: dict[str, asyncio.Future[dict | None]],
) -> str:

    seen: set[str] = set()
    unique_urls: list[str] = []
    for u in urls:
        if u and u not in seen:
            seen.add(u)
            unique_urls.append(u)

    titles = await asyncio.gather(
        *(
            get_title_for_url(
                session,
                u,
                semaphore,
                title_cache,
                inflight,
                json_cache,
                json_inflight,
            )
            for u in unique_urls
        )
    )
    normalized = [t for t in titles if t]
    return ", ".join(normalized)


async def get_title_for_url(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
    title_cache: dict[str, str],
    inflight: dict[str, asyncio.Future[str]],
    json_cache: dict[str, dict | None],
    json_inflight: dict[str, asyncio.Future[dict | None]],
) -> str:

    if url in title_cache:
        return title_cache[url]


    existing = inflight.get(url)
    if existing is not None:
        return await existing

    loop = asyncio.get_running_loop()
    fut: asyncio.Future[str] = loop.create_future()
    inflight[url] = fut

    try:
        data = await get_json_cached(session, url, semaphore, json_cache, json_inflight)
        title = _extract_title_from_result(data) or ""
        title_cache[url] = title
        fut.set_result(title)
        return title
    except Exception as exc:
        # Чтобы остальные ожидали корректный результат.
        fut.set_result("")
        print(f"[FAIL] title for {url}: {exc}")
        return ""
    finally:
        inflight.pop(url, None)


async def get_people(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    title_cache: dict[str, str],
    inflight: dict[str, asyncio.Future[str]],
    json_cache: dict[str, dict | None],
    json_inflight: dict[str, asyncio.Future[dict | None]],
):

    uids_list: list[str] = []

    url = f"{BASE_URL}/people?page=1&limit={PAGE_LIMIT}"
    while url is not None:
        page_data = await fetch_json(session, url, semaphore)
        if page_data is None:
            return []

        for i in page_data.get("results", []):
            uid = i.get("uid")
            if uid is not None:
                uids_list.append(str(uid))

        url = page_data.get("next")

    async def get_person_row(uid: str) -> dict | None:
        data = await fetch_json(session, f"{BASE_URL}/people/{uid}", semaphore)
        if not data:
            return None

        props = data.get("result", {}).get("properties", {}) or {}

        homeworld_url = props.get("homeworld")
        async def fetch_homeworld_title():
            if not homeworld_url:
                return ""
            return await get_title_for_url(
                session,
                homeworld_url,
                semaphore,
                title_cache,
                inflight,
                json_cache,
                json_inflight,
            )

        film_urls = props.get("films", []) or []

        async def fetch_species_titles_from_films():

            seen_species: set[str] = set()
            species_urls: list[str] = []


            seen_films: set[str] = set()
            unique_film_urls: list[str] = []
            for fu in film_urls:
                if fu and fu not in seen_films:
                    seen_films.add(fu)
                    unique_film_urls.append(fu)

            film_datas = await asyncio.gather(
                *(
                    get_json_cached(session, fu, semaphore, json_cache, json_inflight)
                    for fu in unique_film_urls
                )
            )

            for fd in film_datas:
                props_film = fd.get("result", {}).get("properties", {}) if fd else {}
                for su in props_film.get("species", []) or []:
                    if su and su not in seen_species:
                        seen_species.add(su)
                        species_urls.append(su)

            if not species_urls:
                return ""

            titles = await asyncio.gather(
                *(
                    get_title_for_url(
                        session,
                        su,
                        semaphore,
                        title_cache,
                        inflight,
                        json_cache,
                        json_inflight,
                    )
                    for su in species_urls
                )
            )
            titles = [t for t in titles if t]
            return ", ".join(titles)


        films_task = fetch_related_titles(
            session,
            props.get("films", []) or [],
            semaphore,
            title_cache,
            inflight,
            json_cache,
            json_inflight,
        )
        vehicles_task = fetch_related_titles(
            session,
            props.get("vehicles", []) or [],
            semaphore,
            title_cache,
            inflight,
            json_cache,
            json_inflight,
        )
        starships_task = fetch_related_titles(
            session,
            props.get("starships", []) or [],
            semaphore,
            title_cache,
            inflight,
            json_cache,
            json_inflight,
        )

        homeworld_title, films, vehicles, starships, species = await asyncio.gather(
            fetch_homeworld_title(),
            films_task,
            vehicles_task,
            starships_task,
            fetch_species_titles_from_films(),
        )

        return {
            "id": int(uid),
            "birth_year": props.get("birth_year"),
            "eye_color": props.get("eye_color"),
            "gender": props.get("gender"),
            "hair_color": props.get("hair_color"),
            "homeworld": homeworld_title or props.get("homeworld"),
            "mass": props.get("mass"),
            "name": props.get("name"),
            "skin_color": props.get("skin_color"),
            "films": films,
            "vehicles": vehicles,
            "starships": starships,
            "species": species,
        }

    people_rows = await asyncio.gather(*(get_person_row(uid) for uid in uids_list))
    return [r for r in people_rows if r is not None]


async def insert_peoples(people_list: list[dict]):
    async with DbSession() as session:
        for item in people_list:
            person = SwapiPeople(
                id=item["id"],
                birth_year=item["birth_year"],
                eye_color=item["eye_color"],
                gender=item["gender"],
                hair_color=item["hair_color"],
                homeworld=item["homeworld"],
                mass=item["mass"],
                name=item["name"],
                skin_color=item["skin_color"],
                films=item.get("films", ""),
                species=item.get("species", ""),
                starships=item.get("starships", ""),
                vehicles=item.get("vehicles", ""),
            )
            session.add(person)
        await session.commit()


async def main():
    connector = aiohttp.TCPConnector(force_close=True)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_HTTP)
    title_cache: dict[str, str] = {}
    inflight: dict[str, asyncio.Future[str]] = {}
    json_cache: dict[str, dict | None] = {}
    json_inflight: dict[str, asyncio.Future[dict | None]] = {}

    async with aiohttp.ClientSession(timeout=TIMEOUT, connector=connector) as http_session:
        await init_orm()
        people_list = await get_people(
            http_session,
            semaphore,
            title_cache,
            inflight,
            json_cache,
            json_inflight,
        )
        await insert_peoples(people_list)
        await close_orm()


if __name__ == "__main__":
    asyncio.run(main())
