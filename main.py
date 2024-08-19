import os
import sys
import asyncio
import logging
import requests
import time
import signal
from pprint import pp
from random import choice
from datetime import datetime
from dataclasses import dataclass, asdict
from threading import Thread
from concurrent.futures import ProcessPoolExecutor, wait, as_completed

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from aiohttp import ClientSession, ClientConnectorError, ContentTypeError, TCPConnector
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection

import backoff

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(processName)s | %(asctime)s - %(message)s'
)

logger = logging.getLogger(__name__)

base_url = 'https://egrul.itsoft.ru/sitemap/'
base_url_inn = 'https://egrul.itsoft.ru/'
database = os.environ.get('DATABASE', 'postgresql+asyncpg://zaurbektedeev:123@127.0.0.1:5432/inn')
proxies_url = os.environ.get('PROXIES_URL')

@dataclass()
class Info:
    inn: str
    type: str | None
    name: str | None
    main_okved: str | None
    additionally_okved: str | None
    region: str | None


@dataclass()
class Proxy:
    proxy: str
    country: str
    user_agent: str


def get_proxies(count: int = 1, country: str = None) -> list[Proxy]:
    url = proxies_url + f'v1/web/settings?count={count}'
    if country:
        url += f'&country={country}'

    response = requests.get(url)

    proxies = response.json()
    return [
        Proxy(proxy=p['proxy'], country=p['country'], user_agent=p['user_agent'])
        for p in proxies
    ]


PROXIES = [p.proxy for p in get_proxies(30, country='RU')]


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


@backoff.on_exception(backoff.expo, (Exception,), max_time=120, logger=logger)
async def get_inn(link: str, session: ClientSession):
    async with session.get(
            link,
            proxy=choice(PROXIES),
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                              'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15'
            }
    ) as response:
        soup = BeautifulSoup(await response.text(), 'html.parser')
        inns = [a.attrs['href'].strip('/') for a in soup.find_all('a')]

    logger.info('Got %s INNs from %s', len(inns), link)

    return inns


async def collect_inns(links: list[str]):
    engine = create_async_engine(database)

    async with ClientSession() as session:
        async with engine.begin() as connection:
            result = await asyncio.gather(
                *[
                    get_inn(base_url + link, session)
                    for link in links
                ],
                return_exceptions=True
            )

            async def _save(arguments):
                await connection.execute(
                    text(
                        '''
                        INSERT INTO companies(inn) VALUES (:inn)
                        ON CONFLICT (inn) DO NOTHING
                        '''
                    ), arguments
                )
                logger.info('Saved %s inns', len(arguments))

            await asyncio.gather(
                *[
                    _save([{'inn': i} for i in r])
                    for r in result if not isinstance(r, Exception)
                ]
            )

            await connection.commit()


def _collect(c):
    asyncio.run(collect_inns(c))


@backoff.on_exception(backoff.expo, (ClientConnectorError, ContentTypeError), max_time=120, logger=logger)
async def get_inn_info(inn: str, session: ClientSession) -> Info:
    t1 = datetime.now()
    async with session.get(
            base_url_inn + inn + '.json',
            # proxy=choice(PROXIES),
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                              'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15'
            }
    ) as response:

        body = await response.json()

        if isinstance(body, bool):
            return Info(inn, None, None, None, None, None)

        if body.get('error', None) is not None:
            return Info(inn, None, None, None, None, None)

        if body.get('СвЮЛ', None) is None:
            main_key = 'СвИП'
            name = ' '.join(body['СвИП']['СвФЛ']['ФИОРус']['@attributes'].values())
            company_type = body['СвИП']['@attributes']['НаимВидИП']
            city = body['СвИП']['СвРегОрг']['@attributes']['НаимНО']
        else:
            main_key = 'СвЮЛ'
            name = body['СвЮЛ']['СвНаимЮЛ']['@attributes']['НаимЮЛПолн']
            company_type = body['СвЮЛ']['@attributes'].get('ПолнНаимОПФ', None)
            if body['СвЮЛ']['СвАдресЮЛ'].get('АдресРФ', None):
                city = body['СвЮЛ']['СвАдресЮЛ']['АдресРФ']['Регион']['@attributes']['НаимРегион']
            else:
                city = body['СвЮЛ']['СвАдресЮЛ']['СвАдрЮЛФИАС']['НаимРегион']

        okved = body[main_key].get('СвОКВЭД', None)
        if not okved:
            return Info(inn, company_type, name, None, None, city)

        if okved.get('СвОКВЭДОсн', None) is None:
            main_okved = None
        else:
            main_okved = okved['СвОКВЭДОсн']['@attributes']['КодОКВЭД'] + ' ' + \
                         okved['СвОКВЭДОсн']['@attributes']['НаимОКВЭД']

        dop_okved = okved.get('СвОКВЭДДоп', [])
        if not isinstance(dop_okved, list):
            dop_okved = [dop_okved]

        dop_okveds = [i['@attributes']['КодОКВЭД'] + ' ' + i['@attributes']['НаимОКВЭД'] for i in dop_okved]
        logger.debug('Got info for %s by %s sec', inn, (datetime.now() - t1).total_seconds())
        return Info(inn, company_type, name, main_okved, '; '.join(dop_okveds), city)


async def inns_by_chunks(connection: AsyncConnection, start: int, end: int, chunk_size: int):
    offset = start

    while offset < end:

        result = await connection.execute(
            text(
                '''
                SELECT inn FROM companies
                ORDER BY inn
                LIMIT :limit
                OFFSET :offset
                '''
            ), dict(limit=chunk_size if offset + chunk_size < end else end - offset, offset=offset)
        )
        if offset + chunk_size > end:
            offset = end
        else:
            offset += chunk_size

        yield result.scalars().all()


async def update_inns(interval: tuple[int, int, int], chunk_size: int = 10):
    engine = create_async_engine(database)

    async with engine.begin() as connection:
        async with ClientSession(connector=TCPConnector(verify_ssl=False)) as session:
            result = []
            total = 0
            async for i in inns_by_chunks(connection, interval[0], interval[1], chunk_size):
                result.extend(
                    await asyncio.gather(
                        *[
                            get_inn_info(inn, session)
                            for inn in i
                        ]
                    )
                )

                total += chunk_size
                logger.info('Status of %s interval %s/%s', interval[2], total, interval[1] - interval[0])

            await connection.execute(
                text(
                    '''
                    UPDATE companies SET type = :type, name = :name, main_okved = :main_okved,
                    additionally_okved = :additionally_okved, region = :region

                    WHERE inn = :inn
                    '''
                ), [asdict(i) for i in result]
            )
            logger.info('Updated %s inns', len(result))

        await connection.commit()


def _update(interval: tuple[int, int, int], chunk_size: int = 10):
    asyncio.run(update_inns(interval, chunk_size))


def start_thread_to_terminate_when_parent_process_dies(ppid):
    pid = os.getpid()

    def f():
        while True:
            try:
                os.kill(ppid, 0)
            except OSError:
                os.kill(pid, signal.SIGTERM)
            time.sleep(1)

    thread = Thread(target=f, daemon=True)
    thread.start()


async def start_update_inns(interval_size: int, workers: int = 5, chunk_size: int = 10):
    engine = create_async_engine(database)

    async with engine.begin() as connection:
        count = (await connection.execute(
            text(
                '''
                SELECT COUNT(inn) FROM companies
                '''
            )
        )).scalar()

        part_count = count // interval_size
        intervals = []

        start = 0
        for i in range(part_count):
            intervals.append(
                (start, start + interval_size, i + 1)
            )
            start += interval_size

        intervals.append(
            (start, count)
        )

        logger.info('Intervals count %s', len(intervals))
        completed = 0
        with ProcessPoolExecutor(
                max_workers=workers,
                initializer=start_thread_to_terminate_when_parent_process_dies,
                initargs=(os.getpid(), ),
        ) as executor:
            futures = []
            for interval in intervals:
                futures.append(
                    executor.submit(
                        _update, interval, chunk_size
                    )
                )

            try:
                for future in as_completed(futures):
                    future.result()
                    completed += 1
                    logger.info('Completed %s/%s', completed, len(intervals))
            except KeyboardInterrupt:
                executor.shutdown(wait=True)


def start_collect_inns(workers: int = 5):
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = [a.attrs['href'] for a in soup.find_all('a')]

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = []
        for chunk in chunks(links, len(links) // workers):
            futures.append(
                executor.submit(
                    _collect, chunk
                )
            )

        try:
            for future in as_completed(futures):
                future.result()
        except KeyboardInterrupt:
            executor.shutdown(wait=True)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == 'collect':
            start_collect_inns(int(sys.argv[2]))

        if sys.argv[1] == 'update':
            asyncio.run(start_update_inns(int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])))
    else:
        asyncio.run(start_update_inns(10_000, 1, 50))
