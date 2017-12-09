import aiohttp
import asyncio
import backoff
import uvloop
import datetime
import async_timeout

from time import time

BASE_URL = 'https://www.mercadobitcoin.net/api'
RESOURCE = 'BTC/day-summary/{day}'
URL = '/'.join([BASE_URL,RESOURCE])
START = datetime.date(2016, 1, 1)
END = datetime.date(2017, 1, 1)

TIMEOUT_IN_SECONDS = 10


@backoff.on_exception(backoff.expo, asyncio.TimeoutError, max_tries=8)
async def get_daily_balance(url, session):
    with async_timeout.timeout(TIMEOUT_IN_SECONDS):
        async with session.get(url) as response:
            print(await response.text())


async def get_year_balance(from_year):
    async with aiohttp.ClientSession() as session:
        tasks = [
            asyncio.ensure_future(get_daily_balance(url, session))
            for url in _create_urls(start_year=from_year)
        ]
        return await asyncio.gather(*tasks)


def _create_urls(start_year):
    urls = []
    start = datetime.date(start_year, 1, 1)
    end  = datetime.date(start_year + 1, 1, 1)

    while start < end:
        url = URL.format(day=start.strftime('%Y/%m/%d'))
        yield url
        start = start + datetime.timedelta(days=1)


if __name__ == '__main__':
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    start_time = time()
    print('Starting executing script')
    loop.run_until_complete(
        get_year_balance(from_year=2016)
    )
    print('\n\nDone! It took {} seconds'.format(time() - start_time))
    loop.close()
    