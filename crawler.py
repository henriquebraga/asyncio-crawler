import asyncio
import datetime
from time import time

import aiohttp
import async_timeout
import backoff
import uvloop

BASE_URL = 'https://www.mercadobitcoin.net/api'
RESOURCE = 'BTC/day-summary/{day}'
URL = '/'.join([BASE_URL,RESOURCE])
REQUEST_TIMEOUT_IN_SECONDS = 10


@backoff.on_exception(backoff.expo, asyncio.TimeoutError, max_tries=8)
async def get_daily_balance(url, session):
    with async_timeout.timeout(REQUEST_TIMEOUT_IN_SECONDS):
        async with session.get(url) as response:
            print(await response.text())


async def get_year_balance(from_year):
    async with aiohttp.ClientSession() as session:
        get_year_balance_tasks = [
            asyncio.ensure_future(get_daily_balance(url, session))
            for url in format_urls_from_year_by_day(year=from_year)
        ]
        return await asyncio.gather(*get_year_balance_tasks)


def format_urls_from_year_by_day(year):
    start = datetime.date(year, 1, 1)
    end  = datetime.date(year + 1, 1, 1)

    while start < end:
        yield format_url_from_day(from_day=start)
        start = start + datetime.timedelta(days=1)


def format_url_from_day(from_day):
    return URL.format(day=from_day.strftime('%Y/%m/%d'))


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
