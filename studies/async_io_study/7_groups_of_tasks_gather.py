import aiohttp
import asyncio
import os


class AsyncHttpRequest:

    def __init__(self, url: str):
        self.url: str = url

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        response = await self.session.get(self.url)
        return response

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

class ServerError(Exception):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


async def check_url(url: str):

    async with AsyncHttpRequest(url) as r:
        html = await r.text()
        print(os.linesep,"[=Response received=]")
        print(html[:100])

    return  "[DONE]"

async def server_return_error():
    await asyncio.sleep(0.1)
    raise  ServerError("++++ERROR++++")

async def check_all_urls():

    coros = [check_url("https://map.ukrainealarm.com/"), check_url("https://rivne-piano.com/"), check_url("https://www.youtube.com/")]

    result = await asyncio.gather(*coros, server_return_error(), return_exceptions=True)

    print(result)



if __name__ == '__main__':

    asyncio.run(check_all_urls())




