import aiohttp
import asyncio
import os

class ContextManagerProtocolImplementer:

    def __init__(self, filename: str):
        self.filename = filename

    def __enter__(self):
        self.file_object = open(self.filename, "r")
        return self.file_object

    def __exit__(self, exception_type, exc_value, traceback):
        if self.file_object:
            self.file_object.close()

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


async def check_url(url: str):

    async with AsyncHttpRequest(url) as r:
        html = await r.text()
        print(os.linesep,"[=Response received=]")
        print(html[:100])

    return  html

async def check_all_urls():
    await asyncio.gather(check_url("https://map.ukrainealarm.com/"), check_url("https://facebook.com"), check_url("https://www.youtube.com/"))

if __name__ == '__main__':

    asyncio.run(check_all_urls())




