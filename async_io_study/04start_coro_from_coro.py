import asyncio

async def additional():
    print(f"additional stared execution")
    await asyncio.sleep(3)
    print(f"additional finished execution")

async def main(n: int):
    print(f"main stared execution")

    # next code ===will NOT=== allow to close the application
    # before `additional` is finished
    # await additional()

    # next code ===WILL=== allow to close the application
    # before `additional` is finished
    asyncio.create_task(coro=additional())
    await asyncio.sleep(n)
    print(f"main finished execution")

if __name__=="__main__":
    asyncio.run(main(2))