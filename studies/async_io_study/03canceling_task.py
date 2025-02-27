import asyncio
import random


async def basic_coroutine(rand_num):
    print(f"[{rand_num}] started execution")
    await asyncio.sleep(rand_num)
    print(f"[{rand_num}] finished execution")
    return f"[{rand_num}]"

async def main_async_slow():
    the_task = asyncio.create_task(basic_coroutine(random.randrange(8)))

    # seconds = 0
    # while not the_task.done():
    #     await asyncio.sleep(1)
    #     seconds +=1
    #     if  seconds == 5:
    #         the_task.cancel()
    # try:
    #     print(f"{await the_task}, {the_task.get_name()} done?: {the_task.done()}")
    # except asyncio.CancelledError as er:
    #     print("Execution was interrupted.")

    try:
        # asyncio.shield
        shield_task = asyncio.wait_for(asyncio.shield(the_task),timeout=5)
        print(type(shield_task))
        result = await shield_task
        print(result)
    except asyncio.TimeoutError as err:
        print(f"Execution was interrupted: {err.errno}")
        # `shield` allows us to get the result even after an interruption
        result = await the_task
        print(result)

if __name__ == "__main__":

    coro = main_async_slow()
    asyncio.run(coro)

