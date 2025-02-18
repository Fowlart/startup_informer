import asyncio
import random


async def basic_coroutine(rand_num):
    print(f"[{rand_num}] started execution")
    # await used to call another coroutine
    await asyncio.sleep(rand_num)

    print(f"[{rand_num}] finished execution")

    return f"[{rand_num}]"

async def main():

    # `gather` allows code execution in a non-blocking manner
    result = await asyncio.gather(basic_coroutine(random.randrange(5)),
                                  basic_coroutine(random.randrange(5)),
                                  basic_coroutine(random.randrange(5)))
    return result

if __name__ == "__main__":
    # error - trying to execute outside event-loop
    # result = basic_coroutine()

    coro = main()

    print(type(coro))

    # run - provide an event-loop
    # we have a low-level API to interact with event-loop
    # mostly we need to run `run`
    # we can have only one event loop, per thread
    result = asyncio.run(coro)

    print(result)