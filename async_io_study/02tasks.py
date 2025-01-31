import asyncio
import random


async def basic_coroutine(rand_num):
    print(f"[{rand_num}] started execution")
    await asyncio.sleep(rand_num)
    print(f"[{rand_num}] finished execution")
    return f"[{rand_num}]"

async def main_async():
    """
    :return:
     we can see results appearing together, or in sequential order
    """
    the_task_1 = asyncio.create_task(basic_coroutine(random.randrange(8)))
    # One of the reasons for the task creation
    # is to have a possibility of coroutine cancelling
    the_task_2 = asyncio.create_task(basic_coroutine(random.randrange(8)))
    the_task_3 = asyncio.create_task(basic_coroutine(random.randrange(8)))
    the_task_4 = asyncio.create_task(basic_coroutine(random.randrange(8)))
    print(f"{await the_task_1}, {the_task_1.get_name()} done?: {the_task_1.done()}")
    print(f"{await the_task_2}, {the_task_2.get_name()} done?: {the_task_2.done()}")
    print(f"{await the_task_3}, {the_task_3.get_name()} done?: {the_task_3.done()}")
    print(f"{await the_task_4}, {the_task_4.get_name()} done?: {the_task_4.done()}")

async def main_sync(wait_order: list[int]):

    for n in wait_order:
        the_task = asyncio.create_task(basic_coroutine(n))
        print(await the_task)

if __name__ == "__main__":
    # task not only a wrapper, it will start execution
    # as soon as created
    coro = main_async()
    print(type(coro))

    print("Running async example: ")
    asyncio.run(coro)

    # print("Running sync example: ")
    # coro = main_sync([1,20,3,4])
    # asyncio.run(coro)

