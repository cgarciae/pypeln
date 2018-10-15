import asyncio
import threading

async def task_a():
    while True:
        await asyncio.sleep(10)

async def task_b():
    print("Task B")


def run_a(loop, task):
    loop.run_until_complete(task)


loop = asyncio.get_event_loop()

t = threading.Thread(target=run_a, args=(loop, task_a()))
t.daemon = True
t.start()

loop.call_soon_threadsafe(asyncio.ensure_future, task_b())


t.join()