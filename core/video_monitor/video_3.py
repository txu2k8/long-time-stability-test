import asyncio
import threading
import multiprocessing
from multiprocessing import Queue, Pool, Process
# import aiohttp
import os
from cli.log import init_logger
from loguru import logger


async def hello(name):
    logger.info('hello {}   {}**********{}'.format(name, os.getpid(), threading.current_thread()))
    # await asyncio.sleep(int(name))
    await asyncio.sleep(1)
    logger.info('end:{}  {}'.format(name, os.getpid()))


def process_start(*namelist):
    tasks = []
    loop = asyncio.get_event_loop()
    for name in namelist:
        tasks.append(asyncio.ensure_future(hello(name)))
    loop.run_until_complete(asyncio.wait(tasks))


def task_start(namelist):
    i = 0
    lst = []
    flag = 10
    while namelist:
        i += 1
        l = namelist.pop()
        lst.append(l)
        if i == flag:
            p = Process(target=process_start, args=lst)
            p.start()
            # p.join()
            lst = []
            i = 0
    if namelist:
        p = Process(target=process_start, args=lst)
        p.start()
        # p.join()


if __name__ == '__main__':
    init_logger("video_3")
    # 测试使用多个进程来实现函数
    namelist = list('0123456789' * 10)
    print(namelist)
    task_start(namelist)

# 测试使用一个异步io来实现全部函数
# loop=asyncio.get_event_loop()
# tasks=[]
# namelist=list('0123456789'*10)
# for i in namelist:
#     tasks.append(asyncio.ensure_future(hello(i)))
# loop.run_until_complete(asyncio.wait(tasks))
