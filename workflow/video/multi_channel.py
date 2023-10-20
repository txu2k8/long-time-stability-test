#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author:TXU
@file:multi_channel
@time:2023/5/27
@email:tao.xu2008@outlook.com
@description: 视频监控场景测试 - 文件存储 - 多路视频数据流并发执行
"""
import asyncio
from concurrent.futures import ProcessPoolExecutor, as_completed
from loguru import logger

from cli.log import init_logger
from utils.util import split_list_n_list
from workflow.video.fs.one_channel import FSVideoWorkflowOneChannel


async def _run(one_channel_class, process_channel_list, **kwargs):
    try:
        ch_start, ch_end = process_channel_list[0], process_channel_list[-1]
        init_logger(prefix=f"video_fs_{ch_start}_{ch_end}", trace=kwargs["trace"])
    except Exception as e:
        logger.error(e)

    kwargs.pop("trace")

    tasks = []
    for channel_id in process_channel_list:
        kwargs["channel_id"] = channel_id
        vm_obj = one_channel_class(**kwargs)
        tasks.append(asyncio.ensure_future(vm_obj.run()))
    results = await asyncio.gather(*tasks)
    for result in results:
        print(f"Task result:{result}")


def _process_start(one_channel_class, process_channel_list, **kwargs):
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(_run(one_channel_class, process_channel_list, **kwargs))
    loop.run_forever()


def multi_channel_run(one_channel_class, channel_num, process_workers=1, **kwargs):
    """
    模拟多路视频 并发执行
    :param one_channel_class:
    :param channel_num:
    :param process_workers:
    :param kwargs:
    :return:
    """
    process_channel_lists = split_list_n_list(range(channel_num), process_workers)
    futures = []
    with ProcessPoolExecutor(max_workers=process_workers) as pool:
        for pcl in process_channel_lists:
            futures.append(
                pool.submit(
                    _process_start, one_channel_class, pcl, **kwargs
                )
            )
    for future in as_completed(futures):
        print(future.result())


if __name__ == '__main__':
    pass
