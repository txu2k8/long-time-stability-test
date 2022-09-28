#!/usr/bin/python
# -*- coding:utf-8 _*- 
"""
@author:TXU
@file:base
@time:2022/09/09
@email:tao.xu2008@outlook.com
@description:
"""
import random
import datetime
import asyncio
from loguru import logger

from workflow.base_workflow import BaseWorkflow


class BaseStress(BaseWorkflow):
    """操作处理 - 基本抽象，生产->消费模式"""

    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num=1, obj_prefix='', obj_num=10, multipart=False, local_path="",
            concurrent=1, prepare_concurrent=1, idx_width=1, idx_put_start=0, idx_del_start=0,
            depth=1, duration=0, cover=False,
    ):
        super(BaseStress, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num, obj_prefix, obj_num, multipart, local_path,
            concurrent, prepare_concurrent, idx_width, idx_put_start, idx_del_start
        )
        self.depth = depth
        self.duration = duration
        self.cover = cover

    def stage_init(self):
        """
        批量创建特定桶
        :return:
        """
        # 开启debug日志
        # self.set_core_loglevel()
        pass

    def stage_prepare(self):
        """
        批量创建特定对象
        :return:
        """
        pass

    async def worker(self, client, idx):
        """
        操作 worker，各个实例单独实现
        :param client:
        :param idx:
        :return:
        """
        logger.info('worker示例，待实例自定义')
        await asyncio.sleep(1)

    async def producer(self, queue: asyncio.Queue):
        """
        produce queue队列，每秒生产concurrent个，实际生产总数=obj_num * bucket_num（指定时间除外）
        数量优先，指定时间则覆盖idx循环操作
        :param queue:
        :return:
        """
        # 生产待处理的queue列表
        logger.info("PUT obj={}, bucket={}, concurrent={}, ".format(self.obj_num, self.bucket_num, self.concurrent))
        if self.duration <= 0:
            for x in range(self.idx_put_start, self.obj_num):
                logger.trace("producing {}/{}".format(x, self.obj_num))
                client = random.choice(self.client_list)  # 随机选择客户端
                await queue.put((client, x))
                if x % self.concurrent == 0:
                    await asyncio.sleep(1)  # 每秒生产 {concurrent} 个待处理项
                x += 1
            return

        logger.info("Run test duration {}s".format(self.duration))
        start = datetime.datetime.now()
        end = start
        produce_loop = 1
        idx_start = self.idx_put_start
        idx_end = self.obj_num
        while self.duration > (end - start).total_seconds():
            logger.info("Loop: {}".format(produce_loop))
            for idx in range(idx_start, idx_end):
                if self.duration <= (end - start).total_seconds():
                    break
                logger.trace("Loop-{} producing {}/{}".format(produce_loop, idx, self.obj_num))
                client = random.choice(self.client_list)  # 随机选择客户端
                await queue.put((client, idx))
                if idx % self.concurrent == 0:
                    await asyncio.sleep(1)  # 每秒生产 {concurrent} 个待处理项
                idx += 1
                end = datetime.datetime.now()
            produce_loop += 1
            if not self.cover:
                idx_start = idx_end
                idx_end = self.obj_num * produce_loop
        logger.info("duration {}s completed!".format(self.duration))
        return

    async def consumer(self, queue):
        """
        consume queue队列，指定队列中全部被消费
        :param queue:
        :return:
        """
        while True:
            item = await queue.get()
            await self.worker(*item)
            queue.task_done()

    async def stage_main(self):
        """
        执行 生产->消费 queue
        :return:
        """
        logger.log("STAGE", "main->执行测试，obj={}, bucket={}, concurrent={}".format(
            self.obj_num, self.bucket_num, self.concurrent
        ))

        queue = asyncio.Queue()
        # 创建100倍 concurrent 数量的consumer
        consumers = [asyncio.ensure_future(self.consumer(queue)) for _ in range(self.concurrent * 2000)]
        await self.producer(queue)
        await queue.join()
        for c in consumers:
            c.cancel()

    def run(self):
        self.stage_init()
        self.stage_prepare()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.stage_main())
        loop.close()


if __name__ == '__main__':
    pass
