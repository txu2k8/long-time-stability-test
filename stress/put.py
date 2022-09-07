#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:put
@time:2022/09/07
@email:tao.xu2008@outlook.com
@description: 上传对象
"""
import random
import datetime
import asyncio
from loguru import logger

from utils.util import get_local_files
from config.models import ClientType
from stress.client import init_clients
from stress.bucket import generate_bucket_name, make_bucket_if_not_exist


class PutObject(object):
    """上传对象"""
    def __init__(
            self,
            client_type, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num=1, depth=1, obj_prefix='', obj_num=1,
            concurrent=1, disable_multipart=False,
            duration=''
    ):
        self.client_type = client_type
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.tls = tls
        self.alias = alias
        self.local_path = local_path
        self.bucket_prefix = bucket_prefix
        self.bucket_num = bucket_num
        self.depth = depth
        self.obj_num = obj_num
        self.concurrent = concurrent
        self.disable_multipart = disable_multipart
        self.duration = duration
        # 对象目录处理
        nested = ""
        if depth > 1:
            for d in range(2, depth + 1):  # depth=2为第一级文件夹
                nested += f'nested{d - 1}/'
        self.obj_prefix = nested + obj_prefix

        # 准备源数据文件池 字典
        self.file_list = get_local_files(local_path)

        # 初始化客户端
        self.clients_info = init_clients(
            self.client_type, self.endpoint, self.access_key, self.secret_key, self.tls, self.alias)

    def prepare(self, client=None):
        client = client or random.choice(list(self.clients_info.values()))
        # 开启debug日志
        # mc_client = self.clients_info[ClientType.MC.name]
        # mc_client.set_core_loglevel('debug')

        # 准备桶
        make_bucket_if_not_exist(client, self.bucket_prefix, self.bucket_num)

    async def worker(self, idx, client):
        """
        上传对象
        :param idx:
        :param client:
        :return:
        """
        # 准备
        bucket_idx = idx % self.bucket_num
        bucket = generate_bucket_name(self.bucket_prefix, bucket_idx)
        src_file = random.choice(self.file_list)
        obj_path = f"{self.obj_prefix}{str(idx)}"
        await client.put(src_file.full_path, bucket, obj_path, self.disable_multipart, tags=src_file.tags)

    async def producer(self, queue):
        """
        produce queue队列，每秒生产concurrent个
        :param queue:
        :return:
        """
        # 随机选择客户端
        client = random.choice(list(self.clients_info.values()))
        # self.prepare(client)

        # 生产待上传queue列表
        if self.duration:
            # 持续上传时间
            logger.info("Run test duration {}s, concurrent={}".format(self.duration, self.concurrent))
            idx = 0
            start = datetime.datetime.now()
            end = datetime.datetime.now()
            while int(self.duration) > (end - start).total_seconds():
                logger.debug("producing {}".format(idx))
                await queue.put((idx, client))
                if idx % self.concurrent == 0:
                    await asyncio.sleep(1)
                idx += 1
                end = datetime.datetime.now()
            logger.info("duration {}s completed!".format(self.duration))
        else:
            # 指定上传数量
            logger.info("PUT obj_num={}, concurrent={}".format(self.obj_num, self.concurrent))
            for x in range(self.obj_num):
                logger.debug("producing {}/{}".format(x, self.obj_num))
                await queue.put((x, client))
                if x % self.concurrent == 0:
                    await asyncio.sleep(1)

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

    async def run(self):
        """
        执行 生产->消费 queue
        :return:
        """
        queue = asyncio.Queue()
        # 创建100倍 concurrent 数量的consumer
        consumers = [asyncio.ensure_future(self.consumer(queue)) for _ in range(self.concurrent * 100)]
        await self.producer(queue)
        await queue.join()
        for c in consumers:
            c.cancel()


if __name__ == '__main__':
    put = PutObject(
        'mc', '127.0.0.1:9000', 'minioadmin', 'minioadmin', False, 'play',
        'D:\\minio\\upload_data', 'bucket', 1, 2, '', 10,
        3, False, '30'
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(put.run())
    loop.close()

