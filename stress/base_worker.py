#!/usr/bin/python
# -*- coding:utf-8 _*- 
"""
@author:TXU
@file:base_operation
@time:2022/09/09
@email:tao.xu2008@outlook.com
@description:
"""
import re
import random
import datetime
import asyncio
from collections import defaultdict
from typing import List, Text
from loguru import logger

from utils.util import zfill
from config.models import ClientType
from client.clientinterface import ClientInterface
from client.mc import MClient
from client.s3cmd import S3CmdClient


def init_clients(client_types: List[Text], endpoint, access_key, secret_key, tls, alias='play'):
    """
    初始化IO客户端
    :param client_types:
    :param endpoint:
    :param access_key:
    :param secret_key:
    :param tls:
    :param alias:
    :return:
    """
    clients_info = defaultdict(ClientInterface)
    for client_type in client_types:
        logger.info("初始化工具Client({})...".format(client_type))
        if client_type.upper() == ClientType.MC.value:
            client = MClient(endpoint, access_key, secret_key, tls, alias)
        elif client_type.upper() == ClientType.S3CMD.value:
            client = S3CmdClient(endpoint, access_key, secret_key, tls)
        else:
            raise Exception("仅支持工具：{}".format(ClientType.value))
        clients_info[client_type] = client

    return clients_info


class BaseWorker(object):
    """操作处理 - 基本抽象，生产->消费模式"""

    def __init__(
            self,
            client_type, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num=1, depth=1, obj_prefix='', obj_num=1,
            concurrent=1, multipart=False,
            duration=0, cover=False
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
        self.obj_prefix = obj_prefix
        self.obj_num = obj_num
        self.concurrent = concurrent
        self.multipart = multipart
        self.duration = duration
        self.cover = cover
        # 初始化客户端
        self.clients_info = init_clients(
            self.client_type, self.endpoint, self.access_key, self.secret_key, self.tls, self.alias)
        self.client_list = list(self.clients_info.values())

    def disable_multipart_calc(self):
        """
        计算 disable_multipart
        :return:
        """
        if self.multipart == 'enable':
            return False
        elif self.multipart == 'disable':
            return True
        else:
            return random.choice([True, False])

    @staticmethod
    def bucket_name_calc(bucket_prefix, idx):
        """
        依据bucket前缀和idx序号计算bucket名称
        :param bucket_prefix:
        :param idx:
        :return:
        """
        return '{}{}'.format(bucket_prefix, idx)

    @staticmethod
    def obj_prefix_calc(obj_prefix, depth):
        # date_prefix = datetime.date.today().strftime("%Y-%m-%d") + '/'  # 按日期写入不同文件夹
        date_prefix = ''
        nested_prefix = ""
        for d in range(2, depth + 1):  # depth=2开始创建子文件夹，depth=1为日期文件夹
            nested_prefix += f'nested{d - 1}/'
        prefix = date_prefix + nested_prefix + obj_prefix
        return prefix

    def obj_path_calc(self, idx):
        """
        依据idx序号计算对象 path，实际为：<bucket_name>/{obj_path}
        :param idx:
        :return:
        """
        obj_prefix = self.obj_prefix_calc(self.obj_prefix, self.depth)
        obj_path = obj_prefix + zfill(idx)
        return obj_path

    def set_core_loglevel(self, loglevel="debug"):
        """
        MC命令设置core日志级别
        :param loglevel:
        :return:
        """
        client = self.clients_info[ClientType.MC.value] or \
                 MClient(self.endpoint, self.access_key, self.secret_key, self.tls, self.alias)
        client.set_core_loglevel(loglevel)

    def make_bucket_if_not_exist(self, client, bucket_prefix, bucket_num):
        """
        批量创建bucket（如果不存在）
        :param client:
        :param bucket_prefix:
        :param bucket_num:
        :return:
        """
        logger.info("批量创建bucket（如果不存在）...")
        for idx in range(bucket_num):
            bucket = self.bucket_name_calc(bucket_prefix, idx)
            client.mb(bucket)

    def prepare(self):
        # 开启debug日志
        # self.set_core_loglevel()
        pass

    async def worker(self, client, bucket, idx):
        """
        操作 worker，各个实例单独实现
        :param client:
        :param bucket:
        :param idx:
        :return:
        """
        logger.info('worker示例，待实例自定义')
        await asyncio.sleep(1)

    async def producer1(self, queue):
        """
        produce queue队列，每秒生产concurrent个，实际生产总数=obj_num * bucket_num（指定时间除外）
        时间优先，指定时间则累加idx持续操作
        :param queue:
        :return:
        """
        # 生产待处理的queue列表
        if self.duration > 0:
            # 持续时间
            logger.info("Run test duration {}s, concurrent={}".format(self.duration, self.concurrent))
            idx = 0
            total = 0
            start = datetime.datetime.now()
            end = datetime.datetime.now()
            while int(self.duration) > (end - start).total_seconds():
                logger.trace("producing {}".format(idx))
                client = random.choice(self.client_list)  # 随机选择客户端
                for bucket_idx in range(self.bucket_num):  # 依次处理每个桶中数据：写、读、删、列表、删等
                    bucket = self.bucket_name_calc(self.bucket_prefix, bucket_idx)
                    await queue.put((client, bucket, idx))
                    if total % self.concurrent == 0:
                        await asyncio.sleep(1)  # 每秒生产 {concurrent} 个待处理项
                    total += 1
                idx += 1
                end = datetime.datetime.now()
            logger.info("duration {}s completed!".format(self.duration))
        else:
            # 指定处理数量
            logger.info("PUT obj_num={}, concurrent={}".format(self.obj_num, self.concurrent))
            total = 0
            for x in range(self.obj_num):
                logger.trace("producing {}/{}".format(x, self.obj_num))
                client = random.choice(self.client_list)  # 随机选择客户端
                for bucket_idx in range(self.bucket_num):  # 依次处理每个桶中数据：写、读、删、列表、删等
                    bucket = self.bucket_name_calc(self.bucket_prefix, bucket_idx)
                    await queue.put((client, bucket, x))
                    if total % self.concurrent == 0:
                        await asyncio.sleep(1)  # 每秒生产 {concurrent} 个待处理项
                    total += 1

        return True

    async def producer2(self, queue):
        """
        produce queue队列，每秒生产concurrent个，实际生产总数=obj_num * bucket_num（指定时间除外）
        数量优先，指定时间则覆盖idx循环操作
        :param queue:
        :return:
        """
        # 生产待处理的queue列表
        logger.info("PUT obj={}, bucket={}, concurrent={}, ".format(self.obj_num, self.bucket_num, self.concurrent))
        if self.duration <= 0:
            return await self.producer1(queue)

        logger.info("Run test duration {}s".format(self.duration))
        start = datetime.datetime.now()
        end = start
        produce_loop = 1
        while self.duration > (end - start).total_seconds():
            logger.info("Loop: {}".format(produce_loop))
            total = 0
            for idx in range(self.obj_num):
                if self.duration <= (end - start).total_seconds():
                    break
                logger.trace("Loop-{} producing {}/{}".format(produce_loop, idx, self.obj_num))
                client = random.choice(self.client_list)  # 随机选择客户端
                for bucket_idx in range(self.bucket_num):  # 依次处理每个桶中数据：写、读、删、列表、删等
                    bucket = self.bucket_name_calc(self.bucket_prefix, bucket_idx)
                    await queue.put((client, bucket, idx))
                    if total % self.concurrent == 0:
                        await asyncio.sleep(1)  # 每秒生产 {concurrent} 个待处理项
                    total += 1
                end = datetime.datetime.now()
            produce_loop += 1

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

    async def run(self):
        """
        执行 生产->消费 queue
        :return:
        """
        self.prepare()

        queue = asyncio.Queue()
        # 创建100倍 concurrent 数量的consumer
        consumers = [asyncio.ensure_future(self.consumer(queue)) for _ in range(self.concurrent * 100)]
        if self.cover:
            await self.producer2(queue)  # 数量优先，指定时间则覆盖idx循环操作
        else:
            await self.producer1(queue)  # 时间优先，指定时间则累加idx持续操作
        await queue.join()
        for c in consumers:
            c.cancel()


if __name__ == '__main__':
    bw = BaseWorker(
        'mc', '127.0.0.1:9000', 'minioadmin', 'minioadmin', False, 'play',
        'D:\\minio\\upload_data', 'bucket', 1, 2, '', 10,
        3, False, '30'
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(bw.run())
    loop.close()
