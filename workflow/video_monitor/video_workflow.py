#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:base
@time:2022/09/28
@email:tao.xu2008@outlook.com
@description: 视频监控场景测试 - 基类
需求：
    1、单节点1000路视频
    2、码流：4Mbps
    3、7*24h 写删均衡
"""
import random
import asyncio
from abc import ABC
from loguru import logger

from utils.util import get_local_files
from config.models import ClientType


from workflow.workflow_base import WorkflowBase
from workflow.workflow_interface import WorkflowInterface


class VideoWorkflow(WorkflowBase, WorkflowInterface, ABC):
    """
    视频监控场景测试 - 基类 - 写删相同协程中串行处理，多对象协程并行处理
    1、init阶段：新建桶，初始化数据库用于存储写入的对象路径
    2、Prepare阶段：预埋对象，平均分配到桶中，预埋数据并行数=prepare_concurrent
    3、Main阶段：测试执行 上传、下载、列表、删除等，并行数=concurrent
    """

    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num=1, obj_prefix='', obj_num=10, multipart=False, local_path="",
            main_concurrent=1, prepare_concurrent=1, max_workers=1, idx_width=1, idx_put_start=0, idx_del_start=0,
            obj_num_per_day=1,
    ):
        super(VideoWorkflow, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num, obj_prefix, obj_num, multipart, local_path,
            main_concurrent, prepare_concurrent, idx_width, idx_put_start, idx_del_start
        )
        self.max_workers = max_workers if max_workers > main_concurrent else main_concurrent
        self.obj_num_per_day = obj_num_per_day

        # 准备源数据文件池 字典
        self.file_list = get_local_files(local_path)
        # 写入起始日期
        self.start_date = "2022-01-01"
        # 初始化客户端
        self.client = self.clients_info[ClientType.MC.value]

    def bucket_obj_path_calc(self, idx):
        """
        基于对象idx计算该对象应该存储的桶和对象路径
        :param idx:
        :return:
        """
        # 计算对象应该存储的桶名称
        bucket = self.bucket_name_calc(self.bucket_prefix, idx)
        # 计算对象路径
        date_step = idx // self.obj_num_per_day  # 每日写N个对象，放在一个日期命名的文件夹
        current_date = self.date_str_calc(self.start_date, date_step)
        date_prefix = current_date + '/'
        obj_path = self.obj_path_calc(idx, date_prefix)
        return bucket, obj_path, current_date

    async def worker(self, *args, **kwargs):
        """
        worker
        :param args:
        :param kwargs:
        :return:
        """
        await asyncio.sleep(0)

    async def producer_prepare(self, queue):
        """
        prepare阶段 produce queue队列
        :param queue:
        :return:
        """
        logger.info("Produce PREPARE bucket={}, concurrent={}, ".format(self.bucket_num, self.prepare_concurrent))
        client = self.client_list[0]
        idx = self.idx_put_start
        while idx <= self.obj_num:
            await queue.put((client, idx))
            self.idx_put_current = idx
            if idx % self.prepare_concurrent == 0:
                await asyncio.sleep(1)  # 每秒生产 {prepare_concurrent} 个待处理项
            idx += 1

    async def producer_main(self, queue):
        """
        main阶段 produce queue队列
        :param queue:
        :return:
        """
        logger.info("Produce Main PUT/DEL bucket={}, concurrent={}, ".format(self.bucket_num, self.main_concurrent))
        client = self.client_list[0]
        idx = self.idx_main_start
        while idx <= self.obj_num:
            await queue.put((client, idx))
            self.idx_put_current = idx
            if idx % self.main_concurrent == 0:
                await asyncio.sleep(1)  # 每秒生产 {main_concurrent} 个待处理项
            idx += 1

    async def consumer(self, queue):
        """
        consume queue队列，指定队列中全部被消费
        :param queue:
        :return:
        """
        while True:
            item = await queue.async_get()
            await self.worker(*item)
            queue.task_done()

    def stage_init(self):
        """
        批量创建特定桶
        :return:
        """
        logger.log("STAGE", "init->批量创建特定桶、初始化数据库，bucket_prefix={}, bucket_num={}".format(
            self.bucket_prefix, self.bucket_num
        ))

        # 开启debug日志
        # self.set_core_loglevel()

        # 准备桶
        client = random.choice(self.client_list)
        self.make_bucket_if_not_exist(client, self.bucket_prefix, self.bucket_num)

        # 初始化数据库
        self.db_init()

        logger.log("STAGE", "初始化完成！bucket_prefix={}, bucket_num={}".format(self.bucket_prefix, self.bucket_num))

    async def stage_prepare(self):
        """
        批量创建特定对象
        :return:
        """
        logger.log("STAGE", "prepare->预置对象，PUT obj={}, bucket={}, concurrent={}".format(
            self.obj_num, self.bucket_num, self.prepare_concurrent
        ))
        queue = asyncio.Queue()
        # 创建100倍 concurrent 数量的consumer
        consumers = [asyncio.ensure_future(self.consumer(queue)) for _ in range(self.prepare_concurrent * 2)]

        await self.producer_prepare(queue)
        await queue.join()
        for c in consumers:
            c.cancel()
        logger.log("STAGE", "预置对象完成！obj={}, bucket={}".format(self.obj_num, self.bucket_num))
        logger.log("STAGE", "销毁预置阶段consumers！len={}".format(len(consumers)))
        await asyncio.sleep(5)

    async def stage_main(self):
        """
        执行 生产->消费 queue
        :return:
        """
        logger.log("STAGE", "main->写删均衡测试，idx_main_start={}, bucket={}, concurrent={}".format(
            self.idx_main_start, self.bucket_num, self.main_concurrent
        ))

        queue = asyncio.Queue()
        # 创建 max_workers 数量的consumer
        consumers = [asyncio.ensure_future(self.consumer(queue)) for _ in range(self.max_workers)]

        await self.producer_main(queue)
        await queue.join()
        for c in consumers:
            c.cancel()

    def run(self):
        """
        执行 生产->消费 queue
        :return:
        """
        self.stage_init()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.stage_prepare())

        asyncio.ensure_future(self.stage_main())
        loop.run_forever()
