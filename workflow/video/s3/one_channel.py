#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:one_channel.py
@time:2022/09/28
@email:tao.xu2008@outlook.com
@description: 视频监控场景测试 - 对象存储 - 模拟一路视频数据流
"""
import random
import datetime
import asyncio
from abc import ABC
import arrow
from loguru import logger

from utils.util import zfill
from client.s3.s3_api import S3API
from workflow.workflow_base import InitDB
from workflow.workflow_interface import WorkflowInterface
from workflow.video.calculate import VSInfo


class S3VideoWorkflowOneChannel(WorkflowInterface, ABC):
    """
    视频监控场景测试 - 基类（支持追加写）
    1、init阶段：新建桶，初始化数据库用于存储写入的对象路径
    2、Prepare阶段：预埋对象
    3、Main阶段：测试执行 上传、删除等
    """

    def __init__(self, client, file_list, channel_id, vs_info: VSInfo):
        # 初始化客户端
        self.client = client
        # 资源数据文件池 字典
        self.file_list = file_list

        # 输入必填项：原始需求
        self.channel_id = channel_id  # 视频ID
        self.vs_info = vs_info

        # 自定义
        self.bucket = f'{vs_info.root_prefix}{channel_id}'

        # 自定义常量
        self.depth = 1  # 默认使用对象目录深度=1，即不建子目录
        self.start_date = "2023-01-01"  # 写入起始日期

        # 统计信息
        self.start_datetime = datetime.datetime.now()
        self.sum_count = 0
        self.elapsed_sum = 0

    @staticmethod
    def date_str_calc(start_date, date_step=1):
        """获取 date_step 天后的日期"""
        return arrow.get(start_date).shift(days=date_step).datetime.strftime("%Y-%m-%d")

    @staticmethod
    def _obj_prefix_calc(obj_prefix, depth, date_prefix=''):
        """
        拼接对象前缀、路径、日期前缀
        :param obj_prefix:
        :param depth:
        :param date_prefix:
        :return:
        """
        if date_prefix == "today":
            date_prefix = datetime.date.today().strftime("%Y-%m-%d") + '/'  # 按日期写入不同文件夹

        nested_prefix = ""
        for d in range(2, depth + 1):  # depth=2开始创建子文件夹，depth=1为日期文件夹
            nested_prefix += f'nested{d - 1}/'
        prefix = date_prefix + nested_prefix + obj_prefix
        return prefix

    def _obj_path_calc(self, idx, date_prefix=''):
        """
        依据idx序号计算对象 path，实际为：<bucket_name>/{obj_path}
        depth:子目录深度，depth=2开始创建子目录
        :param idx:
        :param date_prefix:按日期写不同文件夹
        :return:
        """
        obj_prefix = self._obj_prefix_calc(self.vs_info.file_prefix, self.depth, date_prefix)
        obj_path = obj_prefix + zfill(idx, width=self.vs_info.idx_width)
        return obj_path

    def obj_path_calc(self, idx):
        """
        基于对象idx计算该对象应该存储的桶和对象路径
        :param idx:
        :return:
        """
        # 计算对象路径
        date_step = idx // self.vs_info.obj_num_pc_pd  # 每日写N个对象，放在一个日期命名的文件夹
        current_date = self.date_str_calc(self.start_date, date_step)
        date_prefix = current_date + '/'
        obj_path = self._obj_path_calc(idx, date_prefix)
        return obj_path, current_date

    def disable_multipart_calc(self):
        """
        计算 disable_multipart
        :return:
        """
        if self.vs_info.multipart == 'enable':
            return False
        elif self.vs_info.multipart == 'disable':
            return True
        else:
            return random.choice([True, False])

    def statistics(self, elapsed):
        self.elapsed_sum += elapsed
        self.sum_count += 1
        datetime_now = datetime.datetime.now()
        elapsed_seconds = (datetime_now - self.start_datetime).seconds
        if elapsed_seconds >= 60:
            # 每分钟统计一次平均值
            ops = round(self.sum_count / elapsed_seconds, 3)
            elapsed_avg = round(self.elapsed_sum / self.sum_count, 3)
            logger.info("OPS={}, elapsed_avg={}".format(ops, elapsed_avg))
            InitDB().db_stat_insert(ops, elapsed_avg)
            self.start_datetime = datetime_now
            self.elapsed_sum = 0
            self.sum_count = 0

    async def worker(self, idx_put):
        """
        worker
        :param client:
        :param idx_put:
        :return:
        """
        # 删除旧数据
        idx_del = idx_put - self.vs_info.obj_num_pc
        if idx_del > 0:
            obj_path_del, _ = self.obj_path_calc(idx_del)
            await self.client.async_delete(self.bucket, obj_path_del)

        # 获取待上传的源文件
        src_file = random.choice(self.file_list)

        # 上传
        obj_path, current_date = self.obj_path_calc(idx_put)
        if self.vs_info.appendable:
            # 追加写模式  TODO
            elapsed = await S3API().append_write_async(
                self.client.endpoint, src_file.full_path, self.bucket, obj_path, 0, src_file.rb_data_list[0][1], self.vs_info.segments
            )
        else:
            disable_multipart = self.disable_multipart_calc()
            _, elapsed = await self.client.put_without_attr(
                src_file.full_path, self.bucket, obj_path, disable_multipart, src_file.tags
            )
        # 统计数据
        self.statistics(elapsed)

    async def producer(self, queue, interval=1.0):
        """
        main阶段 produce queue队列
        :param queue:
        :param interval:
        :return:
        """
        idx = self.vs_info.idx_put_start
        while True:
            await queue.put((idx, ))
            await asyncio.sleep(interval)  # 每N秒产生一个对象，数据预置阶段控制该时间快速预埋数据
            idx += 1
            self.vs_info.idx_put_start = idx

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

    async def stage_init(self):
        """
        批量创建特定桶
        :return:
        """
        logger.log("STAGE", f"初始化阶段：创建特定桶({self.bucket})、初始化数据库")
        self.client.mb(self.bucket, appendable=self.vs_info.appendable)
        await asyncio.sleep(0)

    async def stage_prepare(self):
        """
        批量创建特定对象
        :return:
        """
        # 调节多路视频启动时间，使IO均衡平滑到所有时间段  时间段为一个视频文件对象产生间隔
        sleep_avg = self.vs_info.time_interval / self.vs_info.channel_num
        sleep_avg = 1 if sleep_avg > 1 else sleep_avg  # 最大sleep 1秒
        await asyncio.sleep(round(sleep_avg * self.channel_id, 1))

        # idx超过预埋数量，跳过预埋阶段
        if self.vs_info.idx_put_start >= self.vs_info.obj_num_pc:
            await asyncio.sleep(0)
            logger.log("STAGE", "数据预埋阶段：跳过！")
            return

        interval = (self.vs_info.channel_num / self.vs_info.prepare_channel_num) * self.vs_info.time_interval  # 按比例调节间隔时间
        logger.log("STAGE", f"数据预埋阶段：bucket={self.bucket}, obj_num={self.vs_info.obj_num_pc}, interval={interval}")
        queue = asyncio.Queue()
        # 创建consumer
        consumers = [asyncio.ensure_future(self.consumer(queue)) for _ in range(self.vs_info.max_workers)]

        await self.producer(queue, interval=interval)
        await queue.join()
        for c in consumers:
            c.cancel()
        logger.log("STAGE", "预置对象完成！bucket={}, obj_num={}".format(self.bucket, self.vs_info.obj_num_pc))
        logger.log("STAGE", "销毁预置阶段consumers！len={}".format(len(consumers)))
        await asyncio.sleep(5)

    async def stage_main(self):
        """
        执行 生产->消费 queue
        :return:
        """
        logger.log("STAGE", f"写删均衡阶段：bucket={self.bucket}, obj_num={self.vs_info.obj_num_pc}, interval={self.vs_info.time_interval}")

        queue = asyncio.Queue()
        # 创建 max_workers 数量的consumer
        consumers = [asyncio.ensure_future(self.consumer(queue)) for _ in range(self.vs_info.max_workers)]

        await self.producer(queue, interval=self.vs_info.time_interval)
        await queue.join()
        for c in consumers:
            c.cancel()

    async def workflow(self):
        await self.stage_init()
        await self.stage_prepare()
        await self.stage_main()

    def run(self):
        """
        单路视频直接运行： 生产->消费 queue
        :return:
        """
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.stage_init())
        loop.run_until_complete(self.stage_prepare())

        asyncio.ensure_future(self.stage_main())
        loop.run_forever()
