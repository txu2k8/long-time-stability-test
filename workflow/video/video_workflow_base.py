#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author:TXU
@file:video_workflow_base
@time:2023/5/27
@email:tao.xu2008@outlook.com
@description: 
"""
import os
import datetime
import asyncio
from abc import ABC
import arrow
from loguru import logger

from utils.util import zfill
from config.models import FileInfo
from workflow.workflow_base import InitDB
from workflow.workflow_interface import WorkflowInterface
from workflow.video.calculate import VSInfo


class VideoWorkflowBase(WorkflowInterface, ABC):
    """
    视频监控场景测试 - 基类
    1、init阶段：新建桶，初始化数据库用于存储写入的对象路径
    2、Prepare阶段：预埋数据
    3、Main阶段：测试执行 写入+删除等
    """

    def __init__(
            self, file_info: FileInfo, channel_id, vs_info: VSInfo,
            skip_stage_init=False, write_only=False, delete_immediately=False,
            single_root=False, single_root_name="video",
    ):
        # 源数据 字典
        self.file_info = file_info
        # 输入必填项：原始需求
        self.channel_id = channel_id  # 视频 channel ID
        self.vs_info = vs_info

        # 自定义
        self.skip_stage_init = skip_stage_init  # 跳过init阶段
        self.write_only = write_only  # 只写，不删除
        self.delete_immediately = delete_immediately  # 立即删除，删除前一个文件/对象
        self.single_root = single_root  # 单桶/单根目录模式

        self.channel_name = f"{self.vs_info.root_prefix}{channel_id}"  # 单桶/单根目录模式时，视频写入目录名
        self.root_dir_name = single_root_name if single_root else self.channel_name  # 多桶/目录模式，每路视频写入指定桶/目录

        # 自定义常量
        self.depth = 1  # 默认使用对象目录深度=1，即不建子目录
        self.start_date = "2023-01-01"  # 写入起始日期

        # 统计信息
        self.start_datetime = datetime.datetime.now()
        self.sum_count = 0
        self.elapsed_sum = 0

        self.res_count = 0

    @staticmethod
    def date_str_calc(start_date, date_step=1):
        """获取 date_step 天后的日期"""
        return arrow.get(start_date).shift(days=date_step).datetime.strftime("%Y-%m-%d")

    @staticmethod
    def _file_prefix_calc(obj_prefix, depth, date_prefix=''):
        """
        拼接对象/文件前缀、路径、日期前缀
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

    def _file_path_calc(self, idx, date_prefix=''):
        """
        依据idx序号计算对象 path，实际为：<root_path>/{obj_path}
        depth:子目录深度，depth=2开始创建子目录
        :param idx:
        :param date_prefix:按日期写不同文件夹
        :return:
        """
        file_prefix = self._file_prefix_calc(self.vs_info.file_prefix, self.depth, date_prefix)
        file_path = file_prefix + zfill(idx, width=self.vs_info.idx_width)
        if self.single_root:
            file_path = f"{self.channel_name}/{file_path}"
        return file_path

    def file_path_calc(self, idx):
        """
        基于对象idx计算该对象应该存储的桶和对象路径
        :param idx:
        :return:
        """
        # 计算对象路径
        date_step = idx // self.vs_info.obj_num_pc_pd  # 每日写N个对象，放在一个日期命名的文件夹
        current_date = self.date_str_calc(self.start_date, date_step)
        date_prefix = current_date + '/'
        file_path = self._file_path_calc(idx, date_prefix)
        return file_path, current_date

    def statistics(self, elapsed):
        """
        统计时延变化趋势信息
        :param elapsed:
        :return:
        """
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

    async def worker(self, *args, **kwargs):
        """
        worker：具体业务自定义
        :param args:
        :param kwargs:
        :return:
        """
        await asyncio.sleep(0)

    async def producer(self, queue, interval=1.0):
        """
        main阶段 produce queue队列
        :param queue:
        :param interval: 每个文件产生的时间
        :return:
        """
        s_interval = interval / self.vs_info.segments  # 每个分片文件需要等待的时间，单位：秒
        idx = self.vs_info.idx_put_start
        while True:
            for segment in range(0, self.vs_info.segments):
                logger.debug(f"Produce:channel={self.channel_id},idx={idx}->{segment}. sleep={round(s_interval,2)}, qsize={queue.qsize()}")
                await queue.put((idx, segment))
                await asyncio.sleep(s_interval)  # 每N秒产生一个对象，数据预置阶段控制该时间快速预埋数据
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
            self.res_count += 1
            await self.worker(*item)
            self.res_count -= 1
            if self.res_count >= 1:
                logger.warning(f"请求未返回计数={self.res_count}, Channel={self.channel_id}")
            queue.task_done()

    async def stage_init(self):
        """
        初始化阶段，业务自定义，如：批量创建特定桶、创建根目录
        :return:
        """
        await asyncio.sleep(0)

    async def stage_prepare(self):
        """
        数据预埋阶段，业务自定义
        :return:
        """
        await asyncio.sleep(0)

    async def stage_main(self):
        """
        执行 生产->消费 queue
        :return:
        """
        # 调节多路视频启动时间，使IO均衡平滑到所有时间段，时间段为一个视频文件产生的间隔，例如：4Mbps码流产生128MB文件需要256秒
        avg_sleep = self.vs_info.time_interval / self.vs_info.channel_num  # 平均每路视频睡眠时间
        curr_sleep = round(avg_sleep*self.channel_id, 1)  # 当前 channel 需要睡眠时间
        await asyncio.sleep(curr_sleep)

        if self.vs_info.idx_put_start < self.vs_info.obj_num_pc:
            # 数据预埋阶段，按比例调节间隔时间
            interval = (self.vs_info.channel_num / self.vs_info.prepare_channel_num) * self.vs_info.time_interval
            logger.log("STAGE", f"数据预埋阶段：Channel={self.channel_id}, obj_num={self.vs_info.obj_num_pc}, interval={interval},pid={os.getpid()}")
        else:
            interval = self.vs_info.time_interval
            logger.log("STAGE", f"写删均衡阶段：Channel={self.channel_id}, obj_num={self.vs_info.obj_num_pc}, interval={interval},pid={os.getpid()}")

        queue = asyncio.Queue()
        # 创建 max_workers 数量的consumer
        consumers = [asyncio.ensure_future(self.consumer(queue)) for _ in range(self.vs_info.max_workers)]

        await self.producer(queue, interval)
        await queue.join()

        for c in consumers:
            c.cancel()

    async def run(self):
        """
        单路视频模拟业务执行入口
        :return:
        """
        if not self.skip_stage_init:
            await self.stage_init()
        # await self.stage_prepare()
        await self.stage_main()


if __name__ == '__main__':
    pass
