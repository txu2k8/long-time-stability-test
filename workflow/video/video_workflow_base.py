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
from workflow.workflow_base import WorkflowBase, InitDB
from workflow.workflow_interface import WorkflowInterface
from workflow.video.calculate import VSInfo


class VideoWorkflowBase(WorkflowBase, WorkflowInterface, ABC):
    """
    视频监控场景测试 - 基类
    1、init阶段：新建桶，初始化数据库用于存储写入的对象路径
    2、Prepare阶段：预埋数据
    3、Main阶段：测试执行 写入+删除等
    """

    def __init__(
            self, file_info: FileInfo, channel_id, vs_info: VSInfo,
            skip_stage_init=False, write_only=False, delete_immediately=False,
            single_root=False, single_root_name="video", duration=0
    ):
        super(VideoWorkflowBase, self).__init__()
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
        self.duration = duration  # 持续运行时间，0-代表永久

        self.channel_name = f"{self.vs_info.root_prefix}{channel_id}"  # 单桶/单根目录模式时，视频写入目录名
        self.root_dir_name = single_root_name if single_root else self.channel_name  # 多桶/目录模式，每路视频写入指定桶/目录

        # 自定义常量
        self.depth = 1  # 默认使用对象目录深度=1，即不建子目录
        self.start_date = "2023-01-01"  # 写入起始日期

        # 统计信息
        self.start_datetime = datetime.datetime.now()
        self.sum_count = 0
        self.elapsed_sum = 0

        self.req_count = 0
        self.res_count = 0

    def calc_file_path(self, idx):
        """
        基于对象idx计算该对象应该存储的桶和对象路径
        :param idx:
        :return:
        """
        # 计算对象路径
        date_step = idx // self.vs_info.obj_num_pc_pd  # 每日写N个对象，放在一个日期命名的文件夹
        current_date = self.calc_date_str(self.start_date, date_step)
        date_prefix = current_date + '/'
        file_path = self.calc_file_path_base(
            idx, self.depth, date_prefix, self.vs_info.file_prefix, self.vs_info.idx_width,
            self.file_info.file_type, self.channel_id
        )
        if self.single_root:
            file_path = f"{self.channel_name}/{file_path}"
        return file_path, current_date

    async def put_worker(self, *args, **kwargs):
        """
        PUT worker
        :param args:
        :param kwargs:
        :return:
        """
        pass

    async def get_worker(self, *args, **kwargs):
        """
        GET worker
        :param args:
        :param kwargs:
        :return:
        """
        pass

    async def del_worker(self, *args, **kwargs):
        """
        DELETE worker
        :param args:
        :param kwargs:
        :return:
        """
        pass

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

            if 0 < self.duration < (datetime.datetime.now() - self.start_datetime).total_seconds():
                logger.warning(f"停止生产！req={self.req_count},res={self.res_count}, Channel={self.channel_id}")
                break

    async def consumer(self, queue):
        """
        consume queue队列，指定队列中全部被消费
        :param queue:
        :return:
        """
        while True:
            item = await queue.get()
            self.req_count += 1
            await self.worker(*item)
            self.res_count += 1
            if self.res_count != self.req_count:
                logger.warning(f"请求未返回!req={self.req_count},res={self.res_count} Channel={self.channel_id}")
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
