#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:video_workflow_one_channel.py
@time:2022/09/28
@email:tao.xu2008@outlook.com
@description: 视频监控场景测试 - 模拟一路视频数据流
"""
import random
import datetime
import asyncio
from abc import ABC
import arrow
from loguru import logger

from utils.util import get_local_files, zfill
from config.models import ClientType

from config import DB_SQLITE3
from pkgs.sqlite_opt import Sqlite3Operation
from workflow.workflow_base import init_clients
from workflow.workflow_interface import WorkflowInterface


class VideoWorkflowOneChannel(WorkflowInterface, ABC):
    """
    视频监控场景测试 - 基类（支持追加写）
    1、init阶段：新建桶，初始化数据库用于存储写入的对象路径
    2、Prepare阶段：预埋对象
    3、Main阶段：测试执行 上传、删除等
    """

    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            channel_id, bitstream, local_path, obj_num, obj_size=128, multipart=False,
            bucket_prefix='vc-', obj_prefix='data-', max_workers=2,

    ):
        self.client_types = client_types
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.tls = tls
        self.alias = alias

        # 输入必填项：原始需求
        self.channel_id = channel_id  # 视频ID
        self.bitstream = bitstream  # 码流（Mbps）
        self.local_path = local_path  # 本地文件的路径  dir
        self.obj_num = obj_num  # 一路视频需要保存对象数，由外部计算输入
        self.obj_size = obj_size  # 单个对象大小（MB）
        self.multipart = multipart  # 是否多段上传

        # 自定义
        self.bucket = f'{bucket_prefix}{channel_id}'
        self.obj_prefix = obj_prefix
        self.max_workers = max_workers

        # 自定义常量
        self.idx_width = 11  # 对象idx字符宽度，例如：3 --> 003
        self.depth = 1  # 默认使用对象目录深度=1，即不建子目录
        self.start_date = "2023-01-01"  # 写入起始日期

        # 准备源数据文件池 字典
        self.file_list = get_local_files(local_path)

        # 统计信息
        self.start_datetime = datetime.datetime.now()
        self.sum_count = 0
        self.elapsed_sum = 0

        # 初始化数据库
        self.db_obj_table_name = "obj_info"
        self.db_stat_table_name = "stat_info"
        self.sqlite3_opt = Sqlite3Operation(db_path=DB_SQLITE3, show=False)

        # 初始化客户端
        self.clients_info = init_clients(
            self.client_types, self.endpoint, self.access_key, self.secret_key, self.tls, self.alias)
        self.client_list = list(self.clients_info.values())
        self.client = self.clients_info[ClientType.MC.value]

        # 待计算数据
        self.time_interval = 256  # 产生一个对象的时间间隔
        self.obj_num_per_day = 10
        self.idx_start = 0
        # 计算
        self.io_calc()
        logger.info(self.time_interval)

    def io_calc(self):
        """
        数据模型计算
        :return:
        """
        # 带宽（MB）
        bandwidth = self.bitstream / 8

        # 每写入一个对象间隔时间（秒）
        self.time_interval = self.obj_size / bandwidth

        # 每天需要写入的数据量（MB） = 带宽/s * 1天
        size_pd = bandwidth * 60 * 60 * 24

        # 每天需要写入的对象数 = 每日数据量 / 对象大小
        self.obj_num_per_day = int(size_pd / self.obj_size)

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
        obj_prefix = self._obj_prefix_calc(self.obj_prefix, self.depth, date_prefix)
        obj_path = obj_prefix + zfill(idx, width=self.idx_width)
        return obj_path

    def obj_path_calc(self, idx):
        """
        基于对象idx计算该对象应该存储的桶和对象路径
        :param idx:
        :return:
        """
        # 计算对象路径
        date_step = idx // self.obj_num_per_day  # 每日写N个对象，放在一个日期命名的文件夹
        current_date = self.date_str_calc(self.start_date, date_step)
        date_prefix = current_date + '/'
        obj_path = self._obj_path_calc(idx, date_prefix)
        return obj_path, current_date

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

    def db_init(self):
        """
        初始化数据库：建表
        :return:
        """
        logger.log("STAGE", "init->初始化对象数据表，table={}".format(self.db_obj_table_name))
        sql_create_obj_table = '''CREATE TABLE IF NOT EXISTS `{}` (
                                                      `id` INTEGER PRIMARY KEY,
                                                      `idx` varchar(20) NOT NULL,
                                                      `date` varchar(20) NOT NULL,
                                                      `bucket` varchar(100) NOT NULL,
                                                      `obj` varchar(500) NOT NULL,
                                                      `md5` varchar(100) DEFAULT NULL,
                                                      `put_rc` int(11) DEFAULT NULL,
                                                      `put_elapsed` int(11) DEFAULT NULL,
                                                      `del_rc` int(11) DEFAULT NULL,
                                                      `is_delete` BOOL DEFAULT FALSE,
                                                      `queue_size` int(11) DEFAULT NULL
                                                    )
                                                    '''.format(self.db_obj_table_name)
        self.sqlite3_opt.create_table(sql_create_obj_table)

        logger.log("STAGE", "init->初始化统计数据表，table={}".format(self.db_stat_table_name))
        sql_create_stat_table = '''CREATE TABLE IF NOT EXISTS `{}` (
                                                              `id` INTEGER PRIMARY KEY,
                                                              `ops` int(11) DEFAULT NULL,
                                                              `elapsed_avg` int(11) DEFAULT NULL,
                                                              `queue_size_avg` int(11) DEFAULT NULL,
                                                              `datetime` DATETIME
                                                            )
                                                            '''.format(self.db_stat_table_name)
        self.sqlite3_opt.create_table(sql_create_stat_table)

    def db_stat_insert(self, ops, elapsed_avg, queue_size_avg=0):
        insert_sql = '''
                INSERT INTO {} ( ops, elapsed_avg, queue_size_avg, datetime ) values (?, ?, ?, ?)
                '''.format(self.db_stat_table_name)
        date_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data = [(ops, elapsed_avg, queue_size_avg, date_time)]
        self.sqlite3_opt.insert_update_delete(insert_sql, data)

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
            self.db_stat_insert(ops, elapsed_avg)
            self.start_datetime = datetime_now
            self.elapsed_sum = 0
            self.sum_count = 0

    async def worker(self, client, idx_put):
        """
        worker
        :param client:
        :param idx_put:
        :return:
        """
        # 删除旧数据
        idx_del = idx_put - self.obj_num
        if idx_del > 0:
            obj_path_del, _ = self.obj_path_calc(idx_del)
            await client.async_delete(self.bucket, obj_path_del)

        # 获取待上传的源文件
        src_file = random.choice(self.file_list)
        disable_multipart = self.disable_multipart_calc()
        # 上传
        obj_path, current_date = self.obj_path_calc(idx_put)
        rc, elapsed = await client.put_without_attr(src_file.full_path, self.bucket, obj_path, disable_multipart,
                                                    src_file.tags)
        # 统计数据
        self.statistics(elapsed)

    async def producer(self, queue):
        """
        main阶段 produce queue队列
        :param queue:
        :return:
        """
        client = self.client_list[0]
        idx = self.idx_start
        while True:
            await queue.put((client, idx))
            await asyncio.sleep(self.time_interval)  # 每N秒产生一个对象，数据预置阶段控制该时间快速预埋数据
            idx += 1

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

    def stage_init(self):
        """
        批量创建特定桶
        :return:
        """
        logger.log("STAGE", f"初始化阶段：创建特定桶({self.bucket})、初始化数据库")

        # 开启debug日志
        # self.set_core_loglevel()

        # 准备桶
        self.client.mb(self.bucket)

        # 初始化数据库
        self.db_init()

        logger.log("STAGE", "初始化阶段完成！")

    async def stage_prepare(self):
        """
        批量创建特定对象
        :return:
        """
        if self.idx_start >= self.obj_num:
            await asyncio.sleep(0)
            logger.log("STAGE", "数据预埋阶段：bucket={}, obj_num={}，已有数据，跳过预置！".format(self.bucket, self.obj_num))
            return

        logger.log("STAGE", "数据预埋阶段：bucket={}, obj_num={}".format(self.bucket, self.obj_num))
        queue = asyncio.Queue()
        # 创建100倍 concurrent 数量的consumer
        consumers = [asyncio.ensure_future(self.consumer(queue)) for _ in range(self.max_workers)]

        await self.producer(queue)
        await queue.join()
        for c in consumers:
            c.cancel()
        logger.log("STAGE", "预置对象完成！bucket={}, obj_num={}".format(self.bucket, self.obj_num))
        logger.log("STAGE", "销毁预置阶段consumers！len={}".format(len(consumers)))
        await asyncio.sleep(5)

    async def stage_main(self):
        """
        执行 生产->消费 queue
        :return:
        """
        logger.log("STAGE", "写删均衡阶段：bucket={}, obj_num={}, ".format(self.bucket, self.obj_num))

        queue = asyncio.Queue()
        # 创建 max_workers 数量的consumer
        consumers = [asyncio.ensure_future(self.consumer(queue)) for _ in range(self.max_workers)]

        await self.producer(queue)
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
