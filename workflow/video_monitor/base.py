#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:video_1
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

from loguru import logger

from utils.util import get_local_files
from config.models import ClientType
from client.mc import MClient

from workflow.base_workflow import BaseWorkflow


class BaseVideoMonitor(BaseWorkflow):
    """
    视频监控场景测试 - 基类 - 写删相同协程中串行处理，多对象并行处理（不读取数据库）
    1、init阶段：新建10桶，初始化数据库用于存储写入的对象路径
    2、Prepare阶段：预埋580万个128MB对象，平均分配到10桶中，预埋数据并行数=30
    3、Main阶段：写删均衡测试
    """

    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num=1, obj_prefix='', obj_num=10, multipart=False, local_path="",
            concurrent=1, prepare_concurrent=1, idx_width=1, idx_put_start=0, idx_del_start=0,
            obj_num_per_day=1,
    ):
        super(BaseVideoMonitor, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num, obj_prefix, obj_num, multipart, local_path,
            concurrent, prepare_concurrent, idx_width, idx_put_start, idx_del_start
        )
        self.obj_num_per_day = obj_num_per_day

        # 准备源数据文件池 字典
        self.file_list = get_local_files(local_path)
        # 写入起始日期
        self.start_date = "2022-09-20"
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

    async def worker(self, client: MClient, idx_put, idx_del=-1):
        """
        上传指定对象
        :param client:
        :param idx_put:
        :param idx_del:
        :return:
        """
        bucket, obj_path, current_date = self.bucket_obj_path_calc(idx_put)
        # 获取待上传的源文件
        src_file = random.choice(self.file_list)
        disable_multipart = self.disable_multipart_calc()
        # 上传
        await client.put_without_attr(src_file.full_path, bucket, obj_path, disable_multipart, src_file.tags)
        # 写入结果到数据库
        self.db_insert(str(idx_put), current_date, bucket, obj_path)

        # 删除镀锡
        if idx_del > 0:
            bucket_del, obj_path_del, _ = self.bucket_obj_path_calc(idx_del)
            await client.delete(bucket_del, obj_path_del)

    async def producer_main(self, queue):
        """
        prepare阶段 produce queue队列
        :param queue:
        :return:
        """
        logger.info("Produce PREPARE bucket={}, concurrent={}, ".format(self.bucket_num, self.prepare_concurrent))
        client = self.client_list[0]
        idx_put = self.idx_main_start
        idx_del = self.idx_del_start if self.idx_del_start > 0 else -1
        while True:
            logger.debug("put:{} , del:{}".format(idx_put, idx_del))
            if self.idx_put_current >= self.obj_num:
                await queue.put((client, idx_put, idx_del))  # 写+删
                idx_del += 1
            else:
                await queue.put((client, idx_put, -1))  # 写
            self.idx_put_current = idx_put
            if idx_put % self.prepare_concurrent == 0:
                await asyncio.sleep(1)  # 每秒生产 {prepare_concurrent} 个待处理项
            idx_put += 1
