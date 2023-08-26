#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author:TXU
@file:worker
@time:2023/8/26
@email:tao.xu2008@outlook.com
@description: TODO
"""
import datetime
from loguru import logger
import arrow

from config.models import ClientType
from utils.util import zfill
from client.s3.mc import MClient
from workflow.stress.stress_workflow_base import StressWorkflowBase
from utils.util import get_local_files


class StressS3Worker(StressWorkflowBase):
    """上传对象"""
    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num=1, obj_prefix='', obj_num=10, multipart=False, local_path="",
            main_concurrent=1, prepare_concurrent=1, idx_width=1, idx_put_start=0, idx_del_start=0,
            depth=1, duration=0, cover=False,
    ):
        super(StressS3Worker, self).__init__(
            bucket_prefix, bucket_num, obj_prefix, obj_num, multipart, local_path,
            main_concurrent, prepare_concurrent, idx_width, idx_put_start, idx_del_start,
            depth, duration, cover
        )
        # 准备源数据文件池 字典
        self.file_list = get_local_files(local_path)

        self.client_types = client_types
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.tls = tls
        self.alias = alias

        self.bucket_prefix = bucket_prefix
        self.bucket_num = bucket_num
        self.obj_prefix = obj_prefix
        self.obj_num = obj_num
        self.multipart = multipart
        self.local_path = local_path

        self.main_concurrent = main_concurrent
        self.prepare_concurrent = prepare_concurrent

        self.idx_width = idx_width
        self.idx_put_start = idx_put_start
        self.idx_del_start = idx_del_start
        self.idx_main_start = self.obj_num + 1 if self.idx_put_start <= self.obj_num else self.idx_put_start  # main阶段idx起始
        self.idx_put_current = idx_put_start  # put操作完成的进度idx
        self.depth = 1  # 默认使用对象目录深度=1，即不建子目录

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

    def obj_path_calc(self, idx, date_prefix=''):
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

    def bucket_obj_path_calc(self, idx):
        """
        基于对象idx计算该对象应该存储的桶和对象路径
        :param idx:
        :return:
        """
        # 计算对象应该存储的桶名称
        bucket = self.bucket_name_calc(self.bucket_prefix, idx)
        # 计算对象路径
        obj_path = self.obj_path_calc(idx, date_prefix="")
        return bucket, obj_path

    def set_core_loglevel(self, loglevel="debug"):
        """
        MC命令设置core日志级别
        :param loglevel:
        :return:
        """
        client = self.clients_info[ClientType.MC.value] or MClient(
            self.endpoint, self.access_key, self.secret_key, self.tls, self.alias)
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


if __name__ == '__main__':
    pass
