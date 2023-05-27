#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:base_workflow
@time:2022/09/28
@email:tao.xu2008@outlook.com
@description: 工作流基类
"""
import random
import datetime
from collections import defaultdict
from typing import List, Text
from loguru import logger
import arrow

from config.models import ClientType
from config import DB_SQLITE3
from pkgs.sqlite_opt import Sqlite3Operation
from utils.util import zfill
from client.s3.mc import MClient
from client.s3.s3cmd import S3CmdClient
from client.s3.s3_client_interface import ClientInterface


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


class WorkflowBase(object):
    """
    工作流 - 基类，统一对象名、对象路径等算法
    """

    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num=1, obj_prefix='data', obj_num=10, multipart=False, local_path="",
            main_concurrent=1, prepare_concurrent=1, idx_width=1, idx_put_start=1, idx_del_start=1
    ):
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

    def bucket_name_calc(self, bucket_prefix, idx):
        """
        依据bucket前缀和对象idx序号计算bucket名称
        :param bucket_prefix:
        :param idx:
        :return:
        """
        bucket_idx = idx % self.bucket_num
        return '{}{}'.format(bucket_prefix, bucket_idx)

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


class InitDB(object):
    """初始化数据库"""
    def __init__(self):
        # 初始化数据库
        self.db_obj_table_name = "obj_info"
        self.db_stat_table_name = "stat_info"
        self.sqlite3_opt = Sqlite3Operation(db_path=DB_SQLITE3, show=False)

        # 统计信息
        self.start_datetime = datetime.datetime.now()
        self.sum_count = 0
        self.elapsed_sum = 0

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

    def db_obj_insert(self, str_idx, str_date, bucket, obj_path, md5='', put_rc=0, put_elapsed=0, queue_size=0):
        insert_sql = '''
        INSERT INTO {} ( idx, date, bucket, obj, md5, put_rc, put_elapsed, queue_size ) values (?, ?, ?, ?, ?, ?, ?, ?)
        '''.format(self.db_obj_table_name)
        data = [(str_idx, str_date, bucket, obj_path, md5, put_rc, put_elapsed, queue_size)]
        self.sqlite3_opt.insert_update_delete(insert_sql, data)

    def db_obj_update_delete_flag(self, str_idx):
        update_sql = '''UPDATE {} SET is_delete = true WHERE idx = ? '''.format(self.db_obj_table_name)
        data = [(str_idx,)]
        self.sqlite3_opt.insert_update_delete(update_sql, data)

    def db_obj_delete(self, str_idx):
        delete_sql = '''DELETE FROM {} WHERE idx = ?'''.format(self.db_obj_table_name)
        data = [(str_idx,)]
        self.sqlite3_opt.insert_update_delete(delete_sql, data)

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
