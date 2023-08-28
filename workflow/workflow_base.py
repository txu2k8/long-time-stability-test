#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:base_workflow
@time:2022/09/28
@email:tao.xu2008@outlook.com
@description: 工作流基类
"""
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
    工作流 - 基类，统一对象/文件名、对象/文件路径等算法
    """

    def __init__(self, write_only=False, read_only=False, delete_only=False, delete_immediately=False):
        self.write_only = write_only
        self.read_only = read_only
        self.delete_only = delete_only
        self.delete_immediately = delete_immediately

        # 自定义常量
        self.depth = 1
        self.start_date = "2023-01-01"  # 写入起始日期

        # 统计信息
        self.start_datetime = datetime.datetime.now()
        self.sum_count = 0
        self.elapsed_sum = 0

    @staticmethod
    def calc_date_str(start_date, date_step=1):
        """获取 date_step 天后的日期"""
        return arrow.get(start_date).shift(days=date_step).datetime.strftime("%Y-%m-%d")

    @staticmethod
    def _calc_file_prefix(obj_prefix, depth, date_prefix='', channel_id=None):
        """
        拼接对象/文件前缀、路径、日期前缀
        :param obj_prefix:
        :param depth:
        :param date_prefix:
        :param channel_id:
        :return:
        """
        if date_prefix == "today":
            date_prefix = datetime.date.today().strftime("%Y-%m-%d") + '/'  # 按日期写入不同文件夹

        nested_prefix = ""
        for d in range(2, depth + 1):  # depth=2开始创建子文件夹，depth=1为日期文件夹
            nested_prefix += f'nested{d - 1}/'
        prefix = date_prefix + nested_prefix + obj_prefix
        if channel_id:
            prefix += f"-ch{channel_id}-"
        return prefix

    def calc_file_path_base(self, idx, depth=1, date_prefix='',
                            file_prefix="file", idx_width=11, file_type="data", channel_id=None):
        """
        依据idx序号计算对象 path，实际为：<root_path>/{obj_path}
        depth:子目录深度，depth=2开始创建子目录
        :param idx:
        :param depth:
        :param date_prefix:按日期写不同文件夹
        :param file_prefix
        :param idx_width
        :param file_type
        :param channel_id
        :return:
        """
        file_prefix = self._calc_file_prefix(file_prefix, depth, date_prefix, channel_id)
        file_path = file_prefix + zfill(idx, width=idx_width) + file_type
        return file_path

    def _calc_put_idx(self, idx):
        """
        计算 idx 文件/对象是否需要上传/写入
        :param idx:
        :return:
        """
        return -1 if self.read_only or self.delete_only else idx

    def _calc_get_idx(self, idx):
        """
        计算 idx 文件/对象是否需要下载/读取
        :param idx:
        :return:
        """
        return idx if self.read_only else -1

    def _calc_del_idx(self, idx, store_num=1):
        """
        计算 idx 文件/对象是否需要删除
        :param idx:
        :param store_num: 需要保存的文件数
        :return:
        """
        if self.write_only or self.read_only:
            idx_del = -1
        elif self.delete_immediately:
            idx_del = idx - 1
        elif self.delete_only:
            idx_del = idx
        else:
            idx_del = idx - store_num
        return idx_del

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
