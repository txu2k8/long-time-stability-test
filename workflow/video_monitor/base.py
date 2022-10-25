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
from utils.util import get_local_files
from config.models import ClientType


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
            main_concurrent=1, prepare_concurrent=1, max_workers=1, idx_width=1, idx_put_start=0, idx_del_start=0,
            obj_num_per_day=1,
    ):
        super(BaseVideoMonitor, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num, obj_prefix, obj_num, multipart, local_path,
            main_concurrent, prepare_concurrent, max_workers, idx_width, idx_put_start, idx_del_start
        )
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
