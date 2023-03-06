#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:list
@time:2022/09/07
@email:tao.xu2008@outlook.com
@description:
"""
from workflow.stress.stress_workflow import BaseStress


class ListObject(BaseStress):
    """列表对象"""

    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num=1, obj_prefix='', obj_num=10, multipart=False, local_path="",
            main_concurrent=1, prepare_concurrent=1, idx_width=1, idx_put_start=0, idx_del_start=0,
            depth=1, duration=0, cover=False,
    ):
        super(ListObject, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num, obj_prefix, obj_num, multipart, local_path,
            main_concurrent, prepare_concurrent, idx_width, idx_put_start, idx_del_start,
            depth, duration, cover
        )
        pass

    def worker(self, client, idx):
        """
        list查询指定对象
        :param client:
        :param idx:
        :return:
        """
        bucket, obj_path = self.bucket_obj_path_calc(idx)
        client.list(bucket, obj_path)

    def run(self):
        # self.stage_init()
        # self.stage_prepare()
        self.stage_main()
