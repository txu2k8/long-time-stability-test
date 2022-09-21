#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:list
@time:2022/09/07
@email:tao.xu2008@outlook.com
@description:
"""
from core.stress.base_worker import BaseWorker


class ListObject(BaseWorker):
    """列表对象"""

    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num=1, depth=1, obj_prefix='', obj_num=1,
            concurrent=1, multipart=False, duration=0, cover=False, idx_start=0, idx_width=1
    ):
        super(ListObject, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num, depth, obj_prefix, obj_num,
            concurrent, multipart, duration, cover, idx_start, idx_width
        )
        pass

    async def worker(self, client, bucket, idx):
        """
        list查询指定对象
        :param client:
        :param bucket:
        :param idx:
        :return:
        """
        obj_path = self.obj_path_calc(idx)
        await client.list(bucket, obj_path)


