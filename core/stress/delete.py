#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:delete
@time:2022/09/07
@email:tao.xu2008@outlook.com
@description:
"""
from core.stress.base_worker import BaseWorker


class DeleteObject(BaseWorker):
    """删除对象"""

    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num=1, depth=1, obj_prefix='', obj_num=1,
            multipart=False, concurrent=1, prepare_concurrent=1,
            idx_width=1, idx_put_start=0, idx_del_start=0, duration=0, cover=False
    ):
        super(DeleteObject, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num, depth, obj_prefix, obj_num,
            multipart, concurrent, prepare_concurrent,
            idx_width, idx_put_start, idx_del_start,
            duration, cover
        )
        pass

    async def worker(self, client, bucket, idx):
        """
        删除指定对象
        :param client:
        :param bucket:
        :param idx:
        :return:
        """
        obj_path = self.obj_path_calc(idx)
        await client.delete(bucket, obj_path)
