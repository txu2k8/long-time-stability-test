#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:delete
@time:2022/09/07
@email:tao.xu2008@outlook.com
@description:
"""
from utils.util import zfill
from stress.bucket import generate_bucket_name
from stress.put import PutObject


class DeleteObject(PutObject):
    """删除对象"""

    def __init__(
            self,
            tool_type, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num=1, depth=1, obj_prefix='', obj_num=1,
            concurrent=1, disable_multipart=False,
            duration=''
    ):
        super(DeleteObject, self).__init__(
            tool_type, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num, depth, obj_prefix, obj_num,
            concurrent, disable_multipart,
            duration
        )
        pass

    async def worker(self, idx, client):
        # 准备
        bucket_idx = idx % self.bucket_num
        bucket = generate_bucket_name(self.bucket_prefix, bucket_idx)
        obj_path = self.obj_prefix + zfill(idx)

        await client.delete(bucket, obj_path)
