#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:list
@time:2022/09/07
@email:tao.xu2008@outlook.com
@description:
"""
import os
from loguru import logger

from utils.util import get_md5_value, zfill
from stress.bucket import generate_bucket_name
from stress.put import PutObject


class ListObject(PutObject):
    """列表对象"""

    def __init__(
            self,
            tool_type, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num=1, depth=1, obj_prefix='', obj_num=1,
            concurrent=1, disable_multipart=False,
            duration=''
    ):
        super(ListObject, self).__init__(
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

        await client.list(bucket, obj_path)


