#!/usr/bin/python
# -*- coding:utf-8 _*- 
"""
@author:TXU
@file:put_del
@time:2022/09/07
@email:tao.xu2008@outlook.com
@description: 写删均衡测试
"""
import random

from stress.bucket import generate_bucket_name
from stress.put import PutObject


class PutDeleteObject(PutObject):
    """写删均衡测试"""
    def __init__(
            self,
            tool_type, endpoint, access_key, secret_key, tls, alias,
            src_file_path, bucket_prefix, bucket_num=1, depth=1, obj_prefix='', obj_num=1,
            concurrent=1, disable_multipart=False,
            duration=''
    ):
        super(PutDeleteObject, self).__init__(
            tool_type, endpoint, access_key, secret_key, tls, alias,
            src_file_path, bucket_prefix, bucket_num, depth, obj_prefix, obj_num,
            concurrent, disable_multipart,
            duration
        )
        pass

    async def worker(self, idx, client):
        # 准备
        bucket_idx = idx % self.bucket_num
        bucket = generate_bucket_name(self.bucket_prefix, bucket_idx)
        src_obj = random.choice(self.file_list)
        dst_path = f"{self.obj_prefix}{str(idx)}"

        await client.delete(bucket, dst_path)
        await client.put(src_obj.full_path, bucket, dst_path, self.disable_multipart, tags=src_obj.tags)


if __name__ == '__main__':
    pass
