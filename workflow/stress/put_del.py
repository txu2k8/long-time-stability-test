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

from workflow.stress.base_worker import BaseWorker
from utils.util import get_local_files


class PutDeleteObject(BaseWorker):
    """写删均衡测试"""
    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num=1, depth=1, obj_prefix='', obj_num=1,
            multipart=False, concurrent=1, prepare_concurrent=1,
            idx_width=1, idx_put_start=0, idx_del_start=0, duration=0, cover=False
    ):
        super(PutDeleteObject, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num, depth, obj_prefix, obj_num,
            multipart, concurrent, prepare_concurrent,
            idx_width, idx_put_start, idx_del_start,
            duration, cover
        )
        # 准备源数据文件池 字典
        self.file_list = get_local_files(local_path)

    async def worker(self, client, bucket, idx):
        """
        步骤1：删除对象
        步骤2：上次同名对象
        :param client:
        :param bucket:
        :param idx:
        :return:
        """
        # 准备
        obj_path = self.obj_path_calc(idx)
        src_file = random.choice(self.file_list)
        disable_multipart = self.disable_multipart_calc()
        await client.delete(bucket, obj_path)
        await client.put(src_file.full_path, bucket, obj_path, disable_multipart, src_file.tags, src_file.attr)


if __name__ == '__main__':
    pass
