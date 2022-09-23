#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:put
@time:2022/09/07
@email:tao.xu2008@outlook.com
@description: 上传对象
"""
import random
import asyncio

from core.stress.base_worker import BaseWorker
from utils.util import get_local_files


class PutObject(BaseWorker):
    """上传对象"""
    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num=1, depth=1, obj_prefix='', obj_num=1,
            multipart=False, concurrent=1, prepare_concurrent=1,
            idx_start=0, idx_width=1, duration=0, cover=False
    ):
        super(PutObject, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num, depth, obj_prefix, obj_num,
            multipart, concurrent, prepare_concurrent,
            idx_start, idx_width,
            duration, cover
        )
        # 准备源数据文件池 字典
        self.file_list = get_local_files(local_path)

    def stage_init(self):
        # 开启debug日志
        # self.set_core_loglevel()

        # 准备桶
        client = random.choice(self.client_list)
        self.make_bucket_if_not_exist(client, self.bucket_prefix, self.bucket_num)

    async def worker(self, client, bucket, idx):
        """
        上传指定对象
        :param client:
        :param bucket:
        :param idx:
        :return:
        """
        obj_path = self.obj_path_calc(idx)
        src_file = random.choice(self.file_list)
        disable_multipart = self.disable_multipart_calc()
        await client.put(src_file.full_path, bucket, obj_path, disable_multipart, src_file.tags, src_file.attr)


if __name__ == '__main__':
    put = PutObject(
        'mc', '127.0.0.1:9000', 'minioadmin', 'minioadmin', False, 'play',
        'D:\\minio\\upload_data', 'bucket', 1, 2, '', 10,
        3, False, 30
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(put.run())
    loop.close()

