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

from workflow.stress.base import BaseStress
from utils.util import get_local_files


class PutObject(BaseStress):
    """上传对象"""
    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num=1, obj_prefix='', obj_num=10, multipart=False, local_path="",
            main_concurrent=1, prepare_concurrent=1, idx_width=1, idx_put_start=0, idx_del_start=0,
            depth=1, duration=0, cover=False,
    ):
        super(PutObject, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num, obj_prefix, obj_num, multipart, local_path,
            main_concurrent, prepare_concurrent, idx_width, idx_put_start, idx_del_start,
            depth, duration, cover
        )
        # 准备源数据文件池 字典
        self.file_list = get_local_files(local_path)

    def stage_init(self):
        # 开启debug日志
        # self.set_core_loglevel()

        # 准备桶
        client = random.choice(self.client_list)
        self.make_bucket_if_not_exist(client, self.bucket_prefix, self.bucket_num)

    async def worker(self, client, idx):
        """
        上传指定对象
        :param client:
        :param idx:
        :return:
        """
        bucket, obj_path = self.bucket_obj_path_calc(idx)
        src_file = random.choice(self.file_list)
        disable_multipart = self.disable_multipart_calc()
        await client.put(src_file.full_path, bucket, obj_path, disable_multipart, src_file.tags, src_file.attr)


if __name__ == '__main__':
    pass

