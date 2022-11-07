#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:get
@time:2022/09/07
@email:tao.xu2008@outlook.com
@description:
"""
import os
from loguru import logger

from utils.util import get_md5_value
from workflow.stress.stress_workflow import BaseStress


class GetObject(BaseStress):
    """下载对象"""

    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num=1, obj_prefix='', obj_num=10, multipart=False, local_path="",
            main_concurrent=1, prepare_concurrent=1, idx_width=1, idx_put_start=0, idx_del_start=0,
            depth=1, duration=0, cover=False,
    ):
        super(GetObject, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num, obj_prefix, obj_num, multipart, local_path,
            main_concurrent, prepare_concurrent, idx_width, idx_put_start, idx_del_start,
            depth, duration, cover
        )
        pass

    async def worker(self, client, idx):
        """
        下载对象并比较MD5值
        :param client:
        :param idx:
        :return:
        """
        # 准备
        bucket, obj_path = self.bucket_obj_path_calc(idx)
        local_file_path = os.path.join(self.local_path, '{}_{}'.format(bucket, obj_path.replace('/', '_')))
        disable_multipart = self.disable_multipart_calc()
        rc, expect_md5 = await client.get_obj_md5(bucket, obj_path)
        await client.get(bucket, obj_path, local_file_path, disable_multipart)
        if expect_md5:
            download_md5 = get_md5_value(local_file_path)
            if download_md5 != expect_md5:
                logger.error("MD5不一致：\n本地：{}，MD5={}\n对象：{}/{}，MD5={}".format(
                    local_file_path, download_md5, bucket, obj_path, expect_md5))
            else:
                logger.info("MD5校验通过：{}/{}，MD5={}".format(bucket, obj_path, expect_md5))
                # MD5验证通过后，删除下载到本地的文件
                os.remove(local_file_path)
        else:
            # 未做MD5值校验，删除本地文件
            os.remove(local_file_path)


