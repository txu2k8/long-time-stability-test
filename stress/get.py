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
from stress.base_worker import BaseWorker


class GetObject(BaseWorker):
    """下载对象"""

    def __init__(
            self,
            tool_type, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num=1, depth=1, obj_prefix='', obj_num=1,
            concurrent=1, multipart=False, duration=0, cover=False
    ):
        super(GetObject, self).__init__(
            tool_type, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num, depth, obj_prefix, obj_num,
            concurrent, multipart, duration, cover
        )
        pass

    async def worker(self, client, bucket, idx):
        """
        下载对象并比较MD5值
        :param client:
        :param bucket:
        :param idx:
        :return:
        """
        # 准备
        obj_path = self.obj_path_calc(idx)
        local_file_path = os.path.join(self.local_path, obj_path.replace('/', '_'))
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


