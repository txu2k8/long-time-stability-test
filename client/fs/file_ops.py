#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author:TXU
@file:file_ops
@time:2023/5/27
@email:tao.xu2008@outlook.com
@description: 文件操作
"""
import os
import datetime
import aiofiles

from loguru import logger


class FileOps(object):
    """文件操作"""
    def __init__(self):
        pass

    def file_cp(self, src, dst):
        """
        复制指定文件
        :param src:
        :param dst:
        :return:
        """
        pass

    @staticmethod
    async def async_file_write(filepath, data, idx=1):
        """
        异步复制指定文件
        :param filepath:
        :param data:
        :param idx:
        :return:
        """
        rc = -1
        start = datetime.datetime.now()
        try:
            dir_name = os.path.dirname(filepath)
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)

            async with aiofiles.open(filepath, mode='wb') as f:
                await f.write(data)
                end = datetime.datetime.now()
                elapsed = (end - start).total_seconds()
                logger.success(f"文件写入成功!{filepath}, idx={idx}, 耗时：{round(elapsed, 3)} s")
                rc = 0
        except Exception as e:
            end = datetime.datetime.now()
            elapsed = (end - start).total_seconds()
            logger.error(f"文件写入失败!{filepath}, idx={idx}, 耗时：{round(elapsed, 3)} s, {e}")
        return rc, elapsed


if __name__ == '__main__':
    pass
