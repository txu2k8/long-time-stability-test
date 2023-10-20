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
from client.base_client import BaseClient

# --- OS constants
POSIX = os.name == "posix"
WINDOWS = os.name == "nt"


class FileOps(BaseClient):
    """文件操作"""
    def __init__(self):
        super(FileOps, self).__init__()
        pass

    def file_cp(self, src_path, dst_path):
        """
        复制指定文件
        :param src_path:
        :param dst_path:
        :return:
        """
        rc = -1
        msg = f"{src_path}->{dst_path}"
        start = datetime.datetime.now()
        try:
            dir_name = os.path.dirname(dst_path)
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
            logger.success(f"文件写入开始！{msg}")

            rc, elapsed, output = self._exec(f"cp {src_path} {dst_path}")
            if rc == 0:
                logger.success(f"文件写入成功!{msg}, 耗时：{round(elapsed, 3)} s")
            else:
                logger.error(f"文件写入失败!{msg}, 耗时：{round(elapsed, 3)} s")
        except Exception as e:
            end = datetime.datetime.now()
            elapsed = (end - start).total_seconds()
            logger.error(f"文件写入失败!{msg}, 耗时：{round(elapsed, 3)} s, {e}")
        return rc, elapsed

    @staticmethod
    def file_write(filepath, data, appendable=False, segment_idx=0, segment_total=1, src_path=''):
        """
        open-》write方式 写入指定文件
        :param filepath:
        :param data:
        :param appendable:
        :param segment_idx:
        :param segment_total:
        :param src_path:
        :return:
        """
        rc = -1
        mode = 'ab' if appendable else 'wb'
        msg = f"{src_path}->{filepath}, idx={segment_idx}/{segment_total - 1}"
        start = datetime.datetime.now()
        try:
            dir_name = os.path.dirname(filepath)
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
            logger.success(f"文件写入开始！cp {msg}")
            with open(filepath, mode=mode) as f:
                f.write(data)
                f.flush()
                end = datetime.datetime.now()
                elapsed = (end - start).total_seconds()
                rc = 0
            logger.success(f"文件写入成功!{msg}, 耗时：{round(elapsed, 3)} s")
        except Exception as e:
            end = datetime.datetime.now()
            elapsed = (end - start).total_seconds()
            logger.error(f"文件写入失败!{msg}, 耗时：{round(elapsed, 3)} s, {e}")
        return rc, elapsed

    def file_delete(self, filepath):
        """
        异步删除指定文件
        :param filepath:
        :return:
        """
        rc, elapsed = 0, 0
        if POSIX:
            rc, elapsed, _ = self._exec(f"rm -rf {filepath}")
            if rc == 0:
                logger.success(f"删除成功！{filepath}")
            else:
                logger.success(f"删除失败！{filepath}")
        else:
            try:
                os.remove(filepath)
            except Exception as e:
                rc = -1
                logger.error(f"删除失败！{filepath}, {e}")
        return rc, elapsed

    # 异步函数
    @staticmethod
    async def async_file_write(filepath, data, appendable=False, segment_idx=0, segment_total=1, src_path=''):
        """
        异步写入指定文件
        :param filepath:
        :param data:
        :param appendable:
        :param segment_idx:
        :param segment_total:
        :param src_path:
        :return:
        """
        rc = -1
        mode = 'ab' if appendable else 'wb'
        msg = f"{src_path}->{filepath}, idx={segment_idx}/{segment_total - 1}"
        start = datetime.datetime.now()
        try:
            dir_name = os.path.dirname(filepath)
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
            logger.success(f"文件写入开始！cp {msg}")
            async with aiofiles.open(filepath, mode=mode) as f:
                await f.write(data)
                await f.flush()
                end = datetime.datetime.now()
                elapsed = (end - start).total_seconds()
                rc = 0
            logger.success(f"文件写入成功!{msg}, 耗时：{round(elapsed, 3)} s")
        except Exception as e:
            end = datetime.datetime.now()
            elapsed = (end - start).total_seconds()
            logger.error(f"文件写入失败!{msg}, 耗时：{round(elapsed, 3)} s, {e}")
        return rc, elapsed

    async def async_file_cp(self, src_path, dst_path, segment_idx=0, segment_total=1):
        """
        异步删除指定文件
        :param src_path:
        :param dst_path:
        :param segment_idx:
        :param segment_total:
        :return:
        """
        rc = -1
        msg = f"{src_path}->{dst_path}, idx={segment_idx}/{segment_total - 1}"
        start = datetime.datetime.now()
        try:
            dir_name = os.path.dirname(dst_path)
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
            logger.success(f"文件写入开始！{msg}")

            rc, elapsed, _, _ = await self._async_exec(f"cp {src_path} {dst_path}")
            if rc == 0:
                logger.success(f"文件写入成功!{msg}, 耗时：{round(elapsed, 3)} s")
            else:
                logger.error(f"文件写入失败!{msg}, 耗时：{round(elapsed, 3)} s")
        except Exception as e:
            end = datetime.datetime.now()
            elapsed = (end - start).total_seconds()
            logger.error(f"文件写入失败!{msg}, 耗时：{round(elapsed, 3)} s, {e}")
        return rc, elapsed

    async def async_file_delete(self, filepath):
        """
        异步删除指定文件
        :param filepath:
        :return:
        """
        rc, elapsed = 0, 0
        if POSIX:
            rc, elapsed, _, _ = await self._async_exec(f"rm -rf {filepath}")
            if rc == 0:
                logger.success(f"删除成功！{filepath}")
            else:
                logger.success(f"删除失败！{filepath}")
        else:
            try:
                os.remove(filepath)
            except Exception as e:
                rc = -1
                logger.error(f"删除失败！{filepath}, {e}")
        return rc, elapsed


if __name__ == '__main__':
    pass
