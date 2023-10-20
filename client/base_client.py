#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author:TXU
@file:exec
@time:2023/10/20
@email:tao.xu2008@outlook.com
@description: 
"""
import datetime
import subprocess
import asyncio

from loguru import logger


class BaseClient(object):
    """文件操作"""
    def __init__(self):
        pass

    @staticmethod
    def _exec(cmd):
        start = datetime.datetime.now()
        rc, output = subprocess.getstatusoutput(cmd)
        end = datetime.datetime.now()
        elapsed = (end - start).total_seconds()  # 耗时 x.y 秒
        logger.debug(output.strip('\n'))
        return rc, elapsed, output

    @staticmethod
    async def _async_exec(cmd):
        """
        异步执行命令
        :param cmd:
        :return:
        """
        start = datetime.datetime.now()
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        end = datetime.datetime.now()
        elapsed = (end - start).total_seconds()  # 耗时 x.y 秒
        rc = proc.returncode
        if stdout:
            logger.debug(stdout.decode().strip('\n'))
        if stderr:
            logger.error('Response({}):\n{}'.format(cmd, stderr.decode().strip('\n')))
        return rc, elapsed, stdout, stderr


if __name__ == '__main__':
    pass
