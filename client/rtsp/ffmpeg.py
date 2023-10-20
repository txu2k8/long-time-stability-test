#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author:TXU
@file:pull
@time:2023/10/20
@email:tao.xu2008@outlook.com
@description: 
"""
import os
import datetime

from loguru import logger

from client.base_client import BaseClient


class FfmpegError(Exception):
    def __init__(self, cmd, stdout, stderr):
        super(FfmpegError, self).__init__('{} error (see stderr output for detail)'.format(cmd))
        self.stdout = stdout
        self.stderr = stderr


class Ffmpeg(BaseClient):
    """ffmpeg 实现 rtsp 推流、拉流"""

    def __init__(self, quiet=False, cwd=None):
        super(Ffmpeg, self).__init__()
        self.quiet = quiet
        self.cwd = cwd

    async def async_pull(self, rtsp_url, target, ffmpeg_bin='ffmpeg', loglevel='info', vcodec='copy', overwrite=True):
        """
        拉流
        :param rtsp_url:
        :param target:
        :param ffmpeg_bin:
        :param loglevel:
        :param vcodec:
        :param overwrite:
        :return:
        """
        # 命令组装
        args = [
            ffmpeg_bin,
            '-v', loglevel,
            '-rtsp_transport', 'tcp',
            '-i', rtsp_url,
            '-vcodec', vcodec,
            '-t', '256',
            target
        ]
        if overwrite:
            args += ['-y']

        cmd = ' '.join(args)
        rc = -1
        msg = f"{rtsp_url}->{target}"
        start = datetime.datetime.now()
        try:
            dir_name = os.path.dirname(target)
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
            logger.success(f"文件写入开始！{msg}")

            rc, elapsed, _, _ = await self._async_exec(cmd)
            if rc == 0:
                logger.success(f"文件写入成功!{msg}, 耗时：{round(elapsed, 3)} s")
            else:
                logger.error(f"文件写入失败!{msg}, 耗时：{round(elapsed, 3)} s")
        except Exception as e:
            end = datetime.datetime.now()
            elapsed = (end - start).total_seconds()
            logger.error(f"文件写入失败!{msg}, 耗时：{round(elapsed, 3)} s, {e}")
        return rc, elapsed


if __name__ == '__main__':
    pass
