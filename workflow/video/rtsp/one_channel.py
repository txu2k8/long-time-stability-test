#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author:TXU
@file:one_channel
@time:2023/5/27
@email:tao.xu2008@outlook.com
@description: 视频监控场景测试 - 文件存储 - 模拟一路视频数据流
"""
import os
from loguru import logger

from config.models import FileInfo
from client.fs.file_ops import FileOps
from client.rtsp.ffmpeg import Ffmpeg
from workflow.video.calculate import VSInfo
from workflow.video.video_workflow_base import VideoWorkflowBase

file_info = FileInfo(
    file_type='.mp4'
)


class RtspVideoWorkflowOneChannel(VideoWorkflowBase):
    """视频监控场景测试 - 文件存储 - 流直存"""
    
    def __init__(
            self, target, rtsp_url, channel_id, vs_info: VSInfo,
            skip_stage_init=False, write_only=False, delete_immediately=False,
            single_root=False, single_root_name="video", duration=0
    ):
        super(RtspVideoWorkflowOneChannel, self).__init__(
            file_info, channel_id, vs_info,
            skip_stage_init, write_only, delete_immediately, single_root, single_root_name, duration
        )
        self.rtsp_url = rtsp_url  # rtsp url
        self.target = target  # 文件存储路径

        self.ffmpeg = Ffmpeg()
        self.file_ops = FileOps()

    def file_abspath_calc(self, idx):
        """
        abs path
        :param idx:
        :return:
        """
        file_path, _ = self.calc_file_path(idx)
        return os.path.abspath(os.path.join(self.target, self.root_dir_name, file_path))

    async def worker(self, idx_put, segment):
        """
        写、删 操作
        :param idx_put:
        :param segment:
        :return:
        """
        logger.debug(f"Worker:channel={self.channel_name}, idx={idx_put}, segment={segment}")

        # 拉流写入
        file_abs_path = self.file_abspath_calc(idx_put)
        await self.ffmpeg.async_pull(self.rtsp_url, file_abs_path, ffmpeg_bin=r"D:\ffmpeg-5.1.2-essentials_build\bin\ffmpeg")

        # 删除
        if not self.write_only and segment == 0:
            if self.delete_immediately:
                idx_del = idx_put - 1
            else:
                idx_del = idx_put - self.vs_info.obj_num_pc
            if idx_del > 0:
                del_file_abs_path = self.file_abspath_calc(idx_del)
                await self.file_ops.async_file_delete(del_file_abs_path)


if __name__ == '__main__':
    pass
