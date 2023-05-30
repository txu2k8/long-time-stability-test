#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:one_channel.py
@time:2022/09/28
@email:tao.xu2008@outlook.com
@description: 视频监控场景测试 - 对象存储 - 模拟一路视频数据流
"""
import asyncio
from loguru import logger

from client.s3.s3_api import S3API
from workflow.video.calculate import VSInfo
from workflow.video.video_workflow_base import VideoWorkflowBase


class S3VideoWorkflowOneChannel(VideoWorkflowBase):
    """
    视频监控场景测试 - 文件存储
    1、init阶段：新建桶，初始化数据库用于存储写入的对象路径
    2、Prepare阶段：预埋对象
    3、Main阶段：测试执行 上传、删除等
    """

    def __init__(
            self, client, file_info, channel_id, vs_info: VSInfo,
            skip_stage_init=False, write_only=False, delete_immediately=False,
            single_root=False, single_root_name="video", duration=0
    ):
        super(S3VideoWorkflowOneChannel, self).__init__(
            file_info, channel_id, vs_info,
            skip_stage_init, write_only, delete_immediately, single_root, single_root_name, duration
        )
        # 初始化客户端
        self.client = client

        # 自定义
        self.bucket = self.root_dir_name
        self.s3_api = S3API(self.client.endpoint, self.client.access_key, self.client.secret_key)

    async def worker(self, idx_put, segment=0):
        """
        worker
        :param idx_put:
        :param segment:
        :return:
        """
        # 删除旧数据
        idx_del = idx_put - self.vs_info.obj_num_pc
        if idx_del > 0:
            obj_path_del, _ = self.file_path_calc(idx_del)
            await self.client.async_delete(self.bucket, obj_path_del)

        # 上传
        obj_path, current_date = self.file_path_calc(idx_put)
        if self.vs_info.appendable:
            # 追加写模式  TODO
            elapsed = await self.s3_api.append_write_async(
                self.client.endpoint, self.file_info.full_path, self.bucket, obj_path, 0, self.file_info.rb_data_list[0].data, self.vs_info.segments
            )
        else:
            _, elapsed = await self.client.put_without_attr(
                self.file_info.full_path, self.bucket, obj_path, self.vs_info.disable_multipart, self.file_info.tags
            )
        # 统计数据
        self.statistics(elapsed)

    async def stage_init(self):
        """
        批量创建特定桶
        :return:
        """
        logger.log("STAGE", f"初始化阶段：创建特定桶({self.bucket})、初始化数据库")
        self.client.mb(self.bucket, appendable=self.vs_info.appendable)
        await asyncio.sleep(0)
