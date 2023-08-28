#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author:TXU
@file:worker
@time:2023/8/26
@email:tao.xu2008@outlook.com
@description: 文件系统 - 压力测试 - Worker
"""

import os
from typing import List
from loguru import logger

from config.models import FileInfo
from client.fs.file_ops import FileOps
from workflow.stress.calculate import StressInfo
from workflow.stress.stress_workflow_base import StressWorkflowBase


class FsWorker(StressWorkflowBase):
    """文件系统 - 压力测试 - Worker"""

    def __init__(
            self, target, src_files: List[FileInfo], stress_info: StressInfo,
            channel_id=0, skip_stage_init=False,
            single_root=False, single_root_name="stress",
            write_only=False, read_only=False, delete_only=False, delete_immediately=False,
            duration=0, depth=1, cover=False,
    ):
        super(FsWorker, self).__init__(
            src_files, stress_info, channel_id, skip_stage_init,
            single_root, single_root_name,
            write_only, read_only, delete_only, delete_immediately,
            duration, depth, cover
        )
        self.target = target
        self.file_ops = FileOps()

    def file_abspath_calc(self, idx):
        """
        abs path
        :param idx:
        :return:
        """
        file_path, _ = self.calc_file_path(idx)
        return os.path.abspath(os.path.join(self.target, self.root_dir_name, file_path))

    def put_worker(self, idx_put, segment):
        """
        PUT worker
        :param idx_put:
        :param segment:
        :return:
        """
        logger.debug(f"Worker:channel={self.channel_name}, idx={idx_put}, segment={segment}")

        # 写入
        file_info = self.get_src_file_info(idx_put)
        file_abs_path = self.file_abspath_calc(idx_put)

        # open->write 方式写入
        # self.file_ops.file_write(
        #     file_abs_path, file_info.rb_data_list[segment].data, appendable=self.stress_info.appendable,
        #     segment_idx=segment, segment_total=self.stress_info.segments, src_path=file_info.full_path
        # )

        # cp命令方式写入
        return self.file_ops.file_cp(file_info.full_path, file_abs_path)

    def get_worker(self, idx_get):
        """
        GET worker
        :param idx_get:
        :return:
        """
        file_abs_path = self.file_abspath_calc(idx_get)
        return self.file_ops.file_cp(file_abs_path, dst_path="/tmp/")

    def del_worker(self, idx_del):
        """
        DELETE worker
        :param idx_del:
        :return:
        """
        del_file_abs_path = self.file_abspath_calc(idx_del)
        self.file_ops.file_delete(del_file_abs_path)


if __name__ == '__main__':
    pass
