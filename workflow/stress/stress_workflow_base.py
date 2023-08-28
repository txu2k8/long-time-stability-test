#!/usr/bin/python
# -*- coding:utf-8 _*- 
"""
@author:TXU
@file:base
@time:2022/09/09
@email:tao.xu2008@outlook.com
@description:
"""
import signal
import random
import datetime
from typing import List
from abc import ABC
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger

from config.models import FileInfo
from workflow.workflow_base import WorkflowBase
from workflow.workflow_interface import WorkflowInterface
from workflow.stress.calculate import StressInfo

is_exit = False


def handler(signum, frame):
    global is_exit
    is_exit = True
    logger.warning("receive a signal {0}, is_exit = {1}".format(signum, is_exit))


class StressWorkflowBase(WorkflowBase, WorkflowInterface, ABC):
    """
    压力测试 - 基类，多线程并发处理
    """

    def __init__(
            self,
            src_files: List[FileInfo], stress_info: StressInfo, channel_id=0, skip_stage_init=False,
            single_root=False, single_root_name="stress",
            write_only=False, read_only=False, delete_only=False, delete_immediately=False,
            duration=0, depth=1, cover=False
    ):
        super(StressWorkflowBase, self).__init__(write_only, read_only, delete_only, delete_immediately)
        self.src_files = src_files
        self.len_files = len(self.src_files)
        self.stress_info = stress_info
        self.skip_stage_init = skip_stage_init

        self.channel_id = channel_id  # 视频 channel ID
        self.channel_name = f"{self.stress_info.root_prefix}{channel_id}"  # 单桶/单根目录模式时，视频写入目录名
        self.root_dir_name = single_root_name if single_root else self.channel_name  # 多桶/目录模式，每路视频写入指定桶/目录

        self.duration = duration
        self.depth = depth  # 默认使用对象目录深度=1，即不建子目录
        self.cover = cover

    def get_src_file_info(self, idx) -> FileInfo:
        """
        为每个idx文件/对象 分配指定的 源文件，统一获取方式便于后续检查
        :param idx:
        :return:
        """
        if self.len_files == 1:
            return self.src_files[0]
        lid = idx % self.len_files
        return self.src_files[lid]

    def calc_file_path(self, idx):
        """
        基于对象idx计算该对象应该存储的桶和对象路径
        :param idx:
        :return:
        """
        # 计算对象/文件路径
        file_info = self.get_src_file_info(idx)
        date_step = idx // self.stress_info.obj_num_pc_pd  # 每日写N个对象，放在一个日期命名的文件夹
        current_date = self.calc_date_str(self.start_date, date_step)
        date_prefix = current_date + '/'
        file_path = self.calc_file_path_base(
            idx, self.depth, date_prefix, self.stress_info.file_prefix, self.stress_info.idx_width, file_info.file_type
        )
        return file_path, current_date

    def stage_init(self):
        """
        批量创建特定桶 / 文件目录
        :return:
        """
        # 开启debug日志
        # self.set_core_loglevel()
        pass

    def put_worker(self, *args, **kwargs):
        """
        PUT worker
        :param args:
        :param kwargs:
        :return:
        """
        pass

    def get_worker(self, *args, **kwargs):
        """
        GET worker
        :param args:
        :param kwargs:
        :return:
        """
        pass

    def del_worker(self, *args, **kwargs):
        """
        DELETE worker
        :param args:
        :param kwargs:
        :return:
        """
        pass

    def worker(self, *args, **kwargs):
        """
        操作 worker，各个实例单独实现
        :param args:
        :param kwargs:
        :return:
        """
        idx, segment = args[0], args[1]
        logger.debug(f'worker: idx={idx},segment={segment}')
        idx_put = self._calc_put_idx(idx)
        idx_get = self._calc_get_idx(idx)

        # 上传 / 写入
        if idx_put >= 0:
            return self.put_worker(idx_put, segment)

        # 下载 / 读取
        if idx_get >= 0:
            return self.get_worker(idx_get)

        # 删除
        if segment == 0:
            idx_del = self._calc_del_idx(idx)
            if idx_del >= 0:
                return self.del_worker(idx_del)

        return True

    def producer(self):
        """
        生成器 - 生成待处理数据
        :return:
        """
        idx = self.stress_info.idx_put_start
        while True:
            for segment in range(0, self.stress_info.segments):
                yield idx, segment
            idx += 1

    def stage_prepare(self):
        """
        批量创建特定对象
        :return:
        """
        logger.info('prepare示例，待实例自定义')

    def stage_main(self):
        """
        执行 并发处理
        :return:
        """
        logger.log("STAGE", "main->执行测试，max_workers={}".format(self.stress_info.max_workers))
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        futures = set()
        with ThreadPoolExecutor(max_workers=self.stress_info.max_workers) as executor:
            for item in self.producer():
                futures.add(executor.submit(self.worker, *item))
                if is_exit:
                    break
        for future in as_completed(futures):
            future.result()

    def run(self):
        if not self.skip_stage_init:
            self.stage_init()
        self.stage_main()


if __name__ == '__main__':
    pass
