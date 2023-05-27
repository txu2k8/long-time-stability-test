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
from abc import ABC
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger

from workflow.workflow_base import WorkflowBase
from workflow.workflow_interface import WorkflowInterface

is_exit = False


def handler(signum, frame):
    global is_exit
    is_exit = True
    logger.warning("receive a signal {0}, is_exit = {1}".format(signum, is_exit))


class BaseStress(WorkflowBase, WorkflowInterface, ABC):
    """
    压力测试 - 基类，多线程并发处理
    """

    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num=1, obj_prefix='', obj_num=10, multipart=False, local_path="",
            main_concurrent=1, prepare_concurrent=1, idx_width=1, idx_put_start=0, idx_del_start=0,
            depth=1, duration=0, cover=False,
    ):
        super(BaseStress, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num, obj_prefix, obj_num, multipart, local_path,
            main_concurrent, prepare_concurrent, idx_width, idx_put_start, idx_del_start
        )
        self.depth = depth
        self.duration = duration
        self.cover = cover

    def stage_init(self):
        """
        批量创建特定桶
        :return:
        """
        # 开启debug日志
        # self.set_core_loglevel()
        pass

    def worker(self, client, idx):
        """
        操作 worker，各个实例单独实现
        :param client:
        :param idx:
        :return:
        """
        logger.info('worker示例，待实例自定义')

    def producer_prepare(self):
        """
        预埋阶段：生成器 - 生成待处理数据
        :return:
        """
        logger.info("Produce PREPARE bucket={}, concurrent={}, ".format(self.bucket_num, self.prepare_concurrent))
        client = self.client_list[0]
        idx = self.idx_put_start
        while idx <= self.obj_num:
            yield client, idx
            self.idx_put_current = idx
            idx += 1

    def producer_main(self):
        """
        Main阶段：生成器 - 生成待处理数据
        :return:
        """
        # 生产待处理的queue列表
        logger.info("PUT obj={}, bucket={}, concurrent={}, ".format(self.obj_num, self.bucket_num, self.main_concurrent))
        if self.duration <= 0:
            for x in range(self.idx_put_start, self.obj_num):
                logger.trace("producing {}/{}".format(x, self.obj_num))
                client = random.choice(self.client_list)  # 随机选择客户端
                yield client, x
            return

        logger.info("Run test duration {}s".format(self.duration))
        start = datetime.datetime.now()
        end = start
        produce_loop = 1
        idx_start = self.idx_put_start
        idx_end = self.obj_num
        while self.duration > (end - start).total_seconds():
            logger.info("Loop: {}".format(produce_loop))
            for idx in range(idx_start, idx_end):
                if self.duration <= (end - start).total_seconds():
                    break
                logger.trace("Loop-{} producing {}/{}".format(produce_loop, idx, self.obj_num))
                client = random.choice(self.client_list)  # 随机选择客户端
                yield client, idx
                idx += 1
                end = datetime.datetime.now()
            produce_loop += 1
            if not self.cover:
                idx_start = idx_end
                idx_end = self.obj_num * produce_loop
        logger.info("duration {}s completed!".format(self.duration))
        return

    def stage_prepare(self):
        """
        批量创建特定对象
        :return:
        """
        logger.log("STAGE", "prepare->数据预埋，obj={}, bucket={}, concurrent={}".format(
            self.obj_num, self.bucket_num, self.prepare_concurrent
        ))
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        futures = set()
        with ThreadPoolExecutor(max_workers=self.prepare_concurrent) as executor:
            for item in self.producer_prepare():
                futures.add(executor.submit(self.worker, *item))
                if is_exit:
                    break
        for future in as_completed(futures):
            future.result()

    def stage_main(self):
        """
        执行 生产->消费 queue
        :return:
        """
        logger.log("STAGE", "main->执行测试，obj={}, bucket={}, concurrent={}".format(
            self.obj_num, self.bucket_num, self.main_concurrent
        ))
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        futures = set()
        with ThreadPoolExecutor(max_workers=self.main_concurrent) as executor:
            for item in self.producer_main():
                futures.add(executor.submit(self.worker, *item))
                if is_exit:
                    break
        for future in as_completed(futures):
            future.result()

    def run(self):
        self.stage_init()
        self.stage_prepare()
        self.stage_main()


if __name__ == '__main__':
    pass