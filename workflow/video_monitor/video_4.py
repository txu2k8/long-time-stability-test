#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:video_4
@time:2022/10/25
@email:tao.xu2008@outlook.com
@description: 视频监控场景测试

1、单节点1000路视频
2、码流：4Mbps
3、写删均衡
4、模拟波峰、波谷
"""
import random
import asyncio
from loguru import logger

from workflow.video_monitor.video_3 import VideoMonitor3


class VideoMonitor4(VideoMonitor3):
    """视频监控场景测试 - 4，写删均衡测试（模拟波峰波谷）"""
    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num=1, obj_prefix='', obj_num=10, multipart=False, local_path="",
            main_concurrent=1, prepare_concurrent=1, max_workers=1, idx_width=1, idx_put_start=0, idx_del_start=0,
            obj_num_per_day=1,
    ):
        super(VideoMonitor4, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num, obj_prefix, obj_num, multipart, local_path,
            main_concurrent, prepare_concurrent, max_workers, idx_width, idx_put_start, idx_del_start, obj_num_per_day
        )
        pass

    async def producer_main(self, queue):
        """
        main阶段 produce queue队列
        :param queue:
        :return:
        """
        logger.info("Produce Main PUT/DEL bucket={}, concurrent={}, ".format(self.bucket_num, self.main_concurrent))
        client = self.client_list[0]
        idx_put = self.idx_main_start
        idx_del = self.idx_del_start if self.idx_del_start > 0 else -1
        sleep_base = 1 / self.main_concurrent
        sleep_idx_step_list = [(sleep_base*n, n) for n in (1, 1, 1, 1, 3, 3, 3, 5, 5, 10, 20, 30)]  # sleep x秒,写删文件数
        sleep_n, idx_step = random.choice(sleep_idx_step_list)
        sleep_idx = idx_put + idx_step
        while True:
            logger.info("put:{} , del:{}".format(idx_put, idx_del))
            logger.info("sleep_n={}, idx_step={}".format(sleep_n, idx_step))
            if self.idx_put_current >= self.obj_num:
                await queue.put((client, idx_put, idx_del))  # 写+删
                idx_del += 1
            else:
                await queue.put((client, idx_put, -1))  # 仅写
            self.idx_put_current = idx_put

            if idx_put == sleep_idx:
                await asyncio.sleep(sleep_n)
                sleep_n, idx_step = random.choice(sleep_idx_step_list)
                sleep_idx = idx_put + idx_step
            idx_put += 1


if __name__ == '__main__':
    pass
