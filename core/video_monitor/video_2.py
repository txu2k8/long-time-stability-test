#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:video_1
@time:2022/09/19
@email:tao.xu2008@outlook.com
@description: 视频监控场景测试

1、单节点1000路视频
2、码流：4Mbps
3、写删均衡
"""
import random
import asyncio
import arrow
from loguru import logger

from utils.util import get_local_files
from pkgs.sqlite_opt import Sqlite3Operation
from config import DB_SQLITE3
from core.video_monitor.video_1 import VideoMonitor1


class VideoMonitor2(VideoMonitor1):
    """视频监控场景测试 - 2，不读取数据库，写删不同协程中并行处理，多对象并行处理"""
    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num=1, obj_prefix='', obj_num=10, obj_num_per_day=1,
            multipart=False, concurrent=1, prepare_concurrent=1, idx_width=1, idx_put_start=0, idx_del_start=0
    ):
        super(VideoMonitor2, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num, obj_prefix, obj_num, obj_num_per_day,
            multipart, concurrent, prepare_concurrent, idx_width, idx_put_start, idx_del_start
        )
        pass

    async def worker_delete(self, client, idx):
        """
        上传指定对象
        :param client:
        :param idx:
        :return:
        """
        bucket, obj_path, _ = self.bucket_obj_path_calc(idx)
        await client.delete(bucket, obj_path)

    async def producer_delete(self, queue):
        """
        produce queue队列，每秒生产concurrent个待处理项，持续执行不停止，直到收到kill进程
        idx 一直累加，设计idx宽度为11位=百亿级
        :param queue:
        :return:
        """
        logger.info("Produce DELETE bucket={}, concurrent={}, ".format(self.bucket_num, self.concurrent))
        client = self.client_list[0]
        start_date = self.start_date
        idx_del = self.idx_del_start if self.idx_del_start > 0 else -1
        while True:
            logger.debug("当前 put_idx={}".format(self.idx_put_done))
            if self.idx_put_done > self.obj_num:  # 预置数据完成
                await queue.put((client, idx_del))
                if idx_del % self.concurrent == 0:
                    await asyncio.sleep(1)  # 每秒生产 {concurrent} 个待处理项
                idx_del += 1
            else:
                logger.debug("DELETE：数据预置中，暂不删除...")
                await asyncio.sleep(1)


if __name__ == '__main__':
    pass
