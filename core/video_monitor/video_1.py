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
from concurrent.futures import ProcessPoolExecutor, as_completed
from loguru import logger

from utils.util import get_local_files
from pkgs.sqlite_opt import Sqlite3Operation
from config import DB_SQLITE3
from core.stress.base_worker import BaseWorker


class VideoMonitor1(BaseWorker):
    """视频监控场景测试 - 1"""
    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num=1, obj_prefix='',
            concurrent=1, multipart=False, idx_start=0, idx_width=1
    ):
        super(VideoMonitor1, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num, 1, obj_prefix, 1,
            concurrent, multipart, 0, False, idx_start, idx_width
        )
        # 准备源数据文件池 字典
        self.file_list = get_local_files(local_path)
        # 数据库
        self.sqlite3_opt = Sqlite3Operation(db_path=DB_SQLITE3, show=True)

    def init(self):
        # 开启debug日志
        # self.set_core_loglevel()

        # 准备桶
        client = random.choice(self.client_list)
        self.make_bucket_if_not_exist(client, self.bucket_prefix, self.bucket_num)

        # 初始化数据库
        create_table = '''CREATE TABLE IF NOT EXISTS `video_monitor` (
                                      `id` INTEGER PRIMARY KEY,
                                      `date` varchar(20) NOT NULL,
                                      `object` varchar(500) NOT NULL
                                    )
                                    '''
        self.sqlite3_opt.execute('DROP TABLE IF EXISTS video_monitor')
        self.sqlite3_opt.create_table(create_table)

    async def worker_put(self, client, bucket, idx):
        """
        上传指定对象
        :param client:
        :param bucket:
        :param idx:
        :return:
        """
        # date_prefix = ""
        date_step = idx // 3 - 10  # 示例： 每日写3个对象，写10天
        current_time = "2022-09-20"
        date_str = arrow.get(current_time).shift(days=date_step).datetime.strftime("%Y-%m-%d")
        date_prefix = date_str + '/'

        obj_path = self.obj_path_calc(idx, date_prefix)
        src_file = random.choice(self.file_list)
        disable_multipart = self.disable_multipart_calc()
        insert_sql = '''INSERT INTO video_monitor ( date, object ) values (?, ?)'''
        data = [(date_str, f'{bucket}/{obj_path}')]
        self.sqlite3_opt.insert_update_delete(insert_sql, data)

        await client.put(src_file.full_path, bucket, obj_path, disable_multipart, src_file.tags, src_file.attr)

    async def worker_delete(self, client, bucket, idx):
        """
        上传指定对象
        :param client:
        :param bucket:
        :param idx:
        :return:
        """
        # date_prefix = ""
        date_step = idx // 3 - 10  # 示例： 每日写3个对象，写10天
        current_time = "2022-09-20"
        date_str = arrow.get(current_time).shift(days=date_step).datetime.strftime("%Y-%m-%d")
        date_prefix = date_str + '/'

        obj_path = self.obj_path_calc(idx, date_prefix)
        src_file = random.choice(self.file_list)
        disable_multipart = self.disable_multipart_calc()
        insert_sql = '''INSERT INTO video_monitor ( date, object ) values (?, ?)'''
        data = [(date_str, f'{bucket}/{obj_path}')]
        self.sqlite3_opt.insert_update_delete(insert_sql, data)

        await client.put(src_file.full_path, bucket, obj_path, disable_multipart, src_file.tags, src_file.attr)

    async def producer(self, queue):
        """
        produce queue队列，每秒生产concurrent个待处理项，持续执行不停止，直到收到kill进程
        idx 一直累加，设计idx宽度为11位=百亿级
        :param queue:
        :return:
        """
        logger.info("PUT bucket={}, concurrent={}, ".format(self.bucket_num, self.concurrent))
        client = self.client_list[0]
        idx = 0
        while True:
            for bucket_idx in range(self.bucket_num):  # 依次处理每个桶中数据：写、读、删、列表、删等
                bucket = self.bucket_name_calc(self.bucket_prefix, bucket_idx)
                await queue.put((client, bucket, idx))
                if idx % self.concurrent == 0:
                    await asyncio.sleep(1)  # 每秒生产 {concurrent} 个待处理项
                idx += 1

    async def consumer_put(self, queue):
        """
        consume queue队列，指定队列中全部被消费
        :param queue:
        :return:
        """
        while True:
            item = await queue.get()
            await self.worker_put(*item)
            queue.task_done()

    async def consumer_delete(self, queue):
        """
        consume queue队列，指定队列中全部被消费
        :param queue:
        :return:
        """
        while True:
            item = await queue.get()
            await self.worker_delete(*item)
            queue.task_done()

    async def run_put(self):
        self.init()
        queue = asyncio.Queue()
        # 创建100倍 concurrent 数量的consumer
        consumers = [asyncio.ensure_future(self.consumer_put(queue)) for _ in range(self.concurrent * 2000)]
        await self.producer(queue)
        await queue.join()
        for c in consumers:
            c.cancel()

    async def run_delete(self):
        queue = asyncio.Queue()
        # 创建100倍 concurrent 数量的consumer
        consumers = [asyncio.ensure_future(self.consumer_delete(queue)) for _ in range(self.concurrent * 2000)]
        await self.producer(queue)
        await queue.join()
        for c in consumers:
            c.cancel()

    def run_until_complete_put(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run_put())
        loop.close()

    def run_until_complete_delete(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run_delete())
        loop.close()

    def prepare(self):
        """
        批量创建特定对象
        :return:
        """
        pass

    def run(self):
        """
        执行 生产->消费 queue
        :return:
        """
        futures = set()
        with ProcessPoolExecutor(max_workers=5) as exector:
            futures.add(exector.submit(self.run_until_complete_put))
            futures.add(exector.submit(self.run_until_complete_delete))
        for future in as_completed(futures):
            future.result()


if __name__ == '__main__':
    vm = VideoMonitor1(
        'mc', '127.0.0.1:9000', 'minioadmin', 'minioadmin', False, 'play',
        'D:\\minio\\upload_data', 'bucket', concurrent=2
    )
    vm.run()
