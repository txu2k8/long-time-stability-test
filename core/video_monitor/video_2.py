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
from core.stress.base_worker import BaseWorker


class VideoMonitor1(BaseWorker):
    """视频监控场景测试 - 1"""
    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num=1, obj_prefix='', obj_num=10, obj_per_day=1,
            concurrent=1, multipart=False, idx_start=0, idx_width=1
    ):
        super(VideoMonitor1, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num, 1, obj_prefix, obj_num,
            concurrent, multipart, 0, False, idx_start, idx_width
        )
        self.obj_per_day = obj_per_day
        # 准备源数据文件池 字典
        self.file_list = get_local_files(local_path)
        # 数据库
        self.db_table_name = "video_monitor"
        self.sqlite3_opt = Sqlite3Operation(db_path=DB_SQLITE3, show=False)
        self.start_date = "2022-09-20"
        self.current_date = "2022-09-20"

    async def worker_put(self, client, bucket, idx):
        """
        上传指定对象
        :param client:
        :param bucket:
        :param idx:
        :return:
        """
        # date_prefix = ""
        date_step = idx // self.obj_per_day  # 每日写N个对象，放在一个文件夹
        self.current_date = self.date_str_calc(self.start_date, date_step)
        date_prefix = self.current_date + '/'

        obj_path = self.obj_path_calc(idx, date_prefix)
        src_file = random.choice(self.file_list)
        disable_multipart = self.disable_multipart_calc()
        insert_sql = '''INSERT INTO video_monitor ( idx, date, bucket, obj_path ) values (?, ?, ?, ?)'''
        data = [(str(idx), self.current_date, bucket, obj_path)]
        self.sqlite3_opt.insert_update_delete(insert_sql, data)

        await client.put(src_file.full_path, bucket, obj_path, disable_multipart, src_file.tags, src_file.attr)

    @staticmethod
    async def worker_delete(client, bucket, obj_path):
        """
        上传指定对象
        :param client:
        :param bucket:
        :param obj_path:
        :return:
        """
        await client.delete(bucket, obj_path)

    async def producer_put(self, queue):
        """
        produce queue队列，每秒生产concurrent个待处理项，持续执行不停止，直到收到kill进程
        idx 一直累加，设计idx宽度为11位=百亿级
        :param queue:
        :return:
        """
        logger.info("Produce PUT bucket={}, concurrent={}, ".format(self.bucket_num, self.concurrent))
        client = self.client_list[0]
        idx = 0
        while True:
            for bucket_idx in range(self.bucket_num):  # 依次处理每个桶中数据：写、读、删、列表、删等
                bucket = self.bucket_name_calc(self.bucket_prefix, bucket_idx)
                await queue.put((client, bucket, idx))
                if idx % self.concurrent == 0:
                    await asyncio.sleep(1)  # 每秒生产 {concurrent} 个待处理项
                idx += 1

    async def producer_delete(self, queue):
        """
        produce queue队列，每秒生产concurrent个待处理项，持续执行不停止，直到收到kill进程
        idx 一直累加，设计idx宽度为11位=百亿级
        :param queue:
        :return:
        """
        logger.info("Produce Delete bucket={}, concurrent={}, ".format(self.bucket_num, self.concurrent))
        client = self.client_list[0]
        tmp_date = self.date_str_calc(self.current_date, 3)
        sql_cmd = '''SELECT * FROM {} where date < {}'''.format(self.db_table_name, tmp_date)
        rows = self.sqlite3_opt.fetchall(sql_cmd)
        print(type(rows))
        idx = 0
        while True:
            for row in rows:  # 依次处理每个桶中数据：写、读、删、列表、删等
                bucket = row["bucket"]
                obj_path = row["obj_path"]
                await queue.put((client, bucket, obj_path))
                if idx % self.concurrent == 0:
                    await asyncio.sleep(1)  # 每秒生产 {concurrent} 个待处理项
                idx += 1
            tmp_date = self.date_str_calc(self.current_date, 3)
            sql_cmd = '''SELECT * FROM {} where date < {}'''.format(self.db_table_name, tmp_date)
            rows = self.sqlite3_opt.fetchall(sql_cmd)

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
        queue = asyncio.Queue()
        # 创建100倍 concurrent 数量的consumer
        consumers = [asyncio.ensure_future(self.consumer_put(queue)) for _ in range(self.concurrent * 2000)]
        await self.producer_put(queue)
        await queue.join()
        for c in consumers:
            c.cancel()

    async def run_delete(self):
        queue = asyncio.Queue()
        # 创建100倍 concurrent 数量的consumer
        consumers = [asyncio.ensure_future(self.consumer_delete(queue)) for _ in range(self.concurrent * 2000)]
        await self.producer_delete(queue)
        await queue.join()
        for c in consumers:
            c.cancel()

    def stage_init(self):
        """
        初始化
        :return:
        """
        logger.log("STAGE", "init->批量创建特定桶，bucket_prefix={}, bucket_num={}".format(
            self.bucket_prefix, self.bucket_num
        ))

        # 开启debug日志
        # self.set_core_loglevel()

        # 准备桶
        client = random.choice(self.client_list)
        self.make_bucket_if_not_exist(client, self.bucket_prefix, self.bucket_num)

        # 初始化数据库
        logger.log("STAGE", "init->初始化数据库、建表，table={}".format(self.db_table_name))
        create_table = '''CREATE TABLE IF NOT EXISTS `{}` (
                                      `id` INTEGER PRIMARY KEY,
                                      `idx` varchar(20) NOT NULL,
                                      `date` varchar(20) NOT NULL,
                                      `bucket` varchar(100) NOT NULL,
                                      `obj_path` varchar(500) NOT NULL
                                    )
                                    '''.format(self.db_table_name)
        self.sqlite3_opt.execute('DROP TABLE IF EXISTS {}'.format(self.db_table_name))
        self.sqlite3_opt.create_table(create_table)

    def stage_prepare(self):
        """
        批量创建特定对象
        :return:
        """
        logger.log("STAGE", "prepare->预置对象，PUT obj={}, bucket={}, total={}, concurrent={}".format(
            self.obj_num, self.bucket_num, self.obj_num*self.bucket_num, self.concurrent
        ))

    def tmp1(self, loop):
        # loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run_put())
        # loop.close()

    def tmp2(self, loop):
        # loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run_delete())
        # loop.close()

    def stage_main(self):
        logger.log("STAGE", "main->写删均衡测试，put_obj_idx_start={}, bucket={}, concurrent={}".format(
            self.obj_num, self.bucket_num, self.concurrent
        ))
        # tasks = [
        #     asyncio.ensure_future(self.run_put()),
        #     # asyncio.ensure_future(self.run_delete()),
        # ]
        # loop = asyncio.get_event_loop()
        # loop.run_until_complete(asyncio.gather(*tasks))
        # loop.close()

        import threading as mp
        # p1 = mp.Process(target=asyncio.ensure_future, args=(self.run_put(),))
        # p2 = mp.Process(target=asyncio.ensure_future, args=(self.run_delete(),))

        loop = asyncio.get_event_loop()
        p1 = mp.Thread(target=self.tmp1, args=(loop, ))
        p2 = mp.Thread(target=self.tmp2, args=(loop, ))
        p1.start()
        p1.join()
        p2.start()
        p2.join()
        loop.close()

    def run(self):
        """
        执行 生产->消费 queue
        :return:
        """
        self.stage_init()
        self.stage_prepare()
        self.stage_main()


if __name__ == '__main__':
    vm = VideoMonitor1(
        'mc', '127.0.0.1:9000', 'minioadmin', 'minioadmin', False, 'play',
        'D:\\minio\\upload_data', 'bucket', concurrent=2
    )
    vm.run()
