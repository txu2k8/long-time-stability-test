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
from loguru import logger

from utils.util import get_local_files
from pkgs.sqlite_opt import Sqlite3Operation
from config import DB_SQLITE3
from core.stress.base_worker import BaseWorker


class VideoMonitor1(BaseWorker):
    """
    视频监控场景测试 - 1，读取数据库，写删不同协程中并行处理，多对象并行处理
    1、init阶段：新建10桶，初始化数据库用于存储写入的对象路径
    2、Prepare阶段：预埋580万个128MB对象，平均分配到10桶中，预埋数据并行数=30
    3、Main阶段：写删均衡测试
        （1）.写：每秒项queue队列放入16条待上传对象数据信息，16*2000个消费进行并行上传，上传时基于每日对象数和对象idx计算对象应该存储的桶和日期路径，上传完成后写入本地数据库
        （2）.删：读取本地数据库中最早一天的数据，每秒项queue队列放入16条待删除对象数据信息，16*2000个消费进行并行删除，该日数据删除完成后再从本地数据库读取下一天数据
        （3）.写、删并行处理
        （4）。每秒16并行逻辑：连续放入16条数据到queue后，sleep1秒
    """

    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num=1, obj_prefix='', obj_num=10, obj_num_per_day=1,
            multipart=False, concurrent=1, prepare_concurrent=1, idx_width=1, idx_put_start=0, idx_del_start=0
    ):
        super(VideoMonitor1, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num, 1, obj_prefix, obj_num,
            multipart, concurrent, prepare_concurrent, idx_width, idx_put_start, idx_del_start, 0, False
        )
        self.obj_num_per_day = obj_num_per_day
        # 准备源数据文件池 字典
        self.file_list = get_local_files(local_path)
        # 数据库
        self.db_table_name = "video_monitor"
        self.sqlite3_opt = Sqlite3Operation(db_path=DB_SQLITE3, show=False)
        self.start_date = "2022-09-20"
        self.idx_main_start = self.obj_num + 1 if self.idx_put_start <= self.obj_num else self.idx_put_start  # main阶段idx起始
        self.idx_put_done = self.idx_put_start  # put操作完成的进度idx

    def bucket_obj_path_calc(self, idx):
        """
        基于对象idx计算该对象应该存储的桶和对象路径
        :param idx:
        :return:
        """
        # 计算对象应该存储的桶名称
        bucket = self.bucket_name_calc(self.bucket_prefix, idx)
        # 计算对象路径
        date_step = idx // self.obj_num_per_day  # 每日写N个对象，放在一个日期命名的文件夹
        current_date = self.date_str_calc(self.start_date, date_step)
        date_prefix = current_date + '/'
        obj_path = self.obj_path_calc(idx, date_prefix)
        return bucket, obj_path, current_date

    async def worker_put(self, client, idx):
        """
        上传指定对象
        :param client:
        :param bucket:
        :param idx:
        :return:
        """
        bucket, obj_path, current_date = self.bucket_obj_path_calc(idx)
        # 获取待上传的源文件
        src_file = random.choice(self.file_list)
        disable_multipart = self.disable_multipart_calc()
        await client.put_without_attr(src_file.full_path, bucket, obj_path, disable_multipart, src_file.tags)
        # 写入结果到数据库
        insert_sql = '''INSERT INTO video_monitor ( idx, date, bucket, obj_path ) values (?, ?, ?, ?)'''
        data = [(str(idx), current_date, bucket, obj_path)]
        self.sqlite3_opt.insert_update_delete(insert_sql, data)

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

    async def producer_prepare(self, queue):
        """
        produce queue队列，数据预置
        :param queue:
        :return:
        """
        logger.info("Produce PREPARE bucket={}, concurrent={}, ".format(self.bucket_num, self.prepare_concurrent))
        client = self.client_list[0]
        idx = self.idx_put_start
        while idx <= self.obj_num:
            await queue.put((client, idx))
            self.idx_put_done = idx
            if idx % self.prepare_concurrent == 0:
                await asyncio.sleep(1)  # 每秒生产 {prepare_concurrent} 个待处理项
            idx += 1

    async def producer_put(self, queue):
        """
        produce queue队列，每秒生产concurrent个待处理项，持续执行不停止，直到收到kill进程
        idx 一直累加，设计idx宽度为11位=百亿级
        :param queue:
        :return:
        """
        logger.info("Produce PUT bucket={}, concurrent={}, ".format(self.bucket_num, self.concurrent))
        client = self.client_list[0]
        idx = self.idx_main_start
        while True:
            await queue.put((client, idx))
            self.idx_put_done = idx
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
        logger.info("Produce DELETE bucket={}, concurrent={}, ".format(self.bucket_num, self.concurrent))
        client = self.client_list[0]
        start_date = self.start_date
        rows = []
        count = 0
        while True:
            for row in rows:  # 依次处理每个桶中数据：写、读、删、列表、删等
                #  (19, '18', '2022-09-21', 'bucket0', '2022-09-21/data00000000018')
                # ('bucket0', '2022-09-21/data00000000018')
                bucket = row[0]
                obj_path = row[1]
                await queue.put((client, bucket, obj_path))
                if count % self.concurrent == 0:
                    await asyncio.sleep(1)  # 每秒生产 {concurrent} 个待处理项
                count += 1
            logger.debug("当前 put_idx={}".format(self.idx_put_done))
            if self.idx_put_done > self.obj_num:  # 预置数据完成
                end_date = self.date_str_calc(start_date, 1)  # 删除开始日期1天后的数据
                sql_cmd = '''SELECT bucket,obj_path FROM {} where  date >= \"{}\" and date < \"{}\"'''.format(
                    self.db_table_name, start_date, end_date)
                logger.info(sql_cmd)
                rows = self.sqlite3_opt.fetchall(sql_cmd)
                start_date = end_date
                await asyncio.sleep(0)
            else:
                logger.debug("DELETE：数据预置中，暂不删除...")
                await asyncio.sleep(1)

    async def producer(self, queue_put, queue_delete):
        # await self.producer_put(queue_put)
        # await self.producer_delete(queue_delete)

        asyncio.ensure_future(self.producer_put(queue_put))
        asyncio.ensure_future(self.producer_delete(queue_delete))
        await asyncio.sleep(0)

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

    def stage_init(self):
        """
        初始化
        :return:
        """
        logger.log("STAGE", "init->批量创建特定桶、初始化数据库，bucket_prefix={}, bucket_num={}".format(
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
        logger.log("STAGE", "初始化完成！bucket_prefix={}, bucket_num={}".format(self.bucket_prefix, self.bucket_num))

    async def stage_prepare(self):
        """
        批量创建特定对象
        :return:
        """
        logger.log("STAGE", "prepare->预置对象，PUT obj={}, bucket={}, concurrent={}".format(
            self.obj_num, self.bucket_num, self.prepare_concurrent
        ))
        queue = asyncio.Queue()
        # 创建100倍 concurrent 数量的consumer
        consumers = [asyncio.ensure_future(self.consumer_put(queue)) for _ in range(self.concurrent * 2000)]

        await self.producer_prepare(queue)
        await queue.join()
        for c in consumers:
            c.cancel()
        logger.log("STAGE", "预置对象完成！obj={}, bucket={}".format(self.obj_num, self.bucket_num))
        await asyncio.sleep(5)

    async def stage_main(self):
        logger.log("STAGE", "main->写删均衡测试，put_obj_idx_start={}, bucket={}, concurrent={}".format(
            self.idx_main_start, self.bucket_num, self.concurrent
        ))

        queue_put = asyncio.Queue()
        queue_delete = asyncio.Queue()
        # 创建100倍 concurrent 数量的consumer
        consumers_put = [asyncio.ensure_future(self.consumer_put(queue_put)) for _ in range(self.concurrent * 2000)]
        consumers_del = [asyncio.ensure_future(self.consumer_delete(queue_delete)) for _ in
                         range(self.concurrent * 2000)]

        await self.producer(queue_put, queue_delete)
        await queue_put.join()
        await queue_delete.join()
        for c in consumers_put:
            c.cancel()
        for c in consumers_del:
            c.cancel()

    def run(self):
        """
        执行 生产->消费 queue
        :return:
        """
        self.stage_init()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.stage_prepare())

        asyncio.ensure_future(self.stage_main())
        loop.run_forever()


if __name__ == '__main__':
    vm = VideoMonitor1(
        'mc', '127.0.0.1:9000', 'minioadmin', 'minioadmin', False, 'play',
        'D:\\minio\\upload_data', 'bucket', concurrent=2
    )
    vm.run()
