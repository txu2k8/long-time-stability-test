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

from workflow.video_monitor.video_workflow import VideoWorkflow


class VideoMonitor1(VideoWorkflow):
    """
    视频监控场景测试 - 1，写删不同协程中并行处理，多对象并行处理（读取数据库）
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
            bucket_prefix, bucket_num=1, obj_prefix='', obj_num=10, multipart=False, local_path="",
            main_concurrent=1, prepare_concurrent=1, max_workers=1, idx_width=1, idx_put_start=0, idx_del_start=0,
            obj_num_per_day=1,
    ):
        super(VideoMonitor1, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num, obj_prefix, obj_num, multipart, local_path,
            main_concurrent, prepare_concurrent, max_workers, idx_width, idx_put_start, idx_del_start, obj_num_per_day
        )
        pass

    async def worker_put(self, client, idx):
        """
        上传指定对象
        :param client:
        :param idx:
        :return:
        """
        bucket, obj_path, current_date = self.bucket_obj_path_calc(idx)
        # 获取待上传的源文件
        src_file = random.choice(self.file_list)
        disable_multipart = self.disable_multipart_calc()
        rc, elapsed = await client.put_without_attr(src_file.full_path, bucket, obj_path, disable_multipart, src_file.tags)
        # 写入结果到数据库
        self.db_obj_insert(str(idx), current_date, bucket, obj_path, src_file.md5, rc, elapsed)

    async def worker_delete(self, client, idx):
        """
        上传指定对象
        :param client:
        :param idx:
        :return:
        """
        bucket, obj_path, _ = self.bucket_obj_path_calc(idx)
        rc = await client.delete(bucket, obj_path)
        if rc == 0:
            self.db_obj_delete(str(idx))

    async def producer_put(self, queue):
        """
        produce queue队列，每秒生产concurrent个待处理项，持续执行不停止，直到收到kill进程
        idx 一直累加，设计idx宽度为11位=百亿级
        :param queue:
        :return:
        """
        logger.info("Produce PUT bucket={}, concurrent={}, ".format(self.bucket_num, self.main_concurrent))
        client = self.client_list[0]
        idx = self.idx_main_start
        while True:
            await queue.put((client, idx))
            self.idx_put_current = idx
            if idx % self.main_concurrent == 0:
                await asyncio.sleep(1)  # 每秒生产 {concurrent} 个待处理项
            idx += 1

    async def producer_delete(self, queue):
        """
        produce queue队列，每秒生产concurrent个待处理项，持续执行不停止，直到收到kill进程
        idx 一直累加，设计idx宽度为11位=百亿级
        :param queue:
        :return:
        """
        logger.info("Produce DELETE bucket={}, concurrent={}, ".format(self.bucket_num, self.main_concurrent))
        client = self.client_list[0]
        idx_del = self.idx_del_start if self.idx_del_start > 0 else -1
        while True:
            logger.debug("当前 put_idx={}".format(self.idx_put_current))
            if self.idx_put_current > self.obj_num:  # 预置数据完成
                await queue.put((client, idx_del))
                if idx_del % self.main_concurrent == 0:
                    await asyncio.sleep(1)  # 每秒生产 {concurrent} 个待处理项
                idx_del += 1
            else:
                logger.debug("DELETE：数据预置中，暂不删除...")
                await asyncio.sleep(1)

    async def producer(self, queue_put, queue_delete):
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

    async def stage_main(self):
        logger.log("STAGE", "main->写删均衡测试，put_obj_idx_start={}, bucket={}, concurrent={}".format(
            self.idx_main_start, self.bucket_num, self.main_concurrent
        ))

        queue_put = asyncio.Queue()
        queue_delete = asyncio.Queue()
        # 创建100倍 concurrent 数量的consumer
        consumers_put = [asyncio.ensure_future(self.consumer_put(queue_put)) for _ in range(self.main_concurrent * 2000)]
        consumers_del = [asyncio.ensure_future(self.consumer_delete(queue_delete)) for _ in
                         range(self.main_concurrent * 2000)]

        await self.producer(queue_put, queue_delete)
        await queue_put.join()
        await queue_delete.join()
        for c in consumers_put:
            c.cancel()
        for c in consumers_del:
            c.cancel()


if __name__ == '__main__':
    pass
