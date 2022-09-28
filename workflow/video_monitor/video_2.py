#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:video_2
@time:2022/09/19
@email:tao.xu2008@outlook.com
@description: 视频监控场景测试

1、单节点1000路视频
2、码流：4Mbps
3、写删均衡
"""
import asyncio
from loguru import logger

from workflow.video_monitor.video_1 import VideoMonitor1


class VideoMonitor2(VideoMonitor1):
    """视频监控场景测试 - 2，写删不同协程中并行处理，多对象并行处理（不读取数据库）"""
    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num=1, obj_prefix='', obj_num=10, multipart=False, local_path="",
            concurrent=1, prepare_concurrent=1, idx_width=1, idx_put_start=0, idx_del_start=0,
            obj_num_per_day=1,
    ):
        super(VideoMonitor2, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num, obj_prefix, obj_num, multipart, local_path,
            concurrent, prepare_concurrent, idx_width, idx_put_start, idx_del_start, obj_num_per_day
        )
        pass

    async def worker_delete(self, client, idx):
        """
        上传指定对象
        :param client:
        :param idx:
        :return:
        """
        bucket, obj_path, _ = self.bucket_obj_path_calc(int(idx))
        await client.delete(bucket, obj_path)
        self.db_delete(str(idx))

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
                idx = row[0]
                await queue.put((client, idx))
                if count % self.concurrent == 0:
                    await asyncio.sleep(1)  # 每秒生产 {concurrent} 个待处理项
                count += 1
            logger.debug("当前 put_idx={}".format(self.idx_put_current))
            if self.idx_put_current > self.obj_num:  # 预置数据完成
                end_date = self.date_str_calc(start_date, 1)  # 删除开始日期1天后的数据
                sql_cmd = '''SELECT idx FROM {} where  date >= \"{}\" and date < \"{}\"'''.format(
                    self.db_table_name, start_date, end_date)
                logger.info(sql_cmd)
                rows = self.sqlite3_opt.fetchall(sql_cmd)
                start_date = end_date
                await asyncio.sleep(0)
            else:
                logger.debug("DELETE：数据预置中，暂不删除...")
                await asyncio.sleep(1)


if __name__ == '__main__':
    pass
