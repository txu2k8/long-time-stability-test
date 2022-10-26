#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:video_3
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

from client.mc import MClient
from workflow.video_monitor.base import BaseVideoMonitor


class VideoMonitor3(BaseVideoMonitor):
    """视频监控场景测试 - 3，写删不同协程中并行处理，多对象并行处理（不读取数据库）"""
    def __init__(
            self,
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num=1, obj_prefix='', obj_num=10, multipart=False, local_path="",
            main_concurrent=1, prepare_concurrent=1, max_workers=1, idx_width=1, idx_put_start=0, idx_del_start=0,
            obj_num_per_day=1,
    ):
        super(VideoMonitor3, self).__init__(
            client_types, endpoint, access_key, secret_key, tls, alias,
            bucket_prefix, bucket_num, obj_prefix, obj_num, multipart, local_path,
            main_concurrent, prepare_concurrent, max_workers, idx_width, idx_put_start, idx_del_start, obj_num_per_day
        )
        pass

    async def worker(self, client: MClient, idx_put, idx_del=-1):
        """
        上传指定对象
        :param client:
        :param idx_put:
        :param idx_del:
        :return:
        """
        bucket, obj_path, current_date = self.bucket_obj_path_calc(idx_put)
        # 获取待上传的源文件
        src_file = random.choice(self.file_list)
        disable_multipart = self.disable_multipart_calc()
        # 上传
        rc, elapsed = await client.put_without_attr(src_file.full_path, bucket, obj_path, disable_multipart, src_file.tags)

        # 写入结果到数据库
        # self.db_obj_insert(str(idx_put), current_date, bucket, obj_path, src_file.md5, rc, elapsed, qsize)

        # 统计数据
        self.statistics(elapsed)

        # 删除对象
        if idx_del > 0:
            bucket_del, obj_path_del, _ = self.bucket_obj_path_calc(idx_del)
            await client.delete(bucket_del, obj_path_del)
            # 更新删除结果到数据库表
            # if rc == 0:
            #     self.db_obj_update_delete_flag(str(idx_del))

    async def producer_main(self, queue):
        """
        prepare阶段 produce queue队列
        :param queue:
        :return:
        """
        logger.info("Produce Main PUT/DEL bucket={}, concurrent={}, ".format(self.bucket_num, self.main_concurrent))
        client = self.client_list[0]
        idx_put = self.idx_main_start
        idx_del = self.idx_del_start if self.idx_del_start > 0 else -1
        while True:
            logger.debug("put:{} , del:{}".format(idx_put, idx_del))
            if self.idx_put_current >= self.obj_num:
                await queue.put((client, idx_put, idx_del))  # 写+删
                idx_del += 1
            else:
                await queue.put((client, idx_put, -1))  # 仅写
            self.idx_put_current = idx_put
            if idx_put % self.main_concurrent == 0:
                await asyncio.sleep(1)  # 每秒生产 {main_concurrent} 个待处理项
            idx_put += 1


if __name__ == '__main__':
    pass
