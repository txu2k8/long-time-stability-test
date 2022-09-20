#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:video_monitor
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

from stress.base_worker import BaseWorker
from utils.util import get_local_files
from pkgs.sqlite_opt import Sqlite3Operation
from config import DB_SQLITE3


class VideoMonitor(BaseWorker):
    """视频监控场景测试"""
    def __init__(
            self,
            client_type, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num=1, depth=1, obj_prefix='', obj_num=1,
            concurrent=1, multipart=False, duration=0, cover=False, idx_start=0, idx_width=1
    ):
        super(VideoMonitor, self).__init__(
            client_type, endpoint, access_key, secret_key, tls, alias,
            local_path, bucket_prefix, bucket_num, depth, obj_prefix, obj_num,
            concurrent, multipart, duration, cover, idx_start, idx_width
        )
        # 准备源数据文件池 字典
        self.file_list = get_local_files(local_path)
        # 数据库
        self.sqlite3_opt = Sqlite3Operation(db_path=DB_SQLITE3, show=True)

    def prepare(self):
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

    async def worker(self, client, bucket, idx):
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


if __name__ == '__main__':
    put = VideoMonitor(
        'mc', '127.0.0.1:9000', 'minioadmin', 'minioadmin', False, 'play',
        'D:\\minio\\upload_data', 'bucket', 1, 2, '', 10,
        3, False, 30
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(put.run())
    loop.close()
