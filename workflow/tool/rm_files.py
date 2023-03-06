#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:video_1
@time:2022/09/19
@email:tao.xu2008@outlook.com
@description: 通过 rsync 删除磁盘中的大量文件（十万级）

Linux下删除海量日志文件，达数十万个文件
使用rm -rf * 性能很差，耗时很长。
可以使用rsync来快速删除大量文件
1.先安装rsync: yum install rsync
2.建立一个空文件夹：mkdir /tmp/test
3.用rsync删除目标目录
    rsync --delete-before -a -H -v --progress --stats  /tmp/empty_dir/ /data/target
    这样我们要删除的log目录就会被清空了，删除的速度会非常快。rsync实际上用的是替换原理，处理数十万个文件也是秒删。

    选项说明:
    –delete-before   接收者在传输之前进行删除操作
    –progress        在传输时显示传输过程
    -a                归档模式，表示以递归方式传输文件，并保持所有文件属性
    -H                保持硬连接的文件
    -v                详细输出模式
    –stats           给出某些文件的传输状态
"""
import os
import subprocess
from loguru import logger
from client.mc import MClient
from concurrent.futures import ThreadPoolExecutor, as_completed


def rm_files_by_rsync(drive_path, bucket):
    """
    通过rsync删除存有大量文件的目录
    :param drive_path:
    :param bucket:
    :return:
    """
    empty_dir = "/tmp/empty_{}".format(bucket)
    target_dir = os.path.join(drive_path, bucket)
    subprocess.getstatusoutput("rm -rf {}".format(empty_dir))
    subprocess.getstatusoutput("mkdir {}".format(empty_dir))
    rsync_cmd = "rsync --delete-before -a -H --progress --stats  {} {}".format(empty_dir, target_dir)
    logger.info(rsync_cmd)
    rc, _ = subprocess.getstatusoutput(rsync_cmd)
    logger.info(f"{target_dir} 删除完成！")
    return rc


def multi_rm_files_by_rsync(
        endpoint, access_key, secret_key, tls, alias,
        bucket_name, max_workers=10
):
    logger.info("初始化Client...")
    client = MClient(endpoint, access_key, secret_key, tls, alias)
    drives_num = client.get_all_buckets()
    bucket_list = client.get_all_buckets()
    futures = set()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for bucket in bucket_list:
            if bucket_name and bucket_name not in bucket:
                continue
            for drive in range(1, drives_num+1):
                drive_path = f"/data/minio{drive}"
                futures.add(executor.submit(rm_files_by_rsync, drive_path, bucket))
    for future in as_completed(futures):
        future.result()


if __name__ == "__main__":
    pass