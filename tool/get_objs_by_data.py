#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:put
@time:2022/09/19
@email:tao.xu2008@outlook.com
@description: 从/data/minio*挂载点的目录名称获取对象 桶/对象名
"""
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger


def get_bucket_list(data_path):
    """
    遍历data path的第一级目录，作为桶名称
    :param data_path:
    :return:
    """
    bucket_list = []
    for d in os.listdir(data_path):
        if d.startswith('.'):
            continue
        if os.path.isdir(os.path.join(data_path, d)):
            bucket_list.append(d)
    return bucket_list


def get_objs_by_data_bucket(data_path, bucket):
    bucket_path = os.path.join(data_path, bucket)
    logger.info(bucket_path)
    for dir_path, dir_names, file_names in os.walk(bucket_path):
        if "xl.meta" in file_names:
            obj_path = dir_path.replace(data_path, "")
            logger.log('OBJ', obj_path)
    logger.info("{} - Complete!".format(bucket_path))


def multi_get_objs_by_data(data_path, max_workers=10):
    bucket_list = get_bucket_list(data_path)
    futures = set()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for bucket in bucket_list:
            futures.add(executor.submit(get_objs_by_data_bucket, data_path, bucket))
    for future in as_completed(futures):
        future.result()

    return


if __name__ == "__main__":
    print(get_bucket_list("D:\\"))
