#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:get_objs_by_data
@time:2022/09/19
@email:tao.xu2008@outlook.com
@description: 基于桶 删除桶中所有对象
"""
from loguru import logger
from client.mc import MClient
from concurrent.futures import ThreadPoolExecutor, as_completed


def rm_objs_by_bucket_until_done(client: MClient, bucket):
    loop = 0
    while True:
        loop += 1
        logger.info("删除桶中对象： {}/{}， Loop={}".format(client.alias, bucket, loop))
        rc, output = client.delete_bucket_objs(bucket)
        if rc == 0 or "The specified bucket does not exist" in output:
            logger.info("删除桶中所有对象完成： {}/{}".format(client.alias, bucket))
            break


def multi_rm_objs_by_bucket(
        endpoint, access_key, secret_key, tls, alias,
        bucket_name, max_workers=10
):
    logger.info("初始化工具Client...")
    client = MClient(endpoint, access_key, secret_key, tls, alias)
    bucket_list = client.get_all_buckets()
    futures = set()
    with ThreadPoolExecutor(max_workers=max_workers) as exector:
        for bucket in bucket_list:
            if bucket_name and bucket_name not in bucket:
                continue
            futures.add(exector.submit(rm_objs_by_bucket_until_done, client, bucket))
    for future in as_completed(futures):
        future.result()


def multi_rm_objs_by_name(
        endpoint, access_key, secret_key, tls, alias,
        obj_names_file, max_workers=100
):
    logger.info("初始化工具Client...")
    client = MClient(endpoint, access_key, secret_key, tls, alias)

    futures = set()
    with ThreadPoolExecutor(max_workers=max_workers) as exector:
        with open(obj_names_file, 'r') as fb:
            while True:
                obj_name = fb.readline()
                if obj_name == "\n":
                    continue
                if obj_name == "":
                    break
                obj_name_split = obj_name.rstrip("/").split("/")
                bucket = obj_name_split[0]
                obj_path = '/'.join(obj_name_split[1:])
                futures.add(exector.submit(client.delete, bucket, obj_path))
    for future in as_completed(futures):
        future.result()
