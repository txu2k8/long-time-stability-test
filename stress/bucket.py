#!/usr/bin/python
# -*- coding:utf-8 _*- 
"""
@author:TXU
@file:bucket
@time:2022/09/07
@email:tao.xu2008@outlook.com
@description:
"""
from loguru import logger


def generate_bucket_name(bucket_prefix, idx):
    return '{}{}'.format(bucket_prefix, idx)


def make_bucket_if_not_exist(client, bucket_prefix, bucket_num):
    logger.info("创建bucket（如果不存在）...")
    for idx in range(bucket_num):
        bucket = generate_bucket_name(bucket_prefix, idx)
        client.mb(bucket)


if __name__ == '__main__':
    pass
