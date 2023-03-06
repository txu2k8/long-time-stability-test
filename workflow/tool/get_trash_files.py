#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:get_trash_files
@time:2022/09/19
@email:tao.xu2008@outlook.com
@description: 删除*/tmp/.trash/目录文件
"""
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger


def get_parent_dir_list(data_path_prefix):
    """
    遍历/data/* 目录
    :param data_path_prefix:
    :return:
    """
    dir_list = []
    rc, outpot = subprocess.getstatusoutput("df -h | grep {}".format(data_path_prefix))
    ds = outpot.strip("\n").split(" ")
    parent_dir = ds[-1]
    if data_path_prefix in parent_dir:
        dir_list.append(parent_dir)

    return dir_list


def get_trash_files_by_parent_dir(parent_dir):
    trash_path = os.path.join(parent_dir, ".ubiscale.sys/tmp/.trash")
    logger.info(trash_path)
    for dir_path, dir_names, file_names in os.walk(trash_path):
        for dir_name in dir_names:
            obj_path = os.path.join(trash_path, dir_name)
            logger.log('OBJ', obj_path)
    logger.info("{} - Complete!".format(parent_dir))


def multi_get_trash_files(data_path_prefix, max_workers=10):
    parent_dir_list = get_parent_dir_list(data_path_prefix)
    futures = set()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for parent_dir in parent_dir_list:
            futures.add(executor.submit(get_trash_files_by_parent_dir, parent_dir))
    for future in as_completed(futures):
        future.result()
    return


if __name__ == "__main__":
    print(multi_get_trash_files("D:\\"))
