#!/usr/bin/python
# -*- coding:utf-8 _*- 
"""
@author:TXU
@file:log_init
@time:2022/09/06
@email:tao.xu2008@outlook.com
@description:
"""
import os
import sys
import atexit
from loguru import logger

from config import TIME_STR, LOG_DIR, LOG_ROTATION, LOG_RETENTION, get_global_value


def init_logger(prefix='lts'):
    """
    初始化logger日志配置
    :param prefix:
    :return:
    """
    # 删除默认
    logger.remove()

    # 初始化控制台配置
    loglevel = get_global_value('LOG_LEVEL')
    logger.add(sys.stderr, level=loglevel)

    # 初始化日志配置 -- all日志文件
    logger.add(
        os.path.join(LOG_DIR, '{}-{}-all.log'.format(TIME_STR, prefix)),
        rotation=LOG_ROTATION,  # '100 MB',
        retention=LOG_RETENTION,  # '7 days',
        enqueue=True,
        encoding="utf-8",
        level=loglevel
    )

    # 初始化日志配置 -- error日志文件
    logger.add(
        os.path.join(LOG_DIR, '{}-{}-err.log'.format(TIME_STR, prefix)),
        rotation=LOG_ROTATION,  # '100 MB',
        retention=LOG_RETENTION,  # '7 days',
        enqueue=True,
        encoding="utf-8",
        level='ERROR'
    )

    atexit.register(logger.remove)


if __name__ == '__main__':
    pass
