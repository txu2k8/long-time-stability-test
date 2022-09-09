#!/usr/bin/python
# -*- coding:utf-8 _*- 
"""
@author:TXU
@file:log_init
@time:2022/09/06
@email:tao.xu2008@outlook.com
@description:
# 日志级别
Level	    value	Logger method
TRACE	    5	    logger.trace
DEBUG	    10	    logger.debug
INFO	    20	    logger.info
SUCCESS	    25	    logger.success
WARNING	    30	    logger.warning
ERROR	    40	    logger.error
CRITICAL	50	    logger.critical

info_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <4}</level> | <level>{message}</level>"
"""
import os
import sys
import atexit
from loguru import logger

from config import TIME_STR, LOG_DIR, LOG_ROTATION, LOG_RETENTION, get_global_value

DEFAULT_FORMAT = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | " \
                 "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> " \
                 "- <level>{message}</level>"
SIMPLE_FORMAT = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <7}</level> | <level>{message}</level>"
OBJECT_FORMAT = "{message}"


def init_logger(prefix='test'):
    """
    初始化logger日志配置
    :param prefix:
    :return:
    """
    # 获取配置
    loglevel = get_global_value('LOG_LEVEL')
    spec_format = DEFAULT_FORMAT if loglevel == 'TRACE' else SIMPLE_FORMAT

    # 删除默认
    logger.remove()

    # 新增级别
    logger.level('MC', no=21, color='<blue><bold>')  # INFO < MC < ERROR
    logger.level('S3CMD', no=22, color='<blue><bold>')  # INFO < S3CMD < ERROR
    logger.level('OBJ', no=51)  # CRITICAL < OBJ，打印操作的对象列表

    # 初始化控制台配置
    logger.add(sys.stderr, level=loglevel, format=spec_format)

    # 初始化日志配置 -- all日志文件
    logger.add(
        os.path.join(LOG_DIR, '{}-{}-all.log'.format(TIME_STR, prefix)),
        rotation=LOG_ROTATION,  # '100 MB',
        retention=LOG_RETENTION,  # '7 days',
        enqueue=True,
        encoding="utf-8",
        level=loglevel,
        format=spec_format,
        backtrace=True,
        diagnose=True
    )

    # 初始化日志配置 -- 记录对象列表
    if prefix in ['put', 'delete']:
        logger.add(
            os.path.join(LOG_DIR, '{}-{}-obj.log'.format(TIME_STR, prefix)),
            rotation=LOG_ROTATION,  # '100 MB',
            retention=LOG_RETENTION,  # '7 days',
            enqueue=True,
            encoding="utf-8",
            level='OBJ',
            format=OBJECT_FORMAT
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
