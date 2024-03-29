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

from config import TIME_STR, LOG_DIR, LOG_LEVEL, LOG_ROTATION, LOG_RETENTION

DEFAULT_FORMAT = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <7}</level> | " \
                 "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> " \
                 "- <level>{message}</level>"
SIMPLE_FORMAT = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <7}</level> | <level>{message}</level>"
OBJECT_FORMAT = "{message}"
TRACE_FORMAT = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <7}</level> | " \
                 "<cyan>P{process}</cyan>:<cyan>T{thread}</cyan>:<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> " \
                 "- <level>{message}</level>"


def init_logger(prefix='test', case_id=0, trace=False):
    """
    初始化logger日志配置
    :param prefix:
    :param case_id:测试用例ID，作为文件名的一部分
    :param trace: 是否打印trace信息
    :return:
    """
    # 获取配置
    loglevel = 'TRACE' if trace else LOG_LEVEL
    spec_format = TRACE_FORMAT if loglevel == 'TRACE' else SIMPLE_FORMAT

    # 删除默认
    logger.remove()

    # 新增级别
    logger.level('STAGE', no=21, color='<blue><bold>')  # INFO < STAGE < ERROR
    logger.level('MC', no=22, color='<blue><bold>')  # INFO < MC < ERROR
    logger.level('S3CMD', no=23, color='<blue><bold>')  # INFO < S3CMD < ERROR
    logger.level('OBJ', no=51)  # CRITICAL < OBJ，打印操作的对象列表
    logger.level('DESC', no=52)  # CRITICAL < DESC，打印描述信息到所有日志文件

    # 初始化控制台配置
    if trace:
        logger.add(sys.stderr, level=loglevel, format=spec_format, filter=lambda x: "OBJ" not in str(x['level']).upper())

    # 日志文件名处理
    logfile_prefix = '{}_{}'.format(TIME_STR, prefix)
    if case_id > 1:
        logfile_prefix += '_tc{}'.format(case_id)

    logger.info(LOG_DIR)
    # 初始化日志配置 -- all日志文件
    logger.add(
        os.path.join(LOG_DIR, '{}_all.log'.format(logfile_prefix)),
        # os.path.join(LOG_DIR, '{time}'+'_{prefix}_all.log'.format(prefix=prefix)),
        rotation=LOG_ROTATION,  # '100 MB',
        retention=LOG_RETENTION,  # '30 days',
        enqueue=True,
        encoding="utf-8",
        level=loglevel,
        format=spec_format,
        filter=lambda x: "OBJ" not in str(x['level']).upper(),
        backtrace=True,
        diagnose=True
    )

    # 初始化日志配置 -- 记录对象列表
    logger.add(
        os.path.join(LOG_DIR, '{}_obj.log'.format(logfile_prefix)),
        rotation=LOG_ROTATION,  # '100 MB',
        retention=LOG_RETENTION,  # '30 days',
        enqueue=True,
        encoding="utf-8",
        level='OBJ',
        format=OBJECT_FORMAT
    )

    # 初始化日志配置 -- error日志文件
    logger.add(
        os.path.join(LOG_DIR, '{}_err.log'.format(logfile_prefix)),
        rotation=LOG_ROTATION,  # '100 MB',
        retention=LOG_RETENTION,  # '30 days',
        enqueue=True,
        encoding="utf-8",
        level='ERROR',
        format=TRACE_FORMAT,
        filter=lambda x: "OBJ" not in str(x['level']).upper()
    )

    atexit.register(logger.remove)


if __name__ == '__main__':
    pass
