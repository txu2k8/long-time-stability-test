#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:__init__.py
@time:2022/04/03
@email:tao.xu2008@outlook.com
@description: 全局配置，及配置文件读写方法。
"""
from config.globals import *


__version__ = "2.0.2 - 2023-05-28"
__author__ = "tao.xu"

__all__ = [
    "__version__",
    # 基本方法
    "ConfigIni",
    # 全局内存变量-读写
    "set_global_value", "get_global_value", "get_global_dict",
    # 环境变量-读写
    "set_os_environ", "unset_os_environ", "get_os_environment",
    # 全局常量
    "BASE_DIR", "LOG_DIR",  # 全局路径 dir
    "global_cf",
    "DB_SQLITE3",  # 数据库配置
    "TIME_STR",  # 时间戳
    "LOG_LEVEL", "LOG_ROTATION", "LOG_RETENTION",  # 日志配置
]


if __name__ == '__main__':
    pass
