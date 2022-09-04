#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:main
@time:2022/09/01
@email:tao.xu2008@outlook.com
@description:
"""
import os
import typer
from loguru import logger

from config import TIME_STR, LOG_DIR, LOG_LEVEL, LOG_ROTATION, LOG_RETENTION

app = typer.Typer(help="长稳测试工具集 CLI.")

logger.add(
    os.path.join(LOG_DIR, 'lts-{}.log'.format(TIME_STR)),
    rotation=LOG_ROTATION,  # '100 MB',
    retention=LOG_RETENTION,  # '7 days',
    enqueue=True,
    encoding="utf-8",
    level=LOG_LEVEL
)


if __name__ == '__main__':
    app()
