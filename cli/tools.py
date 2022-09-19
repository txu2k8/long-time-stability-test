#!/usr/bin/python
# -*- coding:utf-8 _*- 
"""
@author:TXU
@file:tools
@time:2022/09/01
@email:tao.xu2008@outlook.com
@description:
"""
from loguru import logger
import typer

from cli.main import app
from cli.log import init_logger
from tool.get_objs_by_data import multi_get_objs_by_data


default_size_list = ['0B', '16B', '1KB', '3KB', '4KB', '5KB', '64KB']


@app.command(help='从data目录获取对象')
def get_data_objs(
        data_path: str = typer.Option(..., help="指定data挂在点路径，例如：/data/xxx1/"),
        max_workers: int = typer.Option(1, min=1, help="最大并行数"),
):
    init_logger(prefix='get_data_objs')
    logger.info(data_path)
    multi_get_objs_by_data(data_path, max_workers)


if __name__ == '__main__':
    pass
