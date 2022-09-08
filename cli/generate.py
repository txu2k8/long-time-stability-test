#!/usr/bin/python
# -*- coding:utf-8 _*- 
"""
@author:TXU
@file:generator
@time:2022/09/01
@email:tao.xu2008@outlook.com
@description:
"""
from loguru import logger
from typing import List, Text
import typer

from cli.main import app
from config.models import GenFileType


default_size_list = ['0B', '16B', '1KB', '3KB', '4KB', '5KB', '64KB']


@app.command(help='generate local files')
def generate(
        local_path: str = typer.Option(..., help="指定本地文件夹，生产数据存放"),
        file_type: List[GenFileType] = typer.Option([GenFileType.TXT.value], help="生产文件类型"),
        file_size: List[Text] = typer.Option(['1KB'], help="生产文件大小"),
        random_size: bool = typer.Option(False, help="禁用多段上传"),
):
    logger.info("生产{}数据到： {}".format(','.join(file_type), local_path))
    logger.info(file_size)


if __name__ == '__main__':
    pass
