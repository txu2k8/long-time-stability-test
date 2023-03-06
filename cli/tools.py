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
from workflow.tool.get_objs_by_data import multi_get_objs_by_data
from workflow.tool.rm_objs import multi_rm_objs_by_name, multi_rm_objs_by_bucket
from workflow.tool.rm_trash_files import multi_rm_trash_files


default_size_list = ['0B', '16B', '1KB', '3KB', '4KB', '5KB', '64KB']


@app.command(help='生成本地数据')
def generate(
        data_size: str = typer.Option('4KB', help="指定待生产数据的大小"),
        max_workers: int = typer.Option(1, min=1, help="最大并行数"),
):
    init_logger(prefix='get_data_objs')
    logger.info(data_size)
    # TODO


@app.command(help='从data目录获取对象名')
def get_data_objs(
        data_path: str = typer.Option(..., help="指定data挂在点路径，例如：/data/xxx1/"),
        max_workers: int = typer.Option(1, min=1, help="最大并行数"),
):
    init_logger(prefix='get_data_objs')
    logger.info(data_path)
    multi_get_objs_by_data(data_path, max_workers)


@app.command(help='从data目录获取对象名')
def rm_objs(
        endpoint: str = typer.Option(..., help="例：127.0.0.1:9000 or http://127.0.0.1:9000"),
        access_key: str = typer.Option(..., help="ACCESS_KEY"),
        secret_key: str = typer.Option(..., help="SECRET_KEY"),
        tls: bool = typer.Option(False, help="https传输协议"),
        alias: str = typer.Option('play', help="别名"),
        data_path: str = typer.Option('', help="指定对象名保存文件路径，例如：/root/objs.log"),
        bucket_prefix: str = typer.Option('bucket', help="桶名称前缀，未指定data-path才生效"),
        max_workers: int = typer.Option(1, min=1, help="最大并行数"),
):
    init_logger(prefix='get_data_objs')
    if data_path:
        # 指定了对象名保存文件，则读取文件并删除
        logger.info(data_path)
        multi_rm_objs_by_name(endpoint, access_key, secret_key, tls, alias, data_path, max_workers)
    else:
        # 遍历桶列表，删除所有桶中的对象（指定桶则删除指定桶对象）
        logger.info("遍历桶列表，删除所有桶中的对象（指定桶则删除指定桶对象）")
        multi_rm_objs_by_bucket(endpoint, access_key, secret_key, tls, alias, bucket_prefix, max_workers)


@app.command(help='从data目录删除*/tmp/.trash/*')
def rm_trash(
        data_path_prefix: str = typer.Option('bucket', help="挂载点前缀，例如：/data/xxx"),
        max_workers: int = typer.Option(1, min=1, help="最大并行数"),
        trace: bool = typer.Option(False, help="print TRACE level log"),
):
    init_logger(prefix='rm_trash_files', trace=trace)
    logger.info("遍历挂载点，删除所有目录中的*/tmp/.trash/*")
    multi_rm_trash_files(data_path_prefix, max_workers)


if __name__ == '__main__':
    pass
