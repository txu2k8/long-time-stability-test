#!/usr/bin/python
# -*- coding:utf-8 _*- 
"""
@author:TXU
@file:stress
@time:2022/09/01
@email:tao.xu2008@outlook.com
@description: 压力测试 - put/get/list/delete ...
"""
import re
import asyncio
from typing import List
import loguru
import typer

from config.models import ClientType
from cli.log import init_logger
from cli.main import app
from stress.put import PutObject
from stress.put_del import PutDeleteObject
from stress.get import GetObject


def duration_callback(ctx: typer.Context, param: typer.CallbackParam, value: str):
    second = 0
    if ctx.resilient_parsing:
        return
    if not value:
        return second
    else:
        try:
            hms = re.findall(r'-?[0-9]\d*', value)
            h, m, s = hms
            second = int(h) * 3600 + int(m) * 60 + int(s)
        except Exception as e:
            raise typer.BadParameter("duration参数格式错误，必需以h、m、s组合，如1h3m10s")

    return second


@app.command(help='stress put objects')
def put(
        endpoint: str = typer.Option(..., help="例：127.0.0.1:9000 or http://127.0.0.1:9000"),
        access_key: str = typer.Option(..., help="ACCESS_KEY"),
        secret_key: str = typer.Option(..., help="SECRET_KEY"),
        tls: bool = typer.Option(False, help="https传输协议"),
        alias: str = typer.Option('play', help="别名"),
        bucket_prefix: str = typer.Option('bucket', help="桶名称前缀"),
        bucket_num: int = typer.Option(1, min=1, help="桶数量，对象会被均衡分配到各个桶中"),
        disable_multipart: bool = typer.Option(False, help="禁用多段上传"),
        concurrent: int = typer.Option(1, min=1, help="每秒并行数"),
        obj_prefix: str = typer.Option('data', help="对象名前缀，默认空，使用编号+原始文件名"),
        obj_num: int = typer.Option(1, min=1, help="对象数"),
        local_path: str = typer.Option(..., help="源文件路径，指定文件夹，随机上传"),
        depth: int = typer.Option(2, min=1, help="桶下面子目录深度，1-代表无子目录"),
        duration: str = typer.Option('', callback=duration_callback, help="持续执行时间，优先级高于对象数，以h、m、s组合，如1h3m10s"),
        client_type: List[ClientType] = typer.Option([ClientType.MC.value], help="选择IO客户端")
):
    init_logger('put')
    loguru.logger.info(client_type)

    # 并行执行
    put_obj = PutObject(
        client_type, endpoint, access_key, secret_key, tls, alias,
        local_path, bucket_prefix, bucket_num, depth,
        obj_prefix, obj_num, concurrent, disable_multipart, duration
    )
    put_obj.prepare()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(put_obj.run())
    loop.close()


@app.command(help='stress put-del objects')
def put_del(
        endpoint: str = typer.Option(..., help="例：127.0.0.1:9000 or http://127.0.0.1:9000"),
        access_key: str = typer.Option(..., help="ACCESS_KEY"),
        secret_key: str = typer.Option(..., help="SECRET_KEY"),
        tls: bool = typer.Option(False, help="https传输协议"),
        alias: str = typer.Option('play', help="别名"),
        bucket_prefix: str = typer.Option('bucket', help="桶名称前缀"),
        bucket_num: int = typer.Option(1, min=1, help="桶数量，对象会被均衡分配到各个桶中"),
        disable_multipart: bool = typer.Option(False, help="禁用多段上传"),
        concurrent: int = typer.Option(1, min=1, help="每秒并行数"),
        obj_prefix: str = typer.Option('data', help="对象名前缀，默认空，使用编号+原始文件名"),
        obj_num: int = typer.Option(1, min=1, help="对象数"),
        local_path: str = typer.Option(..., help="源文件路径，指定文件夹，随机上传"),
        depth: int = typer.Option(2, min=1, help="桶下面子目录深度，1-代表无子目录"),
        duration: str = typer.Option('', callback=duration_callback, help="持续执行时间，优先级高于对象数，以h、m、s组合，如1h3m10s"),
        client_type: List[ClientType] = typer.Option([ClientType.MC.value], help="选择IO客户端")
):
    init_logger('put-del')
    loguru.logger.info(client_type)

    # 并行执行
    put_del_obj = PutDeleteObject(
        client_type, endpoint, access_key, secret_key, tls, alias,
        local_path, bucket_prefix, bucket_num, depth,
        obj_prefix, obj_num, concurrent, disable_multipart, duration
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(put_del_obj.run())
    loop.close()


@app.command(help='stress get objects')
def get(
        endpoint: str = typer.Option(..., help="例：127.0.0.1:9000 or http://127.0.0.1:9000"),
        access_key: str = typer.Option(..., help="ACCESS_KEY"),
        secret_key: str = typer.Option(..., help="SECRET_KEY"),
        tls: bool = typer.Option(False, help="https传输协议"),
        alias: str = typer.Option('play', help="别名"),
        bucket_prefix: str = typer.Option('bucket', help="桶名称前缀"),
        bucket_num: int = typer.Option(1, min=1, help="桶数量，对象会被均衡分配到各个桶中"),
        disable_multipart: bool = typer.Option(False, help="禁用多段上传"),
        concurrent: int = typer.Option(1, min=1, help="每秒并行数"),
        obj_prefix: str = typer.Option('data', help="对象名前缀，默认空，使用编号+原始文件名"),
        obj_num: int = typer.Option(1, min=1, help="对象数"),
        local_path: str = typer.Option(..., help="下载到本地的文件路径"),
        depth: int = typer.Option(2, min=1, help="桶下面子目录深度，1-代表无子目录"),
        duration: str = typer.Option('', callback=duration_callback, help="持续执行时间，优先级高于对象数，以h、m、s组合，如1h3m10s"),
        client_type: List[ClientType] = typer.Option([ClientType.MC.value], help="选择IO客户端")
):
    init_logger('get')
    loguru.logger.info(client_type)

    # 并行执行
    get_obj = GetObject(
        client_type, endpoint, access_key, secret_key, tls, alias,
        local_path, bucket_prefix, bucket_num, depth,
        obj_prefix, obj_num, concurrent, disable_multipart, duration
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_obj.run())
    loop.close()


@app.command(help='stress delete objects')
def delete(name: str, formal: bool = False):
    init_logger('del')
    if formal:
        print(f"Goodbye Ms. {name}. Have a good day.")
    else:
        print(f"Bye {name}!")


if __name__ == "__main__":
    app()

