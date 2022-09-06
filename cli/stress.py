#!/usr/bin/python
# -*- coding:utf-8 _*- 
"""
@author:TXU
@file:stress
@time:2022/09/01
@email:tao.xu2008@outlook.com
@description:
"""
import re
import asyncio
import loguru
import typer

from config.models import ClientType
from cli.log import init_logger
from cli.main import app
from stress.put import put_obj


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


@app.command(help='stress get objects')
def get(name: str):
    print(f"Hello {name}")
    init_logger('get')


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
        obj_prefix: str = typer.Option('', help="对象名前缀，默认空，使用编号+原始文件名"),
        obj_num: int = typer.Option(1, min=1, help="对象数（实际上传数=对象数*并行数）"),
        src_path: str = typer.Option(..., help="源文件路径，指定文件夹，随机上传"),
        depth: int = typer.Option(2, min=1, help="桶下面子目录深度，1-代表无子目录"),
        duration: str = typer.Option('', callback=duration_callback, help="持续执行时间，优先级高于对象数，以h、m、s组合，如1h3m10s"),
        client_type: ClientType = typer.Option(ClientType.MC.value, help="选择IO客户端")
):
    init_logger('put')
    loguru.logger.info(client_type)
    # asyncio.run(put_obj('mc', endpoint, access_key, secret_key, tls, alias,
    #                     src_path, bucket_prefix, bucket_num, depth,
    #                     obj_prefix, obj_num, concurrent, disable_multipart, duration)
    #             )
    put_obj('mc', endpoint, access_key, secret_key, tls, alias,
            src_path, bucket_prefix, bucket_num, depth,
            obj_prefix, obj_num, concurrent, disable_multipart, duration)


@app.command(help='stress delete objects')
def delete(name: str, formal: bool = False):
    init_logger('del')
    if formal:
        print(f"Goodbye Ms. {name}. Have a good day.")
    else:
        print(f"Bye {name}!")


if __name__ == "__main__":
    app()

