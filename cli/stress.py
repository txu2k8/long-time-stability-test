#!/usr/bin/python
# -*- coding:utf-8 _*- 
"""
@author:TXU
@file:stress
@time:2022/09/01
@email:tao.xu2008@outlook.com
@description:
"""
import loguru
import typer

from config.models import ClientType
from cli.main import app
from stress.put import put_obj


@app.command(help='stress get objects')
def get(name: str):
    print(f"Hello {name}")


@app.command(help='stress put objects')
def put(
        endpoint: str = '',
        access_key: str = '',
        secret_key: str = '',
        tls: bool = False,
        bucket: str = '',
        bucket_num: int = typer.Option(1, min=1),
        disable_multipart: bool = False,
        concurrent: int = typer.Option(1, min=1),
        md5: bool = True,
        obj_prefix: str = '',
        obj_noprefix: bool = False,
        obj_size: str = '',
        obj_num: str = '',
        obj_generator: str = '',
        obj_randsize: str = '',
        duration: str = '',
        clear: bool = typer.Option(False, help="clear all data"),
        client_type: ClientType = typer.Option(ClientType.MC.value, help="Select the IO client")
):
    loguru.logger.info(client_type)
    put_obj('mc', "http://127.0.0.1:9000", 'minioadmin', 'minioadmin', False, 'play',
        'bucket', 1,
        'D:\\minio\\upload_data', 3, 2, 2)


@app.command(help='stress delete objects')
def delete(name: str, formal: bool = False):
    if formal:
        print(f"Goodbye Ms. {name}. Have a good day.")
    else:
        print(f"Bye {name}!")


if __name__ == "__main__":
    app()

