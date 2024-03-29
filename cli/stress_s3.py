#!/usr/bin/python
# -*- coding:utf-8 _*- 
"""
@author:TXU
@file:stress
@time:2022/09/01
@email:tao.xu2008@outlook.com
@description: 压力测试 - put/get/list/delete ...
"""
import sys
import re
from datetime import datetime
from typing import List
from loguru import logger
import typer

from config.models import ClientType, MultipartType
from cli.log import init_logger
from cli.main import app
from workflow.stress.s3.put import PutObject
from workflow.stress.s3.put_del import PutDeleteObject
from workflow.stress.s3.get import GetObject
from workflow.stress.s3.delete import DeleteObject
from workflow.stress.s3.list import ListObject


def init_print(case_id, desc, client_type, bucket_num, obj_num, concurrent, duration):
    logger.log('DESC', '{0}基本信息{0}'.format('*' * 20))
    logger.log('DESC', "测试用例: {}".format(case_id))
    logger.log('DESC', '测试描述：{}'.format(desc))
    logger.log('DESC', '客户端：{}'.format([x.value for x in client_type]))
    logger.log('DESC', '桶数：{}'.format(bucket_num))
    logger.log('DESC', '对象数：{}'.format(obj_num))
    if not duration:
        logger.log('DESC', '对象总数（对象数*桶数）：{}'.format(obj_num*bucket_num))
    logger.log('DESC', '并行数：{}'.format(concurrent))
    logger.log('DESC', '持续时间：{}'.format(duration))
    command = 'python3 ' + ' '.join(sys.argv)
    logger.log('DESC', '执行命令：{}'.format(command))
    logger.log('DESC', '执行时间：{}'.format(datetime.now()))
    logger.log('DESC', '*' * 48)


def duration_callback(ctx: typer.Context, param: typer.CallbackParam, value: str):
    second = 0
    if ctx.resilient_parsing:
        return
    if not value:
        return second
    else:
        try:
            dhms = re.findall(r'-?[0-9]\d*', value)
            d, h, m, s = dhms
            second = int(d) * 86400 + int(h) * 3600 + int(m) * 60 + int(s)
        except Exception as e:
            raise typer.BadParameter("duration参数格式错误，必需以d、h、m、s组合，如0d1h3m10s")

    return second


@app.command(help='stress put objects')
def put(
        endpoint: str = typer.Option(..., help="例：127.0.0.1:9000 or http://127.0.0.1:9000"),
        access_key: str = typer.Option(..., help="ACCESS_KEY"),
        secret_key: str = typer.Option(..., help="SECRET_KEY"),
        tls: bool = typer.Option(False, help="https传输协议"),
        alias: str = typer.Option('play', help="别名"),
        bucket_prefix: str = typer.Option('bucket', help="桶名称前缀"),
        bucket_num: int = typer.Option(1, min=1, help="桶数量，对象会被均衡写入到各个桶中"),
        obj_prefix: str = typer.Option('data', help="对象名前缀"),
        obj_num: int = typer.Option(50, min=1, help="预置对象数，实际均分到每个桶里"),
        multipart: MultipartType = typer.Option(MultipartType.enable.value, help="多段上传"),
        local_path: str = typer.Option(..., help="指定源文件路径，随机上传文件"),
        concurrent: int = typer.Option(1, min=1, help="每秒并行数"),
        prepare_concurrent: int = typer.Option(1, min=1, help="预置数据时每秒并行数"),
        idx_width: int = typer.Option(11, min=1, help="对象序号长度，3=>001"),
        idx_put_start: int = typer.Option(1, min=1, help="上传对象序号起始值"),
        idx_del_start: int = typer.Option(1, min=1, help="删除对象序号起始值"),
        depth: int = typer.Option(1, min=1, help="桶下面子目录深度，1-代表无子目录"),
        duration: str = typer.Option('', callback=duration_callback, help="持续执行时间，优先级高于对象数，以h、m、s组合，如1h3m10s"),
        cover: bool = typer.Option(False, help="覆盖处理，仅适用于duration>0的情况"),
        client_types: List[ClientType] = typer.Option([ClientType.MC.value], help="选择IO客户端"),
        trace: bool = typer.Option(False, help="print TRACE level log"),
        case_id: int = typer.Option(0, min=0, help="测试用例ID，关联到日志文件名"),
        desc: str = typer.Option('', help="测试描述"),
):
    init_logger(prefix='put', case_id=case_id, trace=trace)
    init_print(case_id, desc, client_types, bucket_num, obj_num, concurrent, duration)

    # 并行执行 - 上传
    put_obj = PutObject(
        client_types, endpoint, access_key, secret_key, tls, alias,
        bucket_prefix, bucket_num, obj_prefix, obj_num, multipart, local_path,
        concurrent, prepare_concurrent, idx_width, idx_put_start, idx_del_start,
        depth, duration, cover
    )
    put_obj.run()


@app.command(help='stress put-del objects')
def put_del(
        endpoint: str = typer.Option(..., help="例：127.0.0.1:9000 or http://127.0.0.1:9000"),
        access_key: str = typer.Option(..., help="ACCESS_KEY"),
        secret_key: str = typer.Option(..., help="SECRET_KEY"),
        tls: bool = typer.Option(False, help="https传输协议"),
        alias: str = typer.Option('play', help="别名"),
        bucket_prefix: str = typer.Option('bucket', help="桶名称前缀"),
        bucket_num: int = typer.Option(1, min=1, help="桶数量，对象会被均衡写入到各个桶中"),
        obj_prefix: str = typer.Option('data', help="对象名前缀"),
        obj_num: int = typer.Option(50, min=1, help="预置对象数，实际均分到每个桶里"),
        multipart: MultipartType = typer.Option(MultipartType.enable.value, help="多段上传"),
        local_path: str = typer.Option(..., help="指定源文件路径，随机上传文件"),
        concurrent: int = typer.Option(1, min=1, help="每秒并行数"),
        prepare_concurrent: int = typer.Option(1, min=1, help="预置数据时每秒并行数"),
        idx_width: int = typer.Option(11, min=1, help="对象序号长度，3=>001"),
        idx_put_start: int = typer.Option(1, min=1, help="上传对象序号起始值"),
        idx_del_start: int = typer.Option(1, min=1, help="删除对象序号起始值"),
        depth: int = typer.Option(1, min=1, help="桶下面子目录深度，1-代表无子目录"),
        duration: str = typer.Option('', callback=duration_callback, help="持续执行时间，优先级高于对象数，以h、m、s组合，如1h3m10s"),
        cover: bool = typer.Option(False, help="覆盖处理，仅适用于duration>0的情况"),
        client_types: List[ClientType] = typer.Option([ClientType.MC.value], help="选择IO客户端"),
        trace: bool = typer.Option(False, help="print TRACE level log"),
        case_id: int = typer.Option(0, min=0, help="测试用例ID，关联到日志文件名"),
        desc: str = typer.Option('', help="测试描述"),
):
    init_logger(prefix='put-del', case_id=case_id, trace=trace)
    init_print(case_id, desc, client_types, bucket_num, obj_num, concurrent, duration)

    # 并行执行 - 删除-》上传
    put_del_obj = PutDeleteObject(
        client_types, endpoint, access_key, secret_key, tls, alias,
        bucket_prefix, bucket_num, obj_prefix, obj_num, multipart, local_path,
        concurrent, prepare_concurrent, idx_width, idx_put_start, idx_del_start,
        depth, duration, cover
    )
    put_del_obj.run()


@app.command(help='stress get objects')
def get(
        endpoint: str = typer.Option(..., help="例：127.0.0.1:9000 or http://127.0.0.1:9000"),
        access_key: str = typer.Option(..., help="ACCESS_KEY"),
        secret_key: str = typer.Option(..., help="SECRET_KEY"),
        tls: bool = typer.Option(False, help="https传输协议"),
        alias: str = typer.Option('play', help="别名"),
        bucket_prefix: str = typer.Option('bucket', help="桶名称前缀"),
        bucket_num: int = typer.Option(1, min=1, help="桶数量，对象会被均衡写入到各个桶中"),
        obj_prefix: str = typer.Option('data', help="对象名前缀"),
        obj_num: int = typer.Option(50, min=1, help="预置对象数，实际均分到每个桶里"),
        multipart: MultipartType = typer.Option(MultipartType.enable.value, help="多段上传"),
        local_path: str = typer.Option(..., help="指定本地路径，存放下载文件"),
        concurrent: int = typer.Option(1, min=1, help="每秒并行数"),
        prepare_concurrent: int = typer.Option(1, min=1, help="预置数据时每秒并行数"),
        idx_width: int = typer.Option(11, min=1, help="对象序号长度，3=>001"),
        idx_put_start: int = typer.Option(1, min=1, help="上传对象序号起始值"),
        idx_del_start: int = typer.Option(1, min=1, help="删除对象序号起始值"),
        depth: int = typer.Option(1, min=1, help="桶下面子目录深度，1-代表无子目录"),
        duration: str = typer.Option('', callback=duration_callback, help="持续执行时间，优先级高于对象数，以h、m、s组合，如1h3m10s"),
        cover: bool = typer.Option(False, help="覆盖处理，仅适用于duration>0的情况"),
        client_types: List[ClientType] = typer.Option([ClientType.MC.value], help="选择IO客户端"),
        trace: bool = typer.Option(False, help="print TRACE level log"),
        case_id: int = typer.Option(0, min=0, help="测试用例ID，关联到日志文件名"),
        desc: str = typer.Option('', help="测试描述"),
):
    init_logger(prefix='get', case_id=case_id, trace=trace)
    init_print(case_id, desc, client_types, bucket_num, obj_num, concurrent, duration)

    # 并行执行 - 下载
    get_obj = GetObject(
        client_types, endpoint, access_key, secret_key, tls, alias,
        bucket_prefix, bucket_num, obj_prefix, obj_num, multipart, local_path,
        concurrent, prepare_concurrent, idx_width, idx_put_start, idx_del_start,
        depth, duration, cover
    )
    get_obj.run()


@app.command(help='stress delete objects')
def delete(
        endpoint: str = typer.Option(..., help="例：127.0.0.1:9000 or http://127.0.0.1:9000"),
        access_key: str = typer.Option(..., help="ACCESS_KEY"),
        secret_key: str = typer.Option(..., help="SECRET_KEY"),
        tls: bool = typer.Option(False, help="https传输协议"),
        alias: str = typer.Option('play', help="别名"),
        bucket_prefix: str = typer.Option('bucket', help="桶名称前缀"),
        bucket_num: int = typer.Option(1, min=1, help="桶数量，对象会被均衡写入到各个桶中"),
        obj_prefix: str = typer.Option('data', help="对象名前缀"),
        obj_num: int = typer.Option(50, min=1, help="预置对象数，实际均分到每个桶里"),
        # multipart: MultipartType = typer.Option(MultipartType.enable.value, help="多段上传"),
        # local_path: str = typer.Option(..., help="指定源文件路径，随机上传文件"),
        concurrent: int = typer.Option(1, min=1, help="每秒并行数"),
        prepare_concurrent: int = typer.Option(1, min=1, help="预置数据时每秒并行数"),
        idx_width: int = typer.Option(11, min=1, help="对象序号长度，3=>001"),
        idx_put_start: int = typer.Option(1, min=1, help="上传对象序号起始值"),
        idx_del_start: int = typer.Option(1, min=1, help="删除对象序号起始值"),
        depth: int = typer.Option(1, min=1, help="桶下面子目录深度，1-代表无子目录"),
        duration: str = typer.Option('', callback=duration_callback, help="持续执行时间，优先级高于对象数，以h、m、s组合，如1h3m10s"),
        cover: bool = typer.Option(False, help="覆盖处理，仅适用于duration>0的情况"),
        client_types: List[ClientType] = typer.Option([ClientType.MC.value], help="选择IO客户端"),
        trace: bool = typer.Option(False, help="print TRACE level log"),
        case_id: int = typer.Option(0, min=0, help="测试用例ID，关联到日志文件名"),
        desc: str = typer.Option('', help="测试描述"),
):
    init_logger(prefix='delete', case_id=case_id, trace=trace)
    init_print(case_id, desc, client_types, bucket_num, obj_num, concurrent, duration)

    # 并行执行 - 删除
    delete_obj = DeleteObject(
        client_types, endpoint, access_key, secret_key, tls, alias,
        bucket_prefix, bucket_num, obj_prefix, obj_num, False, "",
        concurrent, prepare_concurrent, idx_width, idx_put_start, idx_del_start,
        depth, duration, cover
    )
    delete_obj.run()


@app.command(help='stress list objects')
def list(
        endpoint: str = typer.Option(..., help="例：127.0.0.1:9000 or http://127.0.0.1:9000"),
        access_key: str = typer.Option(..., help="ACCESS_KEY"),
        secret_key: str = typer.Option(..., help="SECRET_KEY"),
        tls: bool = typer.Option(False, help="https传输协议"),
        alias: str = typer.Option('play', help="别名"),
        bucket_prefix: str = typer.Option('bucket', help="桶名称前缀"),
        bucket_num: int = typer.Option(1, min=1, help="桶数量，对象会被均衡写入到各个桶中"),
        obj_prefix: str = typer.Option('data', help="对象名前缀"),
        obj_num: int = typer.Option(50, min=1, help="预置对象数，实际均分到每个桶里"),
        # multipart: MultipartType = typer.Option(MultipartType.enable.value, help="多段上传"),
        # local_path: str = typer.Option(..., help="指定源文件路径，随机上传文件"),
        concurrent: int = typer.Option(1, min=1, help="每秒并行数"),
        prepare_concurrent: int = typer.Option(1, min=1, help="预置数据时每秒并行数"),
        idx_width: int = typer.Option(11, min=1, help="对象序号长度，3=>001"),
        idx_put_start: int = typer.Option(1, min=1, help="上传对象序号起始值"),
        idx_del_start: int = typer.Option(1, min=1, help="删除对象序号起始值"),
        depth: int = typer.Option(1, min=1, help="桶下面子目录深度，1-代表无子目录"),
        duration: str = typer.Option('', callback=duration_callback, help="持续执行时间，优先级高于对象数，以h、m、s组合，如1h3m10s"),
        cover: bool = typer.Option(False, help="覆盖处理，仅适用于duration>0的情况"),
        client_types: List[ClientType] = typer.Option([ClientType.MC.value], help="选择IO客户端"),
        trace: bool = typer.Option(False, help="print TRACE level log"),
        case_id: int = typer.Option(0, min=0, help="测试用例ID，关联到日志文件名"),
        desc: str = typer.Option('', help="测试描述"),
):
    init_logger(prefix='list', case_id=case_id, trace=trace)
    init_print(case_id, desc, client_types, bucket_num, obj_num, concurrent, duration)

    # 并行执行 - 列表对象
    ls_obj = ListObject(
        client_types, endpoint, access_key, secret_key, tls, alias,
        bucket_prefix, bucket_num, obj_prefix, obj_num, False, "",
        concurrent, prepare_concurrent, idx_width, idx_put_start, idx_del_start,
        depth, duration, cover
    )
    ls_obj.run()


if __name__ == "__main__":
    app()
