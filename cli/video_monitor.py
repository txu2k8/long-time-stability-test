#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:video_monitor
@time:2022/09/01
@email:tao.xu2008@outlook.com
@description: 视频监控场景测试 CLI
"""
import sys
from datetime import datetime
from loguru import logger
import typer

from config.models import ClientType, MultipartType
from cli.log import init_logger
from cli.main import app
from core.video_monitor.video_1 import VideoMonitor1


def init_print(case_id, desc, client_types, bucket_num, concurrent):
    logger.log('DESC', '{0}基本信息{0}'.format('*' * 20))
    logger.log('DESC', "测试用例: {}".format(case_id))
    logger.log('DESC', '测试描述：{}'.format(desc))
    logger.log('DESC', '客户端：{}'.format([c.value for c in client_types]))
    logger.log('DESC', '桶数：{}'.format(bucket_num))
    logger.log('DESC', '并行数：{}'.format(concurrent))
    command = 'python3 ' + ' '.join(sys.argv)
    logger.log('DESC', '执行命令：{}'.format(command))
    logger.log('DESC', '执行时间：{}'.format(datetime.now()))
    logger.log('DESC', '*' * 48)


@app.command(help='视频监控场景测试-1')
def video_monitor_1(
        endpoint: str = typer.Option(..., help="例：127.0.0.1:9000 or http://127.0.0.1:9000"),
        access_key: str = typer.Option(..., help="ACCESS_KEY"),
        secret_key: str = typer.Option(..., help="SECRET_KEY"),
        tls: bool = typer.Option(False, help="https传输协议"),
        alias: str = typer.Option('play', help="别名"),
        bucket_prefix: str = typer.Option('bucket', help="桶名称前缀"),
        bucket_num: int = typer.Option(1, min=1, help="桶数量，对象会被均衡写入到各个桶中"),
        multipart: MultipartType = typer.Option(MultipartType.enable.value, help="多段上传"),
        concurrent: int = typer.Option(1, min=1, help="每秒并行数"),
        obj_prefix: str = typer.Option('data', help="对象名前缀"),
        obj_num: int = typer.Option(500, min=1, help="预置对象数，实际均分到每个桶里"),
        obj_num_pd: int = typer.Option(100, min=1, help="每天预置对象数，实际均分到每个桶里"),
        local_path: str = typer.Option(..., help="指定源文件路径，随机上传文件"),
        idx_start: int = typer.Option(0, min=0, help="对象序号起始值"),
        idx_width: int = typer.Option(11, min=1, help="对象序号长度，3=>001"),
        trace: bool = typer.Option(False, help="print TRACE level log"),
        case_id: int = typer.Option(0, min=0, help="测试用例ID，关联到日志文件名"),
        desc: str = typer.Option('', help="测试描述"),
):
    client_types = [ClientType.MC]
    init_logger(prefix='video_monitor_1', case_id=case_id, trace=trace)
    init_print(case_id, desc, client_types, bucket_num, concurrent)

    # 并行执行 - 上传
    vm_obj = VideoMonitor1(
        client_types, endpoint, access_key, secret_key, tls, alias,
        local_path, bucket_prefix, bucket_num, obj_prefix, obj_num, obj_num_pd,
        concurrent, multipart, idx_start, idx_width
    )
    vm_obj.run()
