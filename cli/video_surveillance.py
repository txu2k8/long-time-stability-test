#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author:TXU
@file:video_surveillance
@time:2022/12/12
@email:tao.xu2008@outlook.com
@description: 视频监控存储 - 数据模型模拟测试
"""

import sys
from datetime import datetime
from loguru import logger
import typer

from config.models import ClientType, MultipartType
from cli.log import init_logger
from cli.main import app
from workflow.video_surveillance.vs_calculate import VSCalc
from workflow.video_monitor.video_3 import VideoMonitor3


def init_print(case_id, desc, client_types, video_channel, video_stream, multipart, max_workers):
    logger.log('DESC', '{0}基本信息{0}'.format('*' * 20))
    logger.log('DESC', "测试用例: {}".format(case_id))
    logger.log('DESC', '测试描述：{}'.format(desc))
    logger.log('DESC', '客户端：{}'.format([c.value for c in client_types]))
    logger.log('DESC', '视频路数：{}'.format(video_channel))
    logger.log('DESC', '视频码流：{}'.format(video_stream))
    logger.log('DESC', '多段上传'.format(multipart))
    logger.log('DESC', '最大并发数'.format(max_workers))
    command = 'python3 ' + ' '.join(sys.argv)
    logger.log('DESC', '执行命令：{}'.format(command))
    logger.log('DESC', '执行时间：{}'.format(datetime.now()))
    logger.log('DESC', '*' * 48)


@app.command(help='视频监控场景测试-1: 写删均衡', hidden=False)
def video_surveillance_1(
        # 存储 环境信息
        endpoint: str = typer.Option(..., help="环境信息：例 127.0.0.1:9000 or http://127.0.0.1:9000"),
        access_key: str = typer.Option(..., help="环境信息：ACCESS_KEY"),
        secret_key: str = typer.Option(..., help="环境信息：SECRET_KEY"),
        tls: bool = typer.Option(False, help="环境信息：https传输协议"),
        alias: str = typer.Option('play', help="环境信息：别名"),

        # 视频监控 业务模型
        channel_num: int = typer.Option(1, min=1, help="业务模型：视频路数"),
        bitstream: int = typer.Option(4, min=1, help="业务模型：视频码流（单位：Mbps）"),
        capacity: int = typer.Option(..., min=1, help="业务模型：可用空间（单位：MB）"),
        local_path: str = typer.Option(..., help="业务模型：指定源文件路径，随机上传文件"),
        multipart: MultipartType = typer.Option(MultipartType.enable.value, help="业务模型：多段上传"),
        max_workers: int = typer.Option(1000, min=1, help="业务模型：写删阶段最大并发数"),
        prepare_channel_num: int = typer.Option(0, min=0, help="业务模型：预置阶段,视频写入路数,默认=channel_num"),
        obj_size: int = typer.Option(1, min=1, help="业务模型：对象大小,默认128MB"),

        # 自定义设置
        bucket_prefix: str = typer.Option('bucket', help="自定义：桶名称前缀"),
        obj_prefix: str = typer.Option('data', help="自定义：对象名前缀"),
        idx_width: int = typer.Option(11, min=1, help="自定义：对象序号长度，3=>001"),
        idx_put_start: int = typer.Option(1, min=1, help="自定义：上传对象序号起始值"),
        idx_del_start: int = typer.Option(1, min=1, help="自定义：删除对象序号起始值"),
        prepare_concurrent: int = typer.Option(1, min=1, help="自定义：预置数据时每秒并行数"),

        # 其他
        trace: bool = typer.Option(False, help="print TRACE level log"),
        case_id: int = typer.Option(0, min=0, help="测试用例ID，关联到日志文件名"),
        desc: str = typer.Option('', help="测试描述"),
):
    client_types = [ClientType.MC]
    init_logger(prefix='video_3', case_id=case_id, trace=trace)
    init_print(case_id, desc, client_types, channel_num, bitstream, multipart, max_workers)

    vs_calc = VSCalc(channel_num, bitstream, capacity, prepare_channel_num)
    vs_calc.vs_info.obj_size = obj_size
    vs_calc.vs_info.idx_width = idx_width
    vs_calc.vs_info.idx_put_start = idx_put_start
    vs_calc.vs_info.idx_del_start = idx_del_start
    vs_calc.vs_info.prepare_concurrent = prepare_concurrent
    vs_calc.calc()
    vs_info = vs_calc.vs_info

    vm_obj = VideoMonitor3(
        client_types, endpoint, access_key, secret_key, tls, alias,
        bucket_prefix, bucket_num=vs_info.channel_num,
        obj_prefix=obj_prefix, obj_num=vs_info.obj_num, multipart=vs_info.multipart, local_path=local_path,
        main_concurrent=vs_info.main_concurrent, prepare_concurrent=vs_info.prepare_concurrent,
        max_workers=vs_info.main_max_workers,
        idx_width=vs_info.idx_width, idx_put_start=vs_info.idx_put_start, idx_del_start=vs_info.idx_del_start,
        obj_num_per_day=vs_info.obj_num_pd
    )
    vm_obj.run()
