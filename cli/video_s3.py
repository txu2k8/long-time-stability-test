#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author:TXU
@file:video_surveillance
@time:2022/12/12
@email:tao.xu2008@outlook.com
@description: 视频监控场景 - 对象存储- 模拟测试
"""

import sys
import time
from datetime import datetime
from loguru import logger
import typer
import asyncio

from cli.main import app
from cli.log import init_logger
from utils.util import get_local_files
from config.models import ClientType, MultipartType
from workflow.workflow_base import init_clients, InitDB
from workflow.video.calculate import VSCalc
from workflow.video.s3.multi_channel import multi_channel_run


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


@app.command(help='视频监控场景测试: 写删均衡', hidden=False)
def video_s3(
        ctx: typer.Context,
        # 存储 环境信息
        endpoint: str = typer.Option(..., help="环境信息：例 127.0.0.1:9000 or http://127.0.0.1:9000"),
        access_key: str = typer.Option(..., help="环境信息：ACCESS_KEY"),
        secret_key: str = typer.Option(..., help="环境信息：SECRET_KEY"),
        tls: bool = typer.Option(False, help="环境信息：https传输协议"),
        alias: str = typer.Option('play', help="环境信息：别名"),

        # 视频监控 业务模型
        channel_num: int = typer.Option(1, min=1, help="业务模型：视频路数"),
        bitstream: int = typer.Option(4, min=1, help="业务模型：视频码流（单位：Mbps）"),
        data_life: int = typer.Option(0, min=0, help="业务模型：数据保留期限（单位：天），0-表示自动推算"),
        capacity: str = typer.Option(..., help="业务模型：可用空间（支持写法如：5GB、10.2TB）"),
        safe_water_level: float = typer.Option(0.9, min=0, help="业务模型：可用空间（单位：MB）"),
        local_path: str = typer.Option(..., help="业务模型：指定源文件路径，随机上传文件"),
        appendable: bool = typer.Option(False, help="业务模型：追加写模式？"),
        segments: int = typer.Option(0, min=0, help="业务模型：追加写模式下，一个对象分片进行追加次数数"),
        disable_multipart: bool = typer.Option(False, help="业务模型：非多段上传？"),
        max_workers: int = typer.Option(1000, min=1, help="业务模型：写删阶段最大并发数"),
        prepare_channel_num: int = typer.Option(0, min=0, help="业务模型：预置阶段,视频写入路数,默认=channel_num"),

        # 自定义设置
        bucket_prefix: str = typer.Option('bucket', help="自定义：桶名称前缀"),
        obj_prefix: str = typer.Option('data', help="自定义：对象名前缀"),
        idx_width: int = typer.Option(11, min=1, help="自定义：对象序号长度，3=>001"),
        idx_start: int = typer.Option(1, min=1, help="自定义：上传对象序号起始值"),
        skip_stage_init: bool = typer.Option(False, help="自定义：跳过 init 阶段"),
        write_only: bool = typer.Option(False, help="自定义：只写入，不删除"),
        delete_immediately: bool = typer.Option(False, help="自定义：写入后，立即删除上一个"),
        single_root: bool = typer.Option(False, help="自定义：单根目录模式？"),
        single_root_name: str = typer.Option('video', help="自定义：单根目录时，根目录名称"),
        process_workers: int = typer.Option(8, min=1, help="自定义：多进程运行协程，进程数"),
        duration: int = typer.Option(0, min=0, help="自定义：指定持续执行时间，0-代表永久"),

        # 其他
        trace: bool = typer.Option(False, help="print TRACE level log"),
        case_id: int = typer.Option(0, min=0, help="测试用例ID，关联到日志文件名"),
        desc: str = typer.Option('', help="测试描述"),
):
    client_types = [ClientType.MC]
    init_logger(prefix='video', case_id=case_id, trace=trace)
    init_print(case_id, desc, client_types, channel_num, bitstream, disable_multipart, max_workers)

    if not appendable:
        # 非追加写模式，分片=1
        segments = 1

    # 读取源数据文件池
    file_list = get_local_files(local_path, with_rb_data=appendable, segments=segments)
    assert len(file_list) > 0, "未找到任何源文件！"
    file_info = file_list[0]

    # 计算分析业务需求，打印业务模型
    vs_info = VSCalc(
        channel_num, bitstream, capacity, data_life, safe_water_level,
        prepare_channel_num, file_info, segments, appendable, disable_multipart, max_workers,
        bucket_prefix, obj_prefix, idx_width, idx_start
    ).vs_info
    time.sleep(3)

    # 初始化客户端
    clients_info = init_clients(client_types, endpoint, access_key, secret_key, tls, alias)
    client = clients_info[ClientType.MC.value]

    # 初始化数据库
    # InitDB().db_init()

    # 单桶模式，只创建一次桶
    if single_root:
        client.mb(single_root_name, appendable=vs_info.appendable)
        skip_stage_init = True

    multi_channel_run(
        channel_num, process_workers,
        client=client, file_info=file_info, vs_info=vs_info,
        skip_stage_init=skip_stage_init, write_only=write_only, delete_immediately=delete_immediately,
        single_root=single_root, single_root_name=single_root_name, duration=duration,
        trace=trace
    )
