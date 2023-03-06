#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author:TXU
@file:vs_workflow
@time:2022/12/12
@email:tao.xu2008@outlook.com
@description: 视频监控场景-存储数据模型模拟测试 - 基类
需求：
    1、单节点1000+路视频
    2、码流：4Mbps
    3、7*24h 写删均衡
"""
import random
import asyncio
from abc import ABC
from loguru import logger

from utils.util import get_local_files
from config.models import ClientType


from workflow.workflow_base import WorkflowBase
from workflow.workflow_interface import WorkflowInterface


def video_surveillance_calc(video_channel, video_stream):
    """
    基于视频路数、视频码流 计算对应的测试数据模型
    :param video_channel:
    :param video_stream:
    :return:
    """


if __name__ == '__main__':
    pass
