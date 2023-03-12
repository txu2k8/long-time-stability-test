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
from typing import Text
from pydantic import BaseModel
from loguru import logger


class VSBaseInfo(BaseModel):
    """视频监控 - 原始需求模型"""
    # 原始需求模型
    channel_num: int = 0  # 视频路数
    bitstream: int = 4  # 码流
    prepare_channel_num: int = 0  # 数据预置阶段,写入视频路数, 0 ==> channel_num
    available_capacity: int = 0  # MB
    save_water_level: float = 0.9  # 默认安全水位为 90%, 即推荐最大写入数据量不超过集群容量90%
    obj_size: int = 128  # 对象大小, MB


class VSDataInfo(BaseModel):
    """视频监控 - 业务模型"""
    # 需求分解 --> 数据模型
    bandwidth: int = 0
    bucket_num: int = 1  # 桶数
    obj_num: int = 0  # 对象数,对象平均分配到每个桶
    obj_num_pd: int = 0  # 每天需要写入的对象数量
    multipart: Text = 'enable'  # 多段上传
    main_concurrent: float = 1  # main阶段每秒并行数
    main_max_workers: int = 1  # main阶段最大worker数
    prepare_concurrent: float = 1  # 预置数据时每秒并行数


class VSInfo(VSBaseInfo, VSDataInfo):
    """视频监控 - 模型"""
    idx_width: int = 11  # 对象序号长度，3=>001
    idx_put_start: int = 1  # 上传对象序号起始值
    idx_del_start: int = 1  # 删除对象序号起始值


description_vs = {
    "channel_num": "视频路数",
    "bitstream": "码流(Mbps)",
    "available_capacity": "可用空间(MB)",
    "prepare_channel_num": "预置阶段视频路数",

    "bandwidth": "带宽(MB/s)",
    "obj_size": "对象大小(MB)",
    "save_water_level": "安全水位",

    "bucket_num": "桶数",
    "obj_num": "对象数,对象平均分配到每个桶",
    "obj_num_pd": "每天需要写入的对象数量",
    "multipart": "多段上传",
    "main_concurrent": "写删阶段每秒并行数",
    "main_max_workers": "写删阶段最大worker数",
    "prepare_concurrent": "预置阶段每秒并行数",

    "idx_width": "对象序号长度，3=>001",
    "idx_put_start": "上传对象序号起始值",
    "idx_del_start": "删除对象序号起始值",
}


class VSCalc(object):
    """基于视频路数、视频码流 计算对应的测试数据模型"""

    def __init__(self, channel_num, bitstream, available_capacity, prepare_channel_num=0):
        self.channel_num = channel_num
        self.bitstream = bitstream
        self.available_capacity = available_capacity
        self.prepare_channel_num = prepare_channel_num if prepare_channel_num > channel_num else channel_num

        self.vs_info = VSInfo(
            channel_num=channel_num,
            bitstream=bitstream,
            available_capacity=available_capacity,
            prepare_channel_num=prepare_channel_num,

            obj_size=128,
            bucket_num=channel_num,
            main_concurrent=0,
            main_max_workers=0,
            prepare_concurrent=3,
        )

    def print_model(self):
        fl = 30  # Width of left column of text
        logger.info("{0} 原始需求 {0}".format("-"*30))
        dict_vs_info = dict(self.vs_info)
        for k in VSBaseInfo().dict().keys():
            desc = description_vs[k]
            v = dict_vs_info[k]
            logger.info("{desc:<{fl}}\t: {v:<}".format(desc=desc, fl=20-len(desc.encode('GBK'))+len(desc), v=v))

        logger.info("{0} 数据模型 {0}".format("-" * 30))
        for k in VSInfo().dict().keys():
            desc = description_vs[k]
            v = dict_vs_info[k]
            # logger.info(f"{desc:<{fl}}: {v:<}")
            logger.info("{desc:<{fl}}\t: {v:<}".format(desc=desc, fl=36-len(desc.encode('GBK'))+len(desc), v=v))

        logger.info("{0} 业务模型 {0}".format("-"*30))

    def calc(self):
        """原始需求 --> 计算 --> 数据模型"""
        # 码流为 Mbps,转换为带宽: MB / s
        bandwidth = self.channel_num * self.vs_info.bitstream / 8
        self.vs_info.bandwidth = bandwidth

        # 对象数 = capacity / obj_size
        obj_num = int(self.vs_info.available_capacity * self.vs_info.save_water_level / self.vs_info.obj_size)
        self.vs_info.obj_num = obj_num

        # 每天需要写入的数据量 = 带宽/s * 1天
        size_pd = bandwidth * 60 * 60 * 24

        # 每天需要写入的对象数 = 每日数据量 / 对象大小
        obj_num_pd = int(size_pd / self.vs_info.obj_size)
        self.vs_info.obj_num_pd = obj_num_pd

        # 最大work数量,超出即意味着 IO堆积
        self.vs_info.main_max_workers = self.channel_num

        # 写删均衡阶段 并发数
        self.vs_info.main_concurrent = int(bandwidth / self.vs_info.obj_size)

        # 数据预置阶段 并发数
        prepare_bandwidth = self.prepare_channel_num * self.bitstream / 8
        self.vs_info.prepare_concurrent = int(prepare_bandwidth / self.vs_info.obj_size)

        self.print_model()


if __name__ == '__main__':
    vs_calc = VSCalc(1000, 4, 650*1024*1024, 2000)
    vs_calc.calc()

