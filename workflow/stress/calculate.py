#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author:TXU
@file:vs_workflow
@time:2023/03/16
@email:tao.xu2008@outlook.com
@description: 压力测试 - 参数分析计算
"""
from typing import Text
from pydantic import BaseModel
from loguru import logger

from config.models import FileInfo
from utils.util import size_convert_byte2str, size_convert_str2byte


class RequirementInfo(BaseModel):
    """视频监控 - 原始需求模型"""
    # 原始需求模型
    channel_num: int = 0  # 视频路数
    bitstream: int = 4  # 码流
    data_life: float = 0  # 数据保留期限（天）
    src_file: FileInfo = FileInfo()  # 源文件信息
    segments: int = 1  # 追加写模式下，分片追加次数

    total_capacity: int = 0  # 存储总容量，byte
    safe_water_level: float = 0.9  # 默认安全水位为 90%, 即推荐最大写入数据量不超过集群容量90%
    safe_water_capacity: int = 0  # 默认安全水位容量，byte

    appendable: bool = False  # 追加写
    disable_multipart: bool = False  # 对象：非多段上传


class DataModelInfo(BaseModel):
    """视频监控 - 数据业务模型"""
    # 需求分解 --> 数据模型
    bandwidth: int = 0  # MB
    bucket_num: int = 1  # 桶数
    obj_num: int = 0  # 总文件数,文件平均分配到每个桶
    obj_num_pd: int = 0  # 每天需要写入的对象数量
    obj_num_pc: int = 0  # 每路视频需要保存的对象数量
    obj_num_pc_pd: int = 0  # 每路视频每天需要写入的对象数量
    time_interval: float = 0  # 一路视频中每个对象产生间隔

    main_concurrent: float = 1  # 写删阶段每秒并行数
    prepare_concurrent: float = 1  # 预置数据时每秒并行数
    max_workers: int = 1  # 写删阶段最大worker数


class CustomizeInfo(BaseModel):
    """视频监控 - 自定义数据模型"""
    prepare_channel_num: int = 0  # 数据预置阶段,写入视频路数, 0 ==> channel_num
    root_prefix: Text = "video-"  # 桶前缀
    file_prefix: Text = "data-"  # 对象/文件前缀
    idx_width: int = 11  # 对象序号长度，3=>001
    idx_put_start: int = 1  # 上传对象序号起始值
    idx_del_start: int = 1  # 删除对象序号起始值


class StressInfo(RequirementInfo, DataModelInfo, CustomizeInfo):
    """压力测试 - 模型"""
    pass


description_vs = {
    "channel_num": "视频路数",
    "bitstream": "码流(Mbps)",
    "data_life": "数据保留期限(天)",
    "src_file": "源文件信息",
    "segments": "追加写次数",
    "total_capacity": "存储总容量",
    "safe_water_level": "安全水位",
    "safe_water_capacity": "安全水位容量",
    "appendable": "追加写模式？",
    "disable_multipart": "非多段上传？",

    "bandwidth": "带宽(MB/s)",
    "bucket_num": "桶数",
    "obj_num": "对象数（总）",
    "obj_num_pd": "对象数（每天）",
    "obj_num_pc": "对象数（单路）",
    "obj_num_pc_pd": "对象数（单路每天）",
    "time_interval": "对象产生间隔（单路）",

    "main_concurrent": "写删阶段每秒并行数",
    "prepare_concurrent": "预置阶段每秒并行数",
    "max_workers": "写删阶段最大worker数",

    "prepare_channel_num": "预置阶段视频路数",
    "root_prefix": "桶前缀",
    "file_prefix": "对象前缀",
    "idx_width": "对象序号长度，3=>001",
    "idx_put_start": "上传对象序号起始值",
    "idx_del_start": "删除对象序号起始值",
}


class StressCalc(object):
    """基于视频路数、视频码流 计算对应的测试数据模型"""

    def __init__(self, channel_num, bitstream, capacity_human, data_life=0, safe_water_level=0.9,
                 prepare_channel_num=0, src_file: FileInfo = FileInfo(),
                 segments=1, appendable=False, disable_multipart=False, max_workers=1,
                 root_prefix="bc-", file_prefix="data-", idx_width=11, idx_start=1):
        self.channel_num = channel_num
        self.bitstream = bitstream
        self.data_life = data_life
        self.src_file = src_file
        self.prepare_channel_num = prepare_channel_num if prepare_channel_num > channel_num else channel_num

        capacity = size_convert_str2byte(capacity_human)
        self.vs_info = StressInfo(
            channel_num=channel_num,
            bitstream=bitstream,
            data_life=data_life,
            src_file=src_file,
            segments=segments,
            total_capacity=capacity,
            safe_water_level=safe_water_level,
            safe_water_capacity=capacity * safe_water_level,
            disable_multipart=disable_multipart,
            appendable=appendable,

            # bandwidth
            bucket_num=channel_num,
            # obj_num
            # obj_num_pd
            # obj_num_pc
            # obj_num_pc_pd
            # time_interval
            # main_concurrent
            # prepare_concurrent
            max_workers=max_workers,  # 最大work数量,超出即意味着 IO堆积

            prepare_channel_num=self.prepare_channel_num,
            root_prefix=root_prefix,
            file_prefix=file_prefix,
            idx_width=idx_width,
            idx_put_start=idx_start,
            # idx_del_start
        )
        self.calc()

    def print_model(self):
        """
        打印计算结果
        :return:
        """
        logger.info("{0} 原始需求 {0}".format("-"*30))
        dict_vs_info = dict(self.vs_info)
        for k in RequirementInfo().dict().keys():
            desc = description_vs[k]
            v = dict_vs_info[k]
            if k == "src_file":
                v = f"size={v.size_human},path={v.full_path}"
            if 'capacity' in k:
                v = size_convert_byte2str(v)
            logger.info("{desc:<{fl}}\t: {v:<}".format(desc=desc, fl=20-len(desc.encode('GBK'))+len(desc), v=v))

        logger.info("{0} 数据模型 {0}".format("-" * 30))
        for k in DataModelInfo().dict().keys():
            desc = description_vs[k]
            v = dict_vs_info[k]
            logger.info("{desc:<{fl}}\t: {v:<}".format(desc=desc, fl=36-len(desc.encode('GBK'))+len(desc), v=v))

        logger.info("{0} 自定义 {0}".format("-" * 30))
        for k in CustomizeInfo().dict().keys():
            desc = description_vs[k]
            v = dict_vs_info[k]
            logger.info("{desc:<{fl}}\t: {v:<}".format(desc=desc, fl=36 - len(desc.encode('GBK')) + len(desc), v=v))

        logger.info("{0} 即将运行 {0}".format("-"*30))

    def calc(self):
        """原始需求 --> 计算 --> 数据模型"""
        # 每天一路视频需要写入的数据量 = (码流 / 8) * 1天 * MB
        size_pc_pd = (self.vs_info.bitstream / 8) * 60 * 60 * 24 * 1024 * 1024

        # 根据码流+容量+保留期限，换算出 支持的视频路数
        if self.vs_info.channel_num == 0 and self.vs_info.data_life > 0:
            self.vs_info.channel_num = round(self.vs_info.safe_water_capacity / self.vs_info.data_life / size_pc_pd)

        # 码流为 Mbps,转换为带宽: MB / s
        self.vs_info.bandwidth = self.vs_info.channel_num * self.vs_info.bitstream / 8

        # 对象数 = safe_water_capacity / obj_size
        self.vs_info.obj_num = int(self.vs_info.safe_water_capacity / self.vs_info.src_file.size)

        # 每天需要写入的数据量 = 带宽/s * 1天
        size_pd = self.vs_info.bandwidth * 60 * 60 * 24 * 1024 * 1024

        # 根据 视频路数+码流+容量，换算出 数据最大保留期限
        if self.vs_info.data_life == 0:
            self.vs_info.data_life = round(self.vs_info.safe_water_capacity / size_pd, 3)
        else:
            # 计算需要的容量大小
            required_capacity = size_pd * self.vs_info.data_life / self.vs_info.safe_water_level
            assert required_capacity < self.vs_info.total_capacity, \
                f"集群空间不足：{self.vs_info.channel_num}路视频保存{self.vs_info.data_life}，" \
                f"期望容量{size_convert_byte2str(required_capacity)}，实际容量{size_convert_byte2str(self.vs_info.total_capacity)}"

        # 每路视频需要保存的 对象数 = 对象总数 / channel数
        self.vs_info.obj_num_pc = int(self.vs_info.obj_num / self.vs_info.channel_num)

        # 每天需要写入的 对象数 = 每日数据量 / 对象大小
        self.vs_info.obj_num_pd = int(size_pd / self.vs_info.src_file.size)
        self.vs_info.obj_num_pc_pd = int(size_pd / self.vs_info.src_file.size / self.vs_info.channel_num)

        # 对象产生间隔 -- 单路
        self.vs_info.time_interval = round(self.vs_info.src_file.size / (self.vs_info.bitstream / 8 * 1024 * 1024), 2)

        # 写删均衡阶段 并发数
        self.vs_info.main_concurrent = round(self.vs_info.bandwidth / self.vs_info.src_file.size * 1024 * 1024, 2)

        # 数据预置阶段 并发数
        prepare_bandwidth = self.prepare_channel_num * self.bitstream / 8
        self.vs_info.prepare_concurrent = round(prepare_bandwidth / self.vs_info.src_file.size * 1024 * 1024, 2)

        # 删除对象起始值
        idx_del_start = self.vs_info.idx_put_start - self.vs_info.obj_num_pc
        self.vs_info.idx_del_start = idx_del_start if idx_del_start >= 0 else 1

        self.print_model()


if __name__ == '__main__':
    obj_info = FileInfo(
        size=128*1024*1024
    )
    VSCalc(1000, 4, "160.05TB", src_file=obj_info, prepare_channel_num=2000, idx_start=4795)

