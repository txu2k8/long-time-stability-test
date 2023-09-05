#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author:TXU
@file:vs_workflow
@time:2023/03/16
@email:tao.xu2008@outlook.com
@description: 压力测试 - 参数分析计算
"""
import os
from typing import Text, List
from pydantic import BaseModel
from loguru import logger

from config.models import FileInfo
from utils.util import size_convert_byte2str, size_convert_str2byte


class RequirementInfo(BaseModel):
    """压力测试 - 原始需求模型"""
    # 原始需求模型
    max_workers: int = 1  # 最大并发数
    depth: int = 1  # 目录深度
    width: int = 1  # 目录宽度
    files: int = 1  # 子目录下文件数

    # 源文件
    src_files: List[FileInfo] = []  # 源文件信息
    src_files_size_avg: int = 0

    total_capacity: int = 0  # 存储总容量，byte
    safe_water_level: float = 0.9  # 默认安全水位为 90%, 即推荐最大写入数据量不超过集群容量90%
    safe_water_capacity: int = 0  # 默认安全水位容量，byte

    appendable: bool = False  # 追加写
    segments: int = 1  # 追加写模式下，分片追加次数
    disable_multipart: bool = False  # 对象：非多段上传


class DataModelInfo(BaseModel):
    """压力测试 - 数据业务模型"""
    # 需求分解 --> 数据模型
    obj_num: int = 0  # 总文件数,文件平均分配到每个桶
    safe_obj_num: int = 0  # 安全水位容量支持总文件数
    obj_num_pb: int = 0  # 平均每个桶写入的对象数量


class CustomizeInfo(BaseModel):
    """压力测试 - 自定义数据模型"""
    root_prefix: Text = "stress-"  # 桶/目录前缀
    file_prefix: Text = "data-"  # 对象/文件前缀
    idx_width: int = 11  # 对象序号长度，3=>001
    idx_put_start: int = 1  # 上传对象序号起始值
    idx_del_start: int = 1  # 删除对象序号起始值


class StressInfo(RequirementInfo, DataModelInfo, CustomizeInfo):
    """压力测试 - 模型"""
    pass


description_vs = {
    "max_workers": "最大并发worker数",
    "depth": "目录深度",
    "width": "目录宽读",
    "files": "子目录下文件数",

    "src_files": "源文件信息",
    "src_files_size_avg": "源文件平均size",
    "total_capacity": "存储总容量",
    "safe_water_level": "安全水位",
    "safe_water_capacity": "安全水位容量",
    "appendable": "追加写模式？",
    "segments": "追加写次数",
    "disable_multipart": "非多段上传？",

    "bucket_num": "桶/根目录数",
    "obj_num": "对象/文件数（总）",
    "obj_num_pd": "对象/文件数（每天）",
    "obj_num_pc": "对象/文件数（单路）",
    "obj_num_pc_pd": "对象/文件数（单路每天）",

    "root_prefix": "桶/目录前缀",
    "file_prefix": "对象/文件前缀",
    "idx_width": "文件序号长度，3=>001",
    "idx_put_start": "写入文件序号起始值",
    "idx_del_start": "删除文件序号起始值",
}


class StressCalc(object):
    """基于视频路数、视频码流 计算对应的测试数据模型"""

    def __init__(
            self,
            capacity_human, safe_water_level=0.9,
            src_files=None, max_workers=1, depth=1, width=1, files=1,
            appendable=False, segments=1, disable_multipart=False,
            root_prefix="stress-", file_prefix="data-", idx_width=11, idx_start=1
    ):
        self.src_files = [] if src_files is None else src_files
        capacity = size_convert_str2byte(capacity_human)
        self.s_info = StressInfo(
            max_workers=max_workers,
            depth=depth,
            width=width,
            files=files,

            src_files=src_files,
            total_capacity=capacity,
            safe_water_level=safe_water_level,
            safe_water_capacity=capacity * safe_water_level,
            appendable=appendable,
            segments=segments,
            disable_multipart=disable_multipart,

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
        logger.info("{0} 原始需求 {0}".format("-" * 30))
        dict_s_info = dict(self.s_info)
        for k in RequirementInfo().dict().keys():
            desc = description_vs[k]
            v = dict_s_info[k]
            if k == "src_files_size_avg":
                continue
            if k == "src_files":
                if len(v) == 1:
                    v = f"Size={v[0].size_human},Path={v[0].full_path}"
                else:
                    size_sum = 0
                    len_v = len(v)
                    dir_path = os.path.dirname(v[0].full_path)
                    for f in v:
                        size_sum += f.size
                    size_avg = size_convert_byte2str(size_sum / len_v)
                    v = f"FileNum={len_v},SizeAvg={size_avg},Path={dir_path}/*"
            if 'capacity' in k:
                v = size_convert_byte2str(v)
            logger.info("{desc:<{fl}}\t: {v:<}".format(desc=desc, fl=20 - len(desc.encode('GBK')) + len(desc), v=v))

        logger.info("{0} 数据模型 {0}".format("-" * 30))
        for k in DataModelInfo().dict().keys():
            desc = description_vs[k]
            v = dict_s_info[k]
            logger.info("{desc:<{fl}}\t: {v:<}".format(desc=desc, fl=36 - len(desc.encode('GBK')) + len(desc), v=v))

        logger.info("{0} 自定义 {0}".format("-" * 30))
        for k in CustomizeInfo().dict().keys():
            desc = description_vs[k]
            v = dict_s_info[k]
            logger.info("{desc:<{fl}}\t: {v:<}".format(desc=desc, fl=36 - len(desc.encode('GBK')) + len(desc), v=v))

        logger.info("{0} 即将运行 {0}".format("-" * 30))

    def calc(self):
        """原始需求 --> 计算 --> 数据模型"""

        # 源文件平均大小
        size_sum = 0
        len_files = len(self.s_info.src_files)
        for f in self.s_info.src_files:
            size_sum += f.size
        self.s_info.src_files_size_avg = size_sum / len_files

        # 对象数/文件数 = width**depth * files
        self.s_info.obj_num = self.s_info.width ** self.s_info.depth * self.s_info.files
        # 每桶对象数 = 对象总数 / 桶数
        self.s_info.obj_num_pb = int(self.s_info.obj_num / self.s_info.width)
        # 安全对象数 = safe_water_capacity / src_files_size_avg / 1.3倍膨胀率
        self.s_info.safe_obj_num = int(self.s_info.safe_water_capacity / self.s_info.src_files_size_avg / 1.3)  # 1.3膨胀率

        self.print_model()


if __name__ == '__main__':
    obj_info = FileInfo(
        size=128 * 1024 * 1024,
        size_human="128MiB"
    )
    StressCalc("160.05TB", src_files=[obj_info, obj_info], idx_start=4795)
