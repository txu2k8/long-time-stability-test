#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author:TXU
@file:vs_workflow
@time:2023/03/16
@email:tao.xu2008@outlook.com
@description: 压力测试 - 参数分析计算
"""
from typing import Text, List
from pydantic import BaseModel
from loguru import logger
from prettytable import PrettyTable, ALL

from config.models import FileInfo
from utils.util import size_convert_byte2str, size_convert_str2byte


class RequirementInfo(BaseModel):
    """压力测试 - 原始需求模型"""
    # 原始需求模型
    src_files: List[FileInfo] = []  # 源文件信息
    max_workers: int = 1  # 最大并发worker数

    total_capacity: int = 0  # 存储总容量，byte
    safe_water_level: float = 0.9  # 默认安全水位为 90%, 即推荐最大写入数据量不超过集群容量90%
    safe_water_capacity: int = 0  # 默认安全水位容量，byte

    appendable: bool = False  # 追加写
    segments: int = 1  # 追加写模式下，分片追加次数
    disable_multipart: bool = False  # 对象：非多段上传


class CustomizeInfo(BaseModel):
    """压力测试 - 自定义数据模型"""
    root_prefix: Text = "stress-"  # 桶/目录前缀
    file_prefix: Text = "data-"  # 对象/文件前缀
    idx_width: int = 11  # 对象序号长度，3=>001
    idx_put_start: int = 1  # 上传对象序号起始值
    idx_del_start: int = 1  # 删除对象序号起始值


class StressInfo(RequirementInfo, CustomizeInfo):
    """压力测试 - 模型"""
    pass


description_vs = {
    "src_files": "源文件信息",
    "max_workers": "最大并发worker数",
    "total_capacity": "存储总容量",
    "safe_water_level": "安全水位",
    "safe_water_capacity": "安全水位容量",
    "appendable": "追加写模式？",
    "segments": "追加写次数",
    "disable_multipart": "非多段上传？",

    "root_prefix": "桶/目录前缀",
    "file_prefix": "对象/文件前缀",
    "idx_width": "对象序号长度，3=>001",
    "idx_put_start": "写入文件序号起始值",
    "idx_del_start": "删除文件序号起始值",
}


class StressCalc(object):
    """基于视频路数、视频码流 计算对应的测试数据模型"""

    def __init__(
            self,
            capacity_human, safe_water_level=0.9,
            src_files=None, max_workers=1,
            appendable=False, segments=1, disable_multipart=False,
            root_prefix="stress-", file_prefix="data-", idx_width=11, idx_start=1
    ):
        self.src_files = [] if src_files is None else src_files
        capacity = size_convert_str2byte(capacity_human)
        self.s_info = StressInfo(
            src_files=src_files,
            max_workers=max_workers,

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
        logger.info("{0} 原始需求 {0}".format("-"*30))
        dict_s_info = dict(self.s_info)
        title = ["参数", "值"]
        tb_r = PrettyTable(title, hrules=ALL)
        for k in RequirementInfo().dict().keys():
            desc = description_vs[k]
            v = dict_s_info[k]
            if k == "src_files":
                v = f"size={v.size_human},path={v.full_path}"
            if 'capacity' in k:
                v = size_convert_byte2str(v)
            # logger.info("{desc:<{fl}}\t: {v:<}".format(desc=desc, fl=36 - len(desc.encode('GBK')) + len(desc), v=v))
            tb_r.add_row((desc, v))
        tb_r.align["参数"] = "l"
        logger.info(f"\n{tb_r}")

        logger.info("{0} 自定义 {0}".format("-" * 30))
        tb_c = PrettyTable(title, hrules=ALL)
        for k in CustomizeInfo().dict().keys():
            desc = description_vs[k]
            v = dict_s_info[k]
            tb_c.add_row((desc, v))
            logger.info("{desc:<{fl}}\t: {v:<}".format(desc=desc, fl=36 - len(desc.encode('GBK')) + len(desc), v=v))
        tb_c.align["参数"] = "l"
        logger.info(f"\n{tb_c}")

        logger.info("{0} 即将运行 {0}".format("-"*30))

    def calc(self):
        """原始需求 --> 计算 --> 数据模型"""
        self.print_model()


if __name__ == '__main__':
    obj_info = FileInfo(
        size=128*1024*1024
    )
    StressCalc("160.05TB", src_files=[obj_info], idx_start=4795)

