from enum import Enum
from typing import Text


# 生产工具类型 - 枚举
class ToolType(Text, Enum):
    """测试步骤类型枚举"""
    API = 'API'  # rest_api 测试
    SDK = 'SDK'  # SDK 调用
    UC = 'UC'  # UC 命令执行
    MC = 'MC'  # MC 命令执行
    S3CMD = 'S3CMD'
