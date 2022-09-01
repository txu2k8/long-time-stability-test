from enum import Enum
from typing import Text
from pydantic import BaseModel


# 生产工具类型 - 枚举
class ToolType(Text, Enum):
    """测试步骤类型枚举"""
    API = 'API'  # rest_api 测试
    SDK = 'SDK'  # SDK 调用
    UC = 'UC'  # UC 命令执行
    MC = 'MC'  # MC 命令执行
    S3CMD = 'S3CMD'


class FileInfo(BaseModel):
    """文件信息 - 数据模型"""
    name: Text
    full_path: Text
    md5: Text
    tags: Text
