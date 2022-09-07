from enum import Enum
from typing import Text
from pydantic import BaseModel


# 生产工具类型 - 枚举
class ClientType(str, Enum):
    """测试步骤类型枚举"""
    SDK = 'SDK'  # SDK 调用
    MC = 'MC'  # MC 命令执行
    S3CMD = 'S3CMD'


class FileInfo(BaseModel):
    """文件信息 - 数据模型"""
    name: Text
    full_path: Text
    file_type: Text = ''
    md5: Text
    tags: Text
