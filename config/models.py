from enum import Enum
from typing import Text
from pydantic import BaseModel


# 客户端工具类型 - 枚举
class ClientType(str, Enum):
    """客户端类型枚举"""
    SDK = 'SDK'  # SDK 调用
    MC = 'MC'  # MC 命令执行
    S3CMD = 'S3CMD'


# 生产数据文件类型 - 枚举
class GenFileType(str, Enum):
    """生产数据文件类型枚举"""
    TXT = '.txt'  # TXT文件
    DATA = '.data'


# 多段设置类型 - 枚举
class MultipartType(str, Enum):
    """多段设置 类型枚举"""
    enable = 'enable'  # 启用
    disable = 'disable'  # 禁用
    random = 'random'  # 随机


# 文件信息
class FileInfo(BaseModel):
    """文件信息 - 数据模型"""
    name: Text
    full_path: Text
    file_type: Text = ''
    md5: Text = ''
    tags: Text = ''  # "key1=value1&key2=value2"
    attr: Text = ''  # "key1=value1;key2=value2"
