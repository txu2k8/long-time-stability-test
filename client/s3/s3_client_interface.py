#!/usr/bin/python
# -*- coding:utf-8 _*- 
"""
@author:TXU
@file:interface
@time:2022/09/08
@email:tao.xu2008@outlook.com
@description: 客户端工具接口定义
"""
from abc import ABC, abstractmethod


class ClientInterface(ABC):
    """定义所有客户端工具需要实现的接口"""
    @abstractmethod
    def mb(self, *args, **kwargs):
        pass

    @abstractmethod
    def put(self, *args, **kwargs):
        pass

    @abstractmethod
    def put_without_attr(self, *args, **kwargs):
        pass

    @abstractmethod
    def get(self, *args, **kwargs):
        pass

    @abstractmethod
    def async_get(self, *args, **kwargs):
        pass

    @abstractmethod
    def delete(self, *args, **kwargs):
        pass

    @abstractmethod
    def async_delete(self, *args, **kwargs):
        pass

    @abstractmethod
    def list(self, *args, **kwargs):
        pass

    @abstractmethod
    def async_list(self, *args, **kwargs):
        pass

    @abstractmethod
    def tag_list(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_obj_md5(self, *args, **kwargs):
        pass


if __name__ == '__main__':
    pass
