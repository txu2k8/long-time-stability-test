#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:workflow_interface
@time:2022/09/28
@email:tao.xu2008@outlook.com
@description: 工作流接口
"""
from abc import ABC, abstractmethod


class WorkflowInterface(ABC):
    """定义所有 工作流 需要实现的接口"""

    @abstractmethod
    def stage_init(self, *args, **kwargs):
        pass

    @abstractmethod
    def stage_prepare(self, *args, **kwargs):
        pass

    @abstractmethod
    def stage_main(self, *args, **kwargs):
        pass

    @abstractmethod
    def run(self, *args, **kwargs):
        pass


if __name__ == '__main__':
    pass
