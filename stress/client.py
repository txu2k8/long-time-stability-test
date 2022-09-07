#!/usr/bin/python
# -*- coding:utf-8 _*- 
"""
@author:TXU
@file:client
@time:2022/09/07
@email:tao.xu2008@outlook.com
@description:
"""
from typing import List, Text
from loguru import logger

from config.models import ClientType
from tools.mc import MClient
from tools.s3cmd import S3CmdClient


def init_clients(client_types: List[Text], endpoint, access_key, secret_key, tls, alias='play'):
    """
    初始化IO客户端
    :param client_types:
    :param endpoint:
    :param access_key:
    :param secret_key:
    :param tls:
    :param alias:
    :return:
    """
    clients_info = {}
    for client_type in client_types:
        logger.info("初始化工具Client({})...".format(client_type))
        if client_type.upper() == ClientType.MC.name:
            client = MClient(endpoint, access_key, secret_key, tls, alias)
        elif client_type == ClientType.S3CMD:
            client = S3CmdClient(endpoint, access_key, secret_key, tls)
        else:
            raise Exception("仅支持工具：{}".format(ClientType.value))
        clients_info[client_type] = client

    return clients_info


if __name__ == '__main__':
    pass
