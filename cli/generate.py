#!/usr/bin/python
# -*- coding:utf-8 _*- 
"""
@author:TXU
@file:generator
@time:2022/09/01
@email:tao.xu2008@outlook.com
@description:
"""
from cli.main import app


@app.command(help='generate local files')
def generate(name: str):
    print(f"Hello {name}")


if __name__ == '__main__':
    pass
