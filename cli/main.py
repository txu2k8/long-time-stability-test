#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:main
@time:2022/09/01
@email:tao.xu2008@outlook.com
@description:
"""
import typer
from typing import Optional

from config import __version__


def version_callback(value: bool):
    if value:
        print(f"LTS Version: {__version__}")
        raise typer.Exit()


def public(
        version: Optional[bool] = typer.Option(
            None, "--version", callback=version_callback, help='Show the tool version'
        ),
):
    """公共参数"""
    pass


app = typer.Typer(name="LTS", callback=public, help="长稳测试工具集 CLI.")


if __name__ == '__main__':
    app()
