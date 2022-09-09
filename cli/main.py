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

from config import __version__, set_global_value, LOG_LEVEL


def version_callback(value: bool):
    if value:
        print(f"LTS Version: {__version__}")
        raise typer.Exit()


def public(
        trace: bool = typer.Option(False, help="print TRACE level log"),
        version: Optional[bool] = typer.Option(
            None, "--version", callback=version_callback
        ),
):
    """公共参数"""
    set_global_value('LOG_LEVEL', 'TRACE' if trace else LOG_LEVEL)


app = typer.Typer(name="LTS", callback=public, help="长稳测试工具集 CLI.")


if __name__ == '__main__':
    app()
