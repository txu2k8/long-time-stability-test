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
from config import __version__


app = typer.Typer(name="LTS", add_completion=False, help="长稳测试工具集 CLI.")
state = {"verbose": False}
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


def dis_version(display: bool):
    if display:
        print(f"LTS Version: {__version__}")
        raise typer.Exit()  # 显示完后退出


@app.callback(invoke_without_command=True, context_settings=CONTEXT_SETTINGS)
def main(ctx: typer.Context,
         verbose: bool = False,
         version: bool = typer.Option(
            False, "--version", "-v", help="Show version", callback=dis_version, is_eager=True
         ),  # 调用 dis_version 函数， 并且优先级最高(is_eager)
         ):
    """
    长稳测试工具集 CLI.
    """
    if verbose:
        typer.echo("Will write verbose output")
        state["verbose"] = True

    # typer.confirm("Are you sure?", default=True, abort=True)  # 给出选项，abort选项表示 No 则直接中断

    if ctx.invoked_subcommand is None:
        print('main process')


if __name__ == '__main__':
    app()
