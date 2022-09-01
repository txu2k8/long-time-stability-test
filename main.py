import typer


app = typer.Typer(help="长稳测试工具集 CLI.")


@app.command(help='stress get objects')
def get(name: str):
    print(f"Hello {name}")


@app.command(help='stress put objects')
def put(
        access_key: str = '',
        secret_key: str = '',
        tls: bool = False,
        bucket: str = '',
        bucket_num: int = 1,
        disable_multipart: bool = False,
        concurrent: int = 1,
        md5: bool = True,
        obj_prefix: str = '',
        obj_noprefix: bool = False,
        obj_size: str = '',
        obj_num: str = '',
        obj_generator: str = '',
        obj_randsize: str = '',
        duration: str = '',
        clear: bool = False
):
    if tls:
        print(f"Goodbye Ms. {access_key}. Have a good day.")
    else:
        print(f"Bye {access_key}!")


@app.command(help='stress delete objects')
def delete(name: str, formal: bool = False):
    if formal:
        print(f"Goodbye Ms. {name}. Have a good day.")
    else:
        print(f"Bye {name}!")


if __name__ == "__main__":
    app()
