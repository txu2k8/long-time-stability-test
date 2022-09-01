import random

from loguru import logger
from config.models import ToolType

from tools.mc import MClient
from utils.util import get_local_files_md5


def put_obj(
        tool_type,
        endpoint, access_key, secret_key, tls, alias,
        bucket_prefix, bucket_num,
        src_file_path, obj_num, depth, concurrent,
        disable_multipart=False,
):
    # 初始化工具client
    if tool_type == ToolType.MC:
        client = MClient(endpoint, access_key, secret_key, tls, alias)
    elif tool_type == ToolType.S3CMD:
        raise Exception("暂不支持 s3cmd工具")
    else:
        raise Exception("仅支持工具：{}".format(ToolType.value))

    # 准备源数据文件池 字典
    file_dict = get_local_files_md5(src_file_path)
    dir_num = int(obj_num * concurrent / depth)  # 文件夹数量
    for x in range(obj_num):
        obj = random.choice(list(file_dict))


    # 上传
    client.cp(src_path, bucket, dst_path, disable_multipart=False, tags="")
