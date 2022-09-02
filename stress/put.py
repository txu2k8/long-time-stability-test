import random

from loguru import logger
from config.models import ClientType
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    if tool_type.upper() == ClientType.MC.name:
        client = MClient(endpoint, access_key, secret_key, tls, alias)
    elif tool_type == ClientType.S3CMD:
        raise Exception("暂不支持 s3cmd工具")
    else:
        raise Exception("仅支持工具：{}".format(ClientType.value))

    # 准备源数据文件池 字典
    file_list = get_local_files_md5(src_file_path)

    dir_num = int(obj_num * concurrent / depth)  # 文件夹数量

    futures = set()
    with ThreadPoolExecutor(max_workers=None) as executor:
        for x in range(obj_num):
            bucket_idx = x % bucket_num
            bucket = '{}{}'.format(bucket_prefix, bucket_idx)
            obj = random.choice(file_list)
            dst_path = ''
            if depth < 2:
                dst_path = obj.name
            else:
                for d in range(2, depth+1):  # depth=2为第一级文件夹
                    dst_path += f'nested{d-1}/'
                dst_path += obj.name

            # 上传
            futures.add(executor.submit(client.put, obj.full_path, bucket, dst_path, disable_multipart, tags=obj.tags))
            future_result = [future.result() for future in as_completed(futures, timeout=None)]
            result = False if False in future_result else True

    return True
