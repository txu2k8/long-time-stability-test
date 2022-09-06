import datetime
import random
import asyncio

from loguru import logger
from config.models import ClientType

from tools.mc import MClient
from utils.util import get_local_files_md5


async def put_obj_aio(client: MClient, concurrent, src_obj, bucket, obj_prefix, disable_multipart):
    """
    异步上传
    :param client:
    :param concurrent:
    :param src_obj:
    :param bucket:
    :param obj_prefix:
    :param disable_multipart:
    :return:
    """
    task_list = []
    for x in range(0, concurrent):
        dst_path = f"{obj_prefix}c{str(x)}{src_obj.name}"
        task = asyncio.create_task(
            client.put(src_obj.full_path, bucket, dst_path, disable_multipart, tags=src_obj.tags)
        )
        task_list.append(task)
    for t in task_list:
        await t
    return task_list


async def put_obj_concurrently(idx, client, bucket_num, bucket_prefix, file_list, obj_prefix, concurrent,
                               disable_multipart):
    """
    并行上传
    :param idx:
    :param client:
    :param bucket_num:
    :param bucket_prefix:
    :param file_list:
    :param obj_prefix:
    :param concurrent:
    :param disable_multipart:
    :return:
    """
    # 准备
    bucket_idx = idx % bucket_num
    bucket = '{}{}'.format(bucket_prefix, bucket_idx)
    obj = random.choice(file_list)
    obj_prefix += str(idx)

    # 每秒并行上传， concurrent
    asyncio.ensure_future(
        put_obj_aio(client, concurrent, obj, bucket, obj_prefix, disable_multipart)
    )
    await asyncio.sleep(1)


def put_obj(
        tool_type,
        endpoint, access_key, secret_key, tls, alias,
        src_file_path, bucket_prefix, bucket_num=1, depth=1, obj_prefix='', obj_num=1,
        concurrent=1, disable_multipart=False,
        duration=''
):
    """
    上传对象
    :param tool_type:
    :param endpoint:
    :param access_key:
    :param secret_key:
    :param tls:
    :param alias:
    :param src_file_path:
    :param bucket_prefix:
    :param bucket_num:
    :param depth:
    :param obj_prefix:
    :param obj_num:
    :param concurrent:
    :param disable_multipart:
    :param duration:
    :return:
    """
    # 初始化工具client
    if tool_type.upper() == ClientType.MC.name:
        client = MClient(endpoint, access_key, secret_key, tls, alias)
    elif tool_type == ClientType.S3CMD:
        raise Exception("暂不支持 s3cmd工具")
    else:
        raise Exception("仅支持工具：{}".format(ClientType.value))

    # 准备源数据文件池 字典
    file_list = get_local_files_md5(src_file_path)
    total_num = obj_num*concurrent
    if depth > 2:
        for d in range(2, depth + 1):  # depth=2为第一级文件夹
            obj_prefix += f'nested{d - 1}/'

    if duration:
        # 持续上传时间
        logger.info("Run test duration {}s, concurrent={}".format(duration, concurrent))
        idx = 0
        start = datetime.datetime.now()
        end = datetime.datetime.now()
        while int(duration) > (end-start).total_seconds():
            logger.info("PUT idx={}, concurrent={}, total={}".format(idx, concurrent, total_num))
            asyncio.run(put_obj_concurrently(
                idx, client, bucket_num, bucket_prefix, file_list, obj_prefix, concurrent, disable_multipart)
            )
            idx += 1
            end = datetime.datetime.now()
        logger.info("duration {} completed!".format(duration))
    else:
        # 指定上传数量
        logger.info("PUT obj_num={}, concurrent={}, total={}".format(obj_num, concurrent, total_num))
        for x in range(obj_num):
            asyncio.run(put_obj_concurrently(
                x, client, bucket_num, bucket_prefix, file_list, obj_prefix, concurrent, disable_multipart)
            )
