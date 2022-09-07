import os
import hashlib
import subprocess
from typing import List
from loguru import logger

from config.models import FileInfo


def get_md5_value(file_path):
    """
    获取文件MD5值
    :param file_path:
    :return:
    """
    h = hashlib.md5()
    with open(file_path, "rb") as fp:
        while True:
            # Hash 32kB chunks
            data = fp.read(32 * 1024)
            if not data:
                break
            h.update(data)
    return h.hexdigest()


def md5sum(file_path):
    """
    md5sum 工具获取文件MD5值
    :param file_path:
    :return:
    """
    md5 = ''
    cmd = f"md5sum {file_path}"
    rc, output = subprocess.getstatusoutput(cmd)
    if rc == 0:
        md5 = output.split(' ')[0].split('\\')[-1]
    else:
        logger.error("获取文件md5值失败：{}，{}".format(cmd, output))
    return rc, md5


def get_local_files(local_path) -> List[FileInfo]:
    """
    获取本地文件路径下所以文件及其MD5
    :param local_path:
    :return:
    """
    file_list = []
    for dir_path, dir_names, file_names in os.walk(local_path):
        for filename in file_names:
            file_type = os.path.splitext(filename)[-1]
            file_full_path = os.path.join(dir_path, filename)
            md5 = get_md5_value(file_full_path)
            file_info = FileInfo(
                name=filename,
                full_path=file_full_path,
                file_type=file_type,
                md5=md5,
                tags="filename={}&md5={}&type={}".format(filename, md5, file_type)
            )
            file_list.append(file_info)
    return file_list


def mkdir_if_not_exist(dir_path):
    """
    如果文件夹不存在，创建
    :param dir_path:
    :return:
    """
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    return
