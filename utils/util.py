import os
import time
import re
import random
import string
import hashlib
import subprocess
from typing import List
from loguru import logger
from pypinyin import lazy_pinyin

from config.models import FileSegmentInfo, FileInfo


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


def get_local_files(local_path, with_rb_data=False, segments=1) -> List[FileInfo]:
    """
    获取本地文件路径下所以文件及其MD5
    :param local_path:
    :param with_rb_data:
    :param segments:
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
                attr="filename={};md5={};type={}".format(filename, md5, file_type),
                tags="md5={}".format(md5),  # filename={}&md5={}&type={}
                segments=segments,
            )
            file_info.size = os.path.getsize(file_full_path)
            file_info.size_human = size_convert_byte2str(file_info.size)
            if with_rb_data:
                with open(file_full_path, "rb") as f:
                    file_s_info = FileSegmentInfo(
                        position=0,
                        size=file_info.size,
                        data=f.read()
                    )
                    file_info.rb_data_list = [file_s_info]
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


def is_contains_zh(content: str):
    """
    检查整个字符串是否包含中文
    :param content: 需要检查的字符串
    :return: bool
    """
    if content is None:
        content = ""
    if not isinstance(content, str):
        content = str(content)
    for ch in content:
        if u'\u4e00' <= ch <= u'\u9fff':
            return True
    return False


def to_safe_name(content: str):
    """
    中文转换为拼音，然后替换字符串中非字母、数字、下划线的字符为下划线，转换小写
    :param content: 原始字符串
    :return: 小写字母、数字、下划线组成的字符串
    """
    return str(re.sub("[^a-zA-Z0-9_]+", "", '_'.join(lazy_pinyin(content)))).lower()


def to_class_name(content: str):
    """
    中文转拼音，删除字符串中非字母、数字、下划线的字符，单词首字母大小，如："class-mall goods" --> ClassMallGoods
    :param content: 原始字符串
    :return: 字母、数字组合的字符串，驼峰格式
    """
    if is_contains_zh(content):
        content = '_'.join(lazy_pinyin(content)).title()
    if ' ' in content:
        content = content.title().replace(' ', '')
    return str(re.sub("[^a-zA-Z0-9]+", "", content))


def zfill(number, width=9):
    """
    转换数字为字符串，并以’0‘填充左侧
    :param number:
    :param width: 最小宽度，如实际数字小于最小宽度，左侧填0
    :return: str, __zfill(123, 6) -> '000123'
    """
    return str(number).zfill(width)


def popen_exec_cmd(cmd_spec, timeout=7200):
    """
    Executes command and Returns (rc, output) tuple
    :param cmd_spec: Command to be executed
    :param timeout
    :return:
    """

    logger.info('Execute: {cmds}'.format(cmds=cmd_spec))
    p = subprocess.Popen(cmd_spec, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    t_beginning = time.time()
    while True:
        if p.poll() is not None:
            break
        seconds_passed = time.time() - t_beginning
        if timeout and seconds_passed > timeout:
            p.terminate()
            raise TimeoutError('TimeOutError: {0} seconds'.format(timeout))
        time.sleep(0.1)
    rc = p.returncode
    stdout, stderr = p.stdout.read(), p.stderr.read()
    if stdout:
        logger.debug(stdout.decode())
    if stderr:
        logger.error(stderr.decode())
    return rc, stdout, stderr


def generate_random_string(str_len=16):
    """
    生产随机字符串
    :param str_len: byte
    :return:
    """

    base_string = string.ascii_letters + string.digits
    # base_string = string.printable
    base_string_len = len(base_string)
    multiple = 1
    if base_string_len < str_len:
        multiple = (str_len // base_string_len) + 1

    return ''.join(random.sample(base_string * multiple, str_len))


def size_convert_str2byte(str_size, ratio=1024):
    """
    转换 1K,1M,1G,1T 为byte数，转换率：1024
    :param str_size:such as 1KB,1MB,1GB,1TB
    :param ratio: 转换倍率，默认1024
    :return:size (byte)
    """
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    keys = re.findall(r'(\d+)([B_K_M_G_T_P_Z]+)', str_size.upper())
    if len(keys) != 1:
        raise Exception('无效的str_size：{}，期望如：1MB、2GB'.format(str_size))
    value, unit = keys[0]
    try:
        power = units.index(unit)
    except Exception as e:
        raise Exception("无效的单位：{}，期望：{}\n {}".format(unit, units, e))
    return int(value) * (ratio**power)


def size_convert_byte2str(value):
    """
    将字节数转换为易读的size， 10000 -> 9.77KB
    :param value:
    :return:
    """
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = 1024.0
    for i in range(len(units)):
        if (value / size) < 1:
            return "%.2f%s" % (value, units[i])
        value = value / size


def seconds_convert_str(seconds):
    """
    把秒数转换为易读的时间长，如：12d, 23:43:42
    :param seconds:
    :return:
    """
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    return "%dd, %02d:%02d:%02d" % (d, h, m, s)


def split_list_n_list(origin_list, n):
    """
    把原始列表切分N等份
    :param origin_list:
    :param n:
    :return:
    """
    if len(origin_list) % n == 0:
        cnt = len(origin_list) // n
    else:
        cnt = len(origin_list) // n + 1

    for i in range(0, n):
        yield origin_list[i*cnt:(i+1)*cnt]


if __name__ == "__main__":
    pass
    # print(size_convert_str2byte('1G'))
    print(seconds_convert_str(1122222))
