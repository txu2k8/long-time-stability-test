#!/usr/bin/python
# -*- coding:utf-8 _*- 
"""
@author:TXU
@file:file_generate
@time:2022/09/08
@email:tao.xu2008@outlook.com
@description: 生成指定大小、内容的文件
"""
import os
from loguru import logger
from utils import util


POSIX = os.name == "posix"
WINDOWS = os.name == "nt"
DD_BINARY = os.path.join(os.getcwd(), r'bin\dd\dd.exe') if WINDOWS else 'dd'
MD5SUM_BINARY = os.path.join(os.getcwd(), r'bin\git\md5sum.exe') if WINDOWS else 'md5sum'


class FileGenerator(object):
    """生产指定文件"""

    def create_file(self, path_name, total_size='4KB', line_size=128, mode='w+'):
        """
        创建txt文件，指定总大小，每行字节数
        create original file, each line with line_number, and specified line size
        :param path_name:
        :param total_size:
        :param line_size:
        :param mode: w+ / a+
        :return:
        """

        logger.info('>> Create file: {0}'.format(path_name))
        original_path = os.path.split(path_name)[0]
        if not os.path.isdir(original_path):
            try:
                os.makedirs(original_path)
            except OSError as e:
                raise Exception(e)

        size = util.size_convert_str2byte(total_size)
        line_count = size // line_size
        unaligned_size = size % line_size

        with open(path_name, mode) as f:
            logger.info("write file: {0}".format(path_name))
            for line_num in range(0, line_count):
                random_sting = util.generate_random_string(line_size - 2 - len(str(line_num))) + '\n'
                f.write('{line_num}:{random_s}'.format(line_num=line_num, random_s=random_sting))
            if unaligned_size > 0:
                f.write(util.generate_random_string(unaligned_size))
            f.flush()
            os.fsync(f.fileno())

        return

    def dd_read_write(self, if_path, of_path, bs, count, skip='', seek='', oflag='', timeout=1800):
        """
        dd 命令 读写
        :param if_path: read path
        :param of_path: write path
        :param bs:
        :param count:
        :param skip: read offset
        :param seek: write offset
        :param oflag: eg: direct
        :param timeout: run_cmd timeout second
        :return:
        """

        dd_cmd = "{0} if={1} of={2} bs={3} count={4}".format(DD_BINARY, if_path, of_path, bs, count)
        if oflag:
            dd_cmd += " oflag={0}".format(oflag)
        if skip:
            dd_cmd += " skip={0}".format(skip)
        if seek:
            dd_cmd += " seek={0}".format(seek)

        rc, output = run_cmd(dd_cmd, 0, tries=2, timeout=timeout)

        return rc, output


if __name__ == '__main__':
    fg = FileGenerator()
    fg.create_file('./a.txt')
