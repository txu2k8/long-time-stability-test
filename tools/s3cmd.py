#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:mc
@time:2022/09/06
@email:tao.xu2008@outlook.com
@description:
"""
import json
import re

from loguru import logger
import asyncio
import subprocess

DEFAULT_S3CMD_BIN = r'python D:\\minio\\s3cmd\\s3cmd'  # s3cmd | /usr/bin/s3cmd


class S3CmdClient(object):
    def __init__(self, endpoint, access_key, secret_key, tls=False, bin_path=DEFAULT_S3CMD_BIN):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.tls = tls
        self.bin_path = bin_path

        if not endpoint.startswith('http'):
            self.endpoint = f'https://{endpoint}' if self.tls else f'http://{endpoint}'

    def _args2cmd(self, args):
        if self.tls:
            cmd = '{} --no-check-certificate {}'.format(self.bin_path, args)
        else:
            cmd = '{} {}'.format(self.bin_path, args)
        logger.debug(cmd)
        return cmd

    def _exec(self, args):
        cmd = self._args2cmd(args)
        return subprocess.getstatusoutput(cmd)

    async def _async_exec(self, args):
        cmd = self._args2cmd(args)
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        await asyncio.sleep(2)
        stdout, stderr = await proc.communicate()

        rc = proc.returncode
        if stdout:
            logger.debug(stdout.decode().strip('\n'))
        if stderr:
            stderr_decode = stderr.decode().strip('\n')
            if stderr_decode.startswith('WARNING:'):
                logger.warning(stderr_decode)
            else:
                logger.error(stderr_decode)
        return rc, stdout, stderr

    # TODO
    def configure(self):
        pass

    # TODO
    def set_core_loglevel(self, loglevel):
        pass

    def mb(self, bucket, *args, **kwargs):
        """
        s3cmd mb s3://{bucket} 创建桶
        :param bucket:
        :param args: 
        :param kwargs: 
        :return: 
        """
        _args = 'mb s3://{}'.format(bucket)

        rc, output = self._exec(_args)
        if rc == 0:
            logger.info("桶创建成功! - {}".format(bucket))
        elif "BucketAlreadyOwnedByYou" in output:
            logger.info("桶创已存在! - {}".format(bucket))
        else:
            logger.error(output)
            raise Exception("桶创建失败! - {}".format(bucket))
        return rc, output

    async def put(self, src_path, bucket, dst_path, disable_multipart=False, tags=""):
        """
        s3cmd put FILE [FILE...] s3://BUCKET[/PREFIX] 命令上传对象
        :param src_path:
        :param bucket:
        :param dst_path:
        :param disable_multipart:
        :param tags:
        :return:
        """
        # tags += "{}disable-multipart={}".format('&' if tags else '', disable_multipart)
        args = 'put {} s3://{}/{}'.format(src_path, bucket, dst_path)
        if disable_multipart:
            args += " --disable-multipart"
        rc, _, _ = await self._async_exec(args)
        if rc == 0:
            logger.info("上传成功！ {} -> {}/{}".format(src_path, bucket, dst_path))
        else:
            logger.error("上传失败！ {} -> {}/{}".format(src_path, bucket, dst_path))
        return rc

    async def get(self, bucket, obj_path, local_path, disable_multipart=False):
        """
        s3cmd get s3://BUCKET/OBJECT LOCAL_FILE 命令下载对象
        :param bucket:
        :param obj_path:
        :param local_path:
        :param disable_multipart:
        :return:
        """
        args = 'get s3://{}/{} {} --force'.format(bucket, obj_path, local_path)
        if disable_multipart:
            args += " --disable-multipart"
        rc, _, _ = await self._async_exec(args)
        if rc == 0:
            logger.info("下载成功！{}/{} -> {}".format(bucket, obj_path, local_path))
        else:
            logger.error("下载失败！{}/{} -> {}".format(bucket, obj_path, local_path))
        return rc

    async def delete(self, bucket, dst_path):
        """
        uc rm命令删除对象
        :param bucket:
        :param dst_path:
        :return:
        """
        args = 'rm {}/{}/{}'.format(self.alias, bucket, dst_path)
        rc, _, _ = await self._async_exec(args)
        if rc == 0:
            logger.info("删除成功！{}/{}".format(bucket, dst_path))
        else:
            logger.error('删除失败！{}/{}'.format(bucket, dst_path))
        return rc

    async def tag_list(self, bucket, obj_path):
        """
        获取tag列表 --TODO, S3CMD不支持
        :param bucket:
        :param obj_path:
        :return:
        """
        tag_dict = {}
        return 0, tag_dict

    async def get_obj_md5(self, bucket, obj_path):
        """
        获取tag中的md5列表
        :param bucket:
        :param obj_path:
        :return:
        """
        md5 = ''
        args = 'info s3://{}/{}'.format(bucket, obj_path)
        rc, stdout, stderr = await self._async_exec(args)
        if rc == 0:
            logger.info("获取对象信息成功！{}/{}".format(bucket, obj_path))
            md5s = re.findall(r'MD5 sum:\s+(.+)\n', stdout.decode())
            if md5s:
                md5 = md5s[0]
        else:
            logger.error("获取对象信息失败！{}/{}".format(bucket, obj_path))
        return rc, md5
