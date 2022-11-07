#!/usr/bin/python
# -*- coding:utf-8 _*-
"""
@author:TXU
@file:mc
@time:2022/09/06
@email:tao.xu2008@outlook.com
@description:
"""
import os
import json
import re
import datetime
from abc import ABC

from loguru import logger
import asyncio
import subprocess

from client.client_interface import ClientInterface

# --- OS constants
POSIX = os.name == "posix"
WINDOWS = os.name == "nt"
DEFAULT_MC_BIN = r'D:\minio\mc.exe' if WINDOWS else 'mc'  # mc | mc.exe


class MClient(ClientInterface, ABC):

    _alias = None

    def __init__(self, endpoint, access_key, secret_key, tls=False, alias='play', bin_path=DEFAULT_MC_BIN):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.tls = tls
        self.alias = alias
        self.bin_path = bin_path

        if not endpoint.startswith('http'):
            self.endpoint = f'https://{endpoint}' if self.tls else f'http://{endpoint}'

        if not self._alias:
            self.set_alias()

    def _args2cmd(self, args):
        if self.tls:
            cmd = '{} --insecure {}'.format(self.bin_path, args)
        else:
            cmd = '{} {}'.format(self.bin_path, args)
        logger.log('MC', cmd)
        return cmd

    def _exec(self, args):
        cmd = self._args2cmd(args)
        rc, output = subprocess.getstatusoutput(cmd)
        logger.debug(output.strip('\n'))
        return rc, output

    async def _async_exec(self, args):
        cmd = self._args2cmd(args)
        start = datetime.datetime.now()
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        end = datetime.datetime.now()
        elapsed = (end - start).total_seconds()  # 耗时 x.y 秒
        rc = proc.returncode
        if stdout:
            logger.debug(stdout.decode().strip('\n'))
        if stderr:
            logger.error('Response({}):\n{}'.format(cmd, stderr.decode().strip('\n')))
        return rc, elapsed, stdout, stderr

    def set_alias(self):
        args = "alias set {} {} {} {}".format(self.alias, self.endpoint, self.access_key, self.secret_key)
        rc, output = self._exec(args)
        if rc == 0:
            logger.success("设置alias成功 -- {}".format(self.alias))
        else:
            logger.error(output)
            raise Exception("设置alias失败 -- {}".format(self.alias))
        return rc, output

    def admin_config_set(self, target, kv):
        args = 'admin config set {} {} {}'.format(self.alias, target, kv)

        rc, output = self._exec(args)
        if rc == 0:
            logger.success("设置config成功 -- {} {}".format(target, kv))
        else:
            logger.error(output)
            raise Exception("设置config失败 -- {} {}".format(target, kv))
        return rc, output

    def set_core_loglevel(self, loglevel):
        return self.admin_config_set('loglevel', f'loglevel={loglevel}')

    def mb(self, bucket, *args, **kwargs):
        uc_args = 'mb --ignore-existing {}/{}'.format(self.alias, bucket)

        rc, output = self._exec(uc_args)
        if rc == 0:
            logger.success("桶创建成功! - {}".format(bucket))
        else:
            logger.error(output)
            raise Exception("桶创建失败! - {}".format(bucket))
        return rc, output

    def put(self, src_path, bucket, dst_path, disable_multipart=False, tags="", attr=""):
        """
        mc cp命令上传对象
        :param src_path:
        :param bucket:
        :param dst_path:
        :param disable_multipart:
        :param tags:
        :param attr:
        :return:
        """
        tags += "{}disable-multipart={}".format('&' if tags else '', disable_multipart)
        attr += "{}disable-multipart={}".format(';' if attr else '', disable_multipart)
        cp = 'cp --disable-multipart' if disable_multipart else 'cp'
        args = '{} --tags "{}" --attr "{}" {} {}/{}/{}'.format(cp, tags, attr, src_path, self.alias, bucket, dst_path)
        start = datetime.datetime.now()
        rc, stdout = self._exec(args)
        end = datetime.datetime.now()
        elapsed = (end - start).total_seconds()  # 耗时 x.y 秒
        if rc == 0:
            logger.success("上传成功！{} -> {}/{}，耗时：{}".format(src_path, bucket, dst_path, elapsed))
            logger.log("OBJ", "{}/{}".format(bucket, dst_path))
        else:
            logger.error("上传失败！{} -> {}/{}，耗时：{}".format(src_path, bucket, dst_path, elapsed))
        return rc, elapsed

    async def put_without_attr(self, src_path, bucket, dst_path, disable_multipart=False, tags=""):
        """
        mc cp命令上传对象
        :param src_path:
        :param bucket:
        :param dst_path:
        :param disable_multipart:
        :param tags:
        :return:
        """
        tags += "{}disable-multipart={}".format('&' if tags else '', disable_multipart)
        cp = 'cp --disable-multipart' if disable_multipart else 'cp'
        args = '{} --tags "{}" {} {}/{}/{}'.format(cp, tags, src_path, self.alias, bucket, dst_path)
        rc, elapsed, _, _ = await self._async_exec(args)
        if rc == 0:
            logger.success("上传成功！{} -> {}/{}，耗时：{}".format(src_path, bucket, dst_path, elapsed))
            logger.log("OBJ", "{}/{}".format(bucket, dst_path))
        else:
            logger.error("上传失败！{} -> {}/{}，耗时：{}".format(src_path, bucket, dst_path, elapsed))
        return rc, elapsed

    async def get(self, bucket, obj_path, local_path, disable_multipart=False):
        """
        mc cp命令下载对象
        :param bucket:
        :param obj_path:
        :param local_path:
        :param disable_multipart:
        :return:
        """
        args = 'cp {}/{}/{} {}'.format(self.alias, bucket, obj_path, local_path)
        if disable_multipart:
            args += " --disable-multipart"
        rc, _, _, _ = await self._async_exec(args)
        if rc == 0:
            logger.success("下载成功！{}/{} -> {}".format(bucket, obj_path, local_path))
        else:
            logger.error("下载失败！{}/{} -> {}".format(bucket, obj_path, local_path))
        return rc

    async def delete(self, bucket, dst_path):
        """
        mc rm命令删除对象
        :param bucket:
        :param dst_path:
        :return:
        """
        args = 'rm {}/{}/{}'.format(self.alias, bucket, dst_path)
        rc, _, _, _ = await self._async_exec(args)
        if rc == 0:
            logger.success("删除成功！{}/{}".format(bucket, dst_path))
            logger.log("OBJ", "{}/{}".format(bucket, dst_path))
        else:
            logger.error('删除失败！{}/{}'.format(bucket, dst_path))
        return rc

    async def list(self, bucket, obj_path):
        """
        mc ls 命令 列表查询对象
        :param bucket:
        :param obj_path:
        :return:
        """
        args = 'ls {}/{}/{}'.format(self.alias, bucket, obj_path)
        rc, _, _, _ = await self._async_exec(args)
        if rc == 0:
            logger.success("列表对象成功！{}/{}".format(bucket, obj_path))
        else:
            logger.error("列表对象失败！{}/{}".format(bucket, obj_path))
        return rc

    async def tag_list(self, bucket, obj_path):
        """
        获取tag列表
        :param bucket:
        :param obj_path:
        :return:
        """
        tag_dict = {}
        args = 'tag list {}/{}/{} --json'.format(self.alias, bucket, obj_path)
        rc, _, stdout, stderr = await self._async_exec(args)
        if rc == 0:
            logger.success("获取对象标签成功！{}/{}".format(bucket, obj_path))
            json_output = json.loads(stdout.decode().strip('\n'))
            if json_output['status'] == 'success':
                tag_dict = json_output['tagset']
        else:
            logger.error("获取对象标签失败！{}/{}".format(bucket, obj_path))
        return rc, tag_dict

    async def get_obj_md5_by_tag(self, bucket, obj_path):
        """
        获取tag中的md5列表
        :param bucket:
        :param obj_path:
        :return:
        """
        md5 = ''
        args = 'tag list {}/{}/{} --json'.format(self.alias, bucket, obj_path)
        rc, _, stdout, stderr = await self._async_exec(args)
        if rc == 0:
            logger.success("获取对象标签成功！{}/{}".format(bucket, obj_path))
            json_output = json.loads(stdout.decode().strip('\n'))
            if json_output['status'] == 'success' and 'tagset' in json_output:
                tag_dict = json_output['tagset']
                if 'md5' in tag_dict:
                    md5 = tag_dict['md5']
        else:
            logger.error("获取对象标签失败！{}/{}".format(bucket, obj_path))
        return rc, md5

    async def get_obj_md5_by_attr(self, bucket, obj_path):
        """
        获取attr中的md5
        :param bucket:
        :param obj_path:
        :return:
        """
        md5 = ''
        args = 'stat {}/{}/{} --json'.format(self.alias, bucket, obj_path)
        rc, _, stdout, stderr = await self._async_exec(args)
        if rc == 0:
            logger.success("获取对象stat信息成功！{}/{}".format(bucket, obj_path))
            json_output = json.loads(stdout.decode().strip('\n'))
            if json_output['status'] == 'success':
                metadata_dict = json_output['metadata']
                if 'X-Amz-Meta-Md5' in metadata_dict:
                    md5 = metadata_dict['X-Amz-Meta-Md5']
                elif 'X-Amz-Meta-S3cmd-Attrs' in metadata_dict:
                    s3cmd_attrs = metadata_dict['X-Amz-Meta-S3cmd-Attrs']
                    for item in s3cmd_attrs.split('/'):
                        k, v = item.split(':')
                        if k == 'md5':
                            md5 = v
                            break
        else:
            logger.error("获取对象信息失败！{}/{}".format(bucket, obj_path))
        return rc, md5

    async def get_obj_md5(self, bucket, obj_path):
        rc, md5 = await self.get_obj_md5_by_tag(bucket, obj_path)
        return rc, md5

    def delete_bucket_objs(self, bucket):
        """
        mc rm命令删除桶中所有对象
        :param bucket:
        :return:
        """
        args = 'rm --recursive --force --dangerous {}/{}'.format(self.alias, bucket)
        rc, output = self._exec(args)
        if rc == 0:
            logger.success("删除成功！{}/{}".format(self.alias, bucket))
        return rc, output

    def get_all_buckets(self):
        """
        获取所有桶列表
        :return:
        """
        buckets = []
        args = 'ls {}'.format(self.alias)
        rc, output = self._exec(args)
        if rc == 0:
            logger.success("桶列表成功！{}/*".format(self.alias))
            for b in output.split("\n"):
                bucket_names = re.findall(r"0B\s+(.*)/", b)
                bucket_name = bucket_names[0] if bucket_names else ""
                if bucket_name:
                    buckets.append(bucket_name)
        return buckets

    def get_drives_num(self):
        """
        获取集群中每个节点的drive数量
        :return:
        """
        drives_num = 0
        args = 'admin info {} | grep Drives'.format(self.alias)
        rc, output = self._exec(args)
        if rc == 0:
            logger.success("集群Drive信息：{}/*".format(output))
            drives_nums = re.findall(r"Drives: (\d+)/", output.strip("\n")[0])
            drives_num = int(drives_nums[0] if drives_nums else 0)
        return drives_num
