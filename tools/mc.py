from loguru import logger

import subprocess

DEFAULT_MC_BIN = r'D:\minio\mc.exe'  # mc | mc.exe


class MClient(object):

    _alias = None

    def __init__(self, endpoint, access_key, secret_key, tls=False, alias='lts', bin_path=DEFAULT_MC_BIN):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.tls = tls
        self.alias = alias
        self.bin_path = bin_path
        if not self._alias:
            self.set_alias()

    def _exec(self, args):
        if self.tls:
            cmd = '{} --insecure {}'.format(self.bin_path, args)
        else:
            cmd = '{} {}'.format(self.bin_path, args)
        logger.debug(cmd)
        return subprocess.getstatusoutput(cmd)

    def set_alias(self):
        args = "alias set {} {} {} {}".format(self.alias, self.endpoint, self.access_key, self.secret_key)
        rc, output = self._exec(args)
        if rc == 0:
            logger.info("设置alias成功 -- {}".format(self.alias))
        else:
            logger.error(output)
            raise Exception("设置alias失败 -- {}".format(self.alias))
        return rc, output

    def admin_config_set(self, target, kv):
        args = 'admin config set {} {} {}'.format(self.alias, target, kv)

        rc, output = self._exec(args)
        if rc == 0:
            logger.info("设置config成功 -- {} {}".format(target, kv))
        else:
            logger.error(output)
            raise Exception("设置config失败 -- {} {}".format(target, kv))
        return rc, output

    def set_core_loglevel(self, loglevel):
        return self.admin_config_set('loglevel', f'loglevel={loglevel}')

    def mb(self, bucket):
        args = 'mb --ignore-existing {}/{}'.format(self.alias, bucket)

        rc, output = self._exec(args)
        if rc == 0:
            logger.info("桶创建成功! - {}".format(bucket))
        else:
            logger.error(output)
            raise Exception("桶创建失败! - {}".format(bucket))
        return rc, output

    def put(self, src_path, bucket, dst_path, disable_multipart=False, tags=""):
        """
        uc cp命令上传对象
        :param src_path:
        :param bucket:
        :param dst_path:
        :param disable_multipart:
        :param tags:
        :return:
        """
        tags += "{}disable-multipart={}".format('&' if tags else '', disable_multipart)
        args = 'cp --tags "{}" {} {}/{}/{}'.format(tags, src_path, self.alias, bucket, dst_path)
        if disable_multipart:
            args += " --disable-multipart"
        rc, output = self._exec(args)
        return rc, output

    def get(self, bucket, obj_path, local_path, disable_multipart=False, tags=""):
        """
        uc cp命令下载对象
        :param bucket:
        :param obj_path:
        :param local_path:
        :param disable_multipart:
        :param tags:
        :return:
        """
        args = 'cp {}/{}/{} {}'.format(self.alias, bucket, obj_path, local_path)
        if disable_multipart:
            args += " --disable-multipart"
        rc, output = self._exec(args)
        return rc, output
