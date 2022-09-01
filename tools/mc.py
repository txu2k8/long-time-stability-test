from loguru import logger

import subprocess

DEFAULT_MC_BIN = r'D:\docker\mc.exe'  # mc | mc.exe


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
        args = 'admin config set {} {} {}'.format(self.bin_path, self.alias, target, kv)

        rc, output = self._exec(args)
        if rc == 0:
            logger.info("设置config成功 -- {} {}".format(target, kv))
        else:
            logger.error(output)
            raise Exception("设置config失败 -- {} {}".format(target, kv))
        return rc, output

    def set_core_loglevel(self, loglevel):
        return self.admin_config_set('loglevel', f'loglevel={loglevel}')

    def cp(self, src_path, bucket, dst_path, disable_multipart=False, tags=""):
        tags += "{}disable-multipart={}".format('&' if tags else '', disable_multipart)
        args = 'cp --tags "{}" {} {}/{}/{}'.format(tags, src_path, self.alias, bucket, dst_path)
        if disable_multipart:
            args += " --disable-multipart"
        rc, output = self._exec(args)
        return rc, output

