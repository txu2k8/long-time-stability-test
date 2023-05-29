#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author:TXU
@file:s3_api
@time:2023/3/20
@email:tao.xu2008@outlook.com
@description: S3 API接口访问
"""
import datetime
import hashlib
import hmac
import asyncio
import aiohttp
import requests
from loguru import logger


class S3API(object):
    """S3 REST API接口请求访问"""
    def __init__(self, endpoint, access_key, secret_key):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key

    @staticmethod
    def sign(key, msg):
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

    def get_signature_key(self, key, date_stamp, region_name, service_name):
        k_date = self.sign(('AWS4' + key).encode('utf-8'), date_stamp)
        k_region = self.sign(k_date, region_name)
        k_service = self.sign(k_region, service_name)
        k_signing = self.sign(k_service, 'aws4_request')
        return k_signing

    def get_header(
            self,
            host, canonical_uri='/', service='s3', region='us-east-1', access_key='admin', secret_key='admin@123',
            method='GET', request_parameters='', content_type='application/octer-stream'
    ):
        t = datetime.datetime.utcnow()
        amz_date = t.strftime('%Y%m%dT%H%M%SZ')
        date_stamp = t.strftime('%Y%m%d')
        x_amz_content_sha256 = 'UNSIGNED-PAYLOAD'
        canonical_headers = 'content-type:' + content_type + '\n' + host + '\n' + 'x-amz-content-sha256:' + x_amz_content_sha256 + '\n' + 'x-amz-date:' + amz_date + '\n'
        signed_headers = 'content-type:host;x-amz-content-sha256;x-amz-date'
        payload_hash = 'UNSIGNED-PAYLOAD'
        canonical_request = method + '\n' + canonical_uri + '\n' + request_parameters + '\n' + canonical_headers + '\n' + signed_headers + '\n' +payload_hash
        algorithm = 'AWS4-HMAC-SHA256'
        credential_scope = date_stamp + '/' + region + '/' + service + '/' + 'aws4_request'
        string_to_sign = algorithm + '\n' + amz_date + '\n' + credential_scope + '\n' + hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
        signing_key = self.get_signature_key(secret_key, date_stamp, region, service)
        signature = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
        authorization_header = algorithm + ' ' + 'Credential=' + access_key + '/' + credential_scope + ', ' + 'SignedHeaders=' + signed_headers +', ' + 'Signature' + signature
        headers = {
            'content-type': content_type,
            'x-amz-date': amz_date,
            'Authorization': authorization_header,
            'x-amz-content-sha256': x_amz_content_sha256
        }
        return headers

    def append_write(self, endpoint, src_path, bucket, dst_path, position=0, data=None, timeout=180):
        host = endpoint.split("//")[-1]
        obj_path = f"{bucket}/{dst_path}"
        if not data:
            with open(src_path, "rb") as f:
                data = f.read()
        parameters = f"append=true&position={position}"
        headers = self.get_header(host, canonical_uri=obj_path, method='PUT', request_parameters=parameters)
        url = endpoint + f"{obj_path}?{parameters}"
        logger.info(url)
        start = datetime.datetime.now()
        resp = requests.put(url=url, data=data, headers=headers, verify=False, timeout=timeout)
        end = datetime.datetime.now()
        elapsed = (end - start).total_seconds()
        if resp.status_code == 200:
            position += len(data)
        else:
            logger.error('Response({}):\n{}'.format(url, resp.text))
            logger.error("追加写失败！{}-->> {}/{}，耗时：{} s".format(src_path, bucket, dst_path, elapsed))
        return position, elapsed

    async def append_write_async(self, endpoint, src_path, bucket, dst_path, position=0, data=None, segments=1, timeout=180):
        host = endpoint.split("//")[-1]
        obj_path = f"{bucket}/{dst_path}"
        if not data:
            with open(src_path, "rb") as f:
                data = f.read()
        elapsed = 0
        for _ in range(0, segments):
            parameters = f"append=true&position={position}"
            headers = self.get_header(host, canonical_uri=obj_path, method='PUT', request_parameters=parameters)
            url = endpoint + f"{obj_path}?{parameters}"
            logger.info(url)
            start = datetime.datetime.now()
            async with aiohttp.ClientSession() as session:
                async with session.put(url=url, data=data, headers=headers, timeout=timeout) as resp:
                    status_code = resp.status
                    resp_text = await resp.text()

            end = datetime.datetime.now()
            elapsed = (end - start).total_seconds()
            if status_code == 200:
                position += len(data)
            else:
                logger.error('Response({}):\n{}'.format(url, resp_text))
                logger.error("追加写失败！{}-->> {}/{}，耗时：{} s".format(src_path, bucket, dst_path, elapsed))
        return position, elapsed


if __name__ == '__main__':
    file_path = "D:\\minio\\upload\\mp4\\128MB.mp4"
    s3_api = S3API()
    p = 0
    # for _ in range(0, 2):
    #     p, e = s3_api.append_write("http://127.0.0.1:9000", file_path, "bucket1", "2023-3-16/data001", p, data=None)

    asyncio.run(s3_api.append_write_async("http://127.0.0.1:9000", file_path, "bucket1", "2023-3-16/data001", p, data=None, segments=2))
