import os
import hashlib


def get_md5_value(file_path):
    """
    获取文件MD5值
    :param file_path:
    :return:
    """
    md5 = hashlib.md5()
    md5.update(file_path)
    md5sum = md5.hexdigest()
    return md5sum


def get_local_files_md5(local_path) -> dict:
    """
    获取本地文件路径下所以文件及其MD5
    :param local_path:
    :return:
    """
    files_md5 = {}
    for dir_path, dir_names, file_names in os.walk(local_path):
        for filename in file_names:
            file_full_path = os.path.join(dir_path, filename)
            files_md5[file_full_path] = {
                'name': filename,
                'md5': get_md5_value(file_full_path)
            }
    return files_md5
