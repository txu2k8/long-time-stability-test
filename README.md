# long-time-stability-test
长稳测试


# 快速开始
## 获取项目
```shell
git clone xxx.git
```

## 安装依赖库
```shell
pip3 install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt
```

## 命令执行
```shell
python3 main.py --help
```

# 二进制打包
## Windows
```shell
pip install pyinstaller
pyinstaller -F -w main.py
```

## Linux


# 下载依赖包到本地
```shell
pip download -i https://pypi.tuna.tsinghua.edu.cn/simple -d packages -r requirements.txt
pip install --no-index --find-links=packages -r requirements.txt
```
