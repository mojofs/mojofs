import os
import site

def add_module_to_path():
    """将当前模块添加到Python搜索路径"""
    # 获取当前文件所在目录
    project_root = os.path.dirname(os.path.abspath(__file__))
    # 获取site-packages目录路径
    site_packages_path = None
    for path in site.getsitepackages():
        print(path)
        if "site-packages" in path:
            site_packages_path = path
            break
    # 将项目根路径写入askkb.pth文件
    pth_file_path = os.path.join(site_packages_path, "python.pth")
    with open(pth_file_path, "w", encoding="utf-8") as f:
        f.write(project_root)

# 导出常用模块内容（如有__all__可自动导出，否则需手动指定）
from .certs import *
from .ip import *
from .net import *
from .retry import *
from .io import *
from .hash import *
from .os import *
from .path import *
from .string_utils import *
from .crypto import *
from .compress import *
from .dirs import *
from .sys import *

# notify模块为可选模块，若存在则导入
try:
    from . import notify
    from .notify import *
except ImportError:
    pass