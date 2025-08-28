import os
import shutil
import stat
from pathlib import Path
from typing import Union, Optional

O_RDONLY = 0x00000
O_WRONLY = 0x00001
O_RDWR   = 0x00002
O_CREATE = 0x00040
O_TRUNC  = 0x00200
O_APPEND = 0x00400

FileMode = int

def same_file(f1: os.stat_result, f2: os.stat_result) -> bool:
    """
    判断两个文件stat结果是否为同一个文件
    """
    if os.name == 'nt':
        # Windows: 只比较mode, file size, file type
        if stat.S_IMODE(f1.st_mode) != stat.S_IMODE(f2.st_mode):
            return False
        if stat.S_IFMT(f1.st_mode) != stat.S_IFMT(f2.st_mode):
            return False
        if f1.st_size != f2.st_size:
            return False
        return True
    else:
        # Unix: 比较设备号、inode、大小、权限、修改时间
        if f1.st_dev != f2.st_dev:
            return False
        if f1.st_ino != f2.st_ino:
            return False
        if f1.st_size != f2.st_size:
            return False
        if stat.S_IMODE(f1.st_mode) != stat.S_IMODE(f2.st_mode):
            return False
        if int(f1.st_mtime) != int(f2.st_mtime):
            return False
        return True

def _mode_to_flags(mode: FileMode) -> str:
    """
    将FileMode转换为Python open的mode字符串
    """
    if (mode & O_RDWR) == O_RDWR:
        base = 'r+'
    elif (mode & O_WRONLY) == O_WRONLY:
        base = 'w' if (mode & O_TRUNC) else 'a' if (mode & O_APPEND) else 'w'
    else:
        base = 'r'

    if (mode & O_CREATE) and 'w' not in base and 'a' not in base:
        base = 'w+'
    if (mode & O_APPEND):
        if 'a' not in base:
            base = base.replace('w', 'a')
    if (mode & O_TRUNC):
        if 'w' not in base:
            base = base.replace('a', 'w')
    return base + 'b'

def open_file(path: Union[str, Path], mode: FileMode):
    """
    打开文件，返回文件对象
    """
    flags = _mode_to_flags(mode)
    return open(path, flags)

def access(path: Union[str, Path]) -> bool:
    """
    检查文件是否存在
    """
    return Path(path).exists()

def access_std(path: Union[str, Path]) -> bool:
    """
    同步检查文件是否存在
    """
    return Path(path).exists()

def lstat(path: Union[str, Path]) -> os.stat_result:
    """
    获取文件stat信息
    """
    return os.stat(path)

def lstat_std(path: Union[str, Path]) -> os.stat_result:
    """
    同步获取文件stat信息
    """
    return os.stat(path)

def make_dir_all(path: Union[str, Path]):
    """
    递归创建目录
    """
    Path(path).mkdir(parents=True, exist_ok=True)

def remove(path: Union[str, Path]):
    """
    删除文件或空目录
    """
    p = Path(path)
    if p.is_dir():
        os.rmdir(p)
    else:
        os.remove(p)

def remove_all(path: Union[str, Path]):
    """
    递归删除文件或目录
    """
    p = Path(path)
    if p.is_dir():
        shutil.rmtree(p)
    else:
        os.remove(p)

def remove_std(path: Union[str, Path]):
    """
    同步删除文件或空目录
    """
    remove(path)

def remove_all_std(path: Union[str, Path]):
    """
    同步递归删除文件或目录
    """
    remove_all(path)

def mkdir(path: Union[str, Path]):
    """
    创建目录
    """
    Path(path).mkdir(exist_ok=True)

def rename(src: Union[str, Path], dst: Union[str, Path]):
    """
    重命名文件或目录
    """
    os.rename(src, dst)

def rename_std(src: Union[str, Path], dst: Union[str, Path]):
    """
    同步重命名文件或目录
    """
    os.rename(src, dst)

def read_file(path: Union[str, Path]) -> bytes:
    """
    读取文件内容，返回bytes
    """
    with open(path, 'rb') as f:
        return f.read()