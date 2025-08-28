import os
import sys
import asyncio
from pathlib import Path
from typing import List, Optional
from mojofs.utils.path import SLASH_SEPARATOR
from mojofs.ecstore.disk.error import DiskError
from mojofs.ecstore.disk.error_conv import to_file_error


def check_path_length(path_name: str) -> None:
    """
    检查路径长度和合法性
    """
    # macOS 路径长度限制
    if sys.platform == "darwin" and len(path_name) > 1016:
        raise DiskError.FileNameTooLong

    # Windows 路径长度限制
    if sys.platform.startswith("win") and len(path_name) > 1024:
        raise DiskError.FileNameTooLong

    # Unix 下不允许 '.', '..', '/'
    invalid_paths = [".", "..", "/"]
    if path_name in invalid_paths:
        raise DiskError.FileAccessDenied

    # 检查每个路径段长度不能超过255
    count = 0
    for c in path_name:
        if c == '/':
            count = 0
        elif c == '\\' and sys.platform.startswith("win"):
            count = 0
        else:
            count += 1
            if count > 255:
                raise DiskError.FileNameTooLong

def is_root_disk(disk_path: str, root_disk: str) -> bool:
    """
    判断是否为根磁盘
    """
    if sys.platform.startswith("win"):
        return False
    # 假设有 same_disk 函数
    from mojofs.utils.os import same_disk
    try:
        return same_disk(disk_path, root_disk)
    except Exception as e:
        raise to_file_error(e)

async def make_dir_all(path: Path, base_dir: Path) -> None:
    """
    递归创建目录
    """
    check_path_length(str(path))
    try:
        await reliable_mkdir_all(path, base_dir)
    except Exception as e:
        raise to_file_error(e)

async def is_empty_dir(path: Path) -> bool:
    """
    判断目录是否为空
    """
    try:
        files = await read_dir(path, 1)
        return len(files) == 0
    except Exception:
        return False

async def read_dir(path: Path, count: int) -> List[str]:
    """
    读取目录内容，count为读取上限，0为不限制
    """
    volumes = []
    try:
        entries = [entry async for entry in await asyncio.to_thread(lambda: list(os.scandir(path)))]
    except Exception as e:
        raise e

    for entry in entries:
        name = entry.name
        if not name or name in (".", ".."):
            continue
        if entry.is_file():
            volumes.append(name)
        elif entry.is_dir():
            volumes.append(f"{name}{SLASH_SEPARATOR}")
        if count > 0:
            count -= 1
            if count == 0:
                break
    return volumes

async def rename_all(src_file_path: Path, dst_file_path: Path, base_dir: Path) -> None:
    """
    重命名文件，确保目标目录存在
    """
    await reliable_rename(src_file_path, dst_file_path, base_dir)

async def reliable_rename(src_file_path: Path, dst_file_path: Path, base_dir: Path) -> None:
    """
    可靠重命名，必要时创建父目录
    """
    parent = dst_file_path.parent
    if parent and not file_exists(parent):
        await reliable_mkdir_all(parent, base_dir)
    i = 0
    while True:
        try:
            os.rename(src_file_path, dst_file_path)
            break
        except FileNotFoundError:
            break
        except Exception as e:
            if i == 0:
                i += 1
                continue
            import warnings
            warnings.warn(
                f"reliable_rename failed. src_file_path: {src_file_path}, dst_file_path: {dst_file_path}, base_dir: {base_dir}, err: {e}"
            )
            raise e

async def reliable_mkdir_all(path: Path, base_dir: Path) -> None:
    """
    可靠递归创建目录
    """
    i = 0
    base_dir = Path(base_dir)
    while True:
        try:
            await os_mkdir_all(path, base_dir)
            break
        except FileNotFoundError as e:
            if i == 0:
                i += 1
                base_parent = base_dir.parent
                if base_parent and base_parent != base_dir.root:
                    base_dir = base_parent
                continue
            raise e
        except Exception as e:
            raise e

async def os_mkdir_all(dir_path: Path, base_dir: Path) -> None:
    """
    递归创建目录，兼容base_dir
    """
    dir_path = Path(dir_path)
    base_dir = Path(base_dir)
    if str(base_dir) and str(dir_path).startswith(str(base_dir)):
        return
    parent = dir_path.parent
    if parent and not parent.exists():
        try:
            await asyncio.to_thread(parent.mkdir, parents=True, exist_ok=True)
        except FileExistsError:
            return
        except Exception as e:
            raise e
    try:
        await asyncio.to_thread(dir_path.mkdir, exist_ok=True)
    except FileExistsError:
        return
    except Exception as e:
        raise e

def file_exists(path: Path) -> bool:
    """
    判断文件或目录是否存在
    """
    return Path(path).exists()