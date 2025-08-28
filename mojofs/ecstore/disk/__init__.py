"""
磁盘存储模块

该模块提供了磁盘存储的抽象接口和实现，支持本地磁盘和远程磁盘操作。
"""

import asyncio
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# 常量定义
RUSTFS_META_BUCKET = ".rustfs.sys"
RUSTFS_META_MULTIPART_BUCKET = ".rustfs.sys/multipart"
RUSTFS_META_TMP_BUCKET = ".rustfs.sys/tmp"
RUSTFS_META_TMP_DELETED_BUCKET = ".rustfs.sys/tmp/.trash"
BUCKET_META_PREFIX = "buckets"
FORMAT_CONFIG_FILE = "format.json"
STORAGE_FORMAT_FILE = "xl.meta"
STORAGE_FORMAT_FILE_BACKUP = "xl.meta.bkp"


class CheckPartStatus(IntEnum):
    """检查部分状态枚举"""
    UNKNOWN = 0
    SUCCESS = 1
    DISK_NOT_FOUND = 2
    VOLUME_NOT_FOUND = 3
    FILE_NOT_FOUND = 4
    FILE_CORRUPT = 5


@dataclass
class DiskLocation:
    """磁盘位置信息"""
    pool_idx: Optional[int] = None
    set_idx: Optional[int] = None
    disk_idx: Optional[int] = None
    
    def valid(self) -> bool:
        """检查位置信息是否有效"""
        return all(x is not None for x in [self.pool_idx, self.set_idx, self.disk_idx])


@dataclass
class DiskOption:
    """磁盘选项"""
    cleanup: bool = False
    health_check: bool = False


@dataclass
class DiskInfoOptions:
    """磁盘信息选项"""
    disk_id: str = ""
    metrics: bool = False
    noop: bool = False


@dataclass
class DiskInfo:
    """磁盘信息"""
    total: int = 0
    free: int = 0
    used: int = 0
    used_inodes: int = 0
    free_inodes: int = 0
    major: int = 0
    minor: int = 0
    nr_requests: int = 0
    fs_type: str = ""
    root_disk: bool = False
    healing: bool = False
    scanning: bool = False
    endpoint: str = ""
    mount_path: str = ""
    id: str = ""
    rotational: bool = False
    metrics: Dict[str, Any] = field(default_factory=dict)
    error: str = ""


@dataclass
class VolumeInfo:
    """卷信息"""
    name: str
    created: Optional[datetime] = None


@dataclass
class DeleteOptions:
    """删除选项"""
    recursive: bool = False
    immediate: bool = False
    undo_write: bool = False
    old_data_dir: Optional[uuid.UUID] = None


@dataclass
class ReadOptions:
    """读取选项"""
    incl_free_versions: bool = False
    read_data: bool = False
    healing: bool = False


@dataclass
class UpdateMetadataOpts:
    """更新元数据选项"""
    no_persistence: bool = False


@dataclass
class WalkDirOptions:
    """遍历目录选项"""
    bucket: str = ""
    base_dir: str = ""
    recursive: bool = False
    report_notfound: bool = False
    filter_prefix: Optional[str] = None
    forward_to: Optional[str] = None
    limit: int = 0
    disk_id: str = ""


@dataclass
class CheckPartsResp:
    """检查部分响应"""
    results: List[int] = field(default_factory=list)


@dataclass
class RenameDataResp:
    """重命名数据响应"""
    old_data_dir: Optional[uuid.UUID] = None
    sign: Optional[bytes] = None


@dataclass
class ReadMultipleReq:
    """批量读取请求"""
    bucket: str = ""
    prefix: str = ""
    files: List[str] = field(default_factory=list)
    max_size: int = 0
    metadata_only: bool = False
    abort404: bool = False
    max_results: int = 0


@dataclass
class ReadMultipleResp:
    """批量读取响应"""
    bucket: str = ""
    prefix: str = ""
    file: str = ""
    exists: bool = False
    error: str = ""
    data: bytes = b""
    mod_time: Optional[datetime] = None


@dataclass
class FileInfoVersions:
    """文件信息版本"""
    volume: str = ""
    name: str = ""
    latest_mod_time: Optional[datetime] = None
    versions: List[Any] = field(default_factory=list)  # FileInfo类型
    free_versions: List[Any] = field(default_factory=list)  # FileInfo类型
    
    def find_version_index(self, v: str) -> Optional[int]:
        """查找版本索引"""
        if not v:
            return None
        
        try:
            vid = uuid.UUID(v)
        except ValueError:
            return None
        
        for i, version in enumerate(self.versions):
            if version.version_id == vid:
                return i
        return None


class DiskAPI(ABC):
    """磁盘API抽象基类"""
    
    @abstractmethod
    def to_string(self) -> str:
        """转换为字符串表示"""
        pass
    
    @abstractmethod
    async def is_online(self) -> bool:
        """检查是否在线"""
        pass
    
    @abstractmethod
    def is_local(self) -> bool:
        """检查是否为本地磁盘"""
        pass
    
    @abstractmethod
    def host_name(self) -> str:
        """获取主机名"""
        pass
    
    @abstractmethod
    def endpoint(self) -> Any:  # Endpoint类型
        """获取端点"""
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """关闭连接"""
        pass
    
    @abstractmethod
    async def get_disk_id(self) -> Optional[uuid.UUID]:
        """获取磁盘ID"""
        pass
    
    @abstractmethod
    async def set_disk_id(self, id: Optional[uuid.UUID]) -> None:
        """设置磁盘ID"""
        pass
    
    @abstractmethod
    def path(self) -> Path:
        """获取路径"""
        pass
    
    @abstractmethod
    def get_disk_location(self) -> DiskLocation:
        """获取磁盘位置"""
        pass
    
    # 卷操作
    @abstractmethod
    async def make_volume(self, volume: str) -> None:
        """创建卷"""
        pass
    
    @abstractmethod
    async def make_volumes(self, volumes: List[str]) -> None:
        """批量创建卷"""
        pass
    
    @abstractmethod
    async def list_volumes(self) -> List[VolumeInfo]:
        """列出所有卷"""
        pass
    
    @abstractmethod
    async def stat_volume(self, volume: str) -> VolumeInfo:
        """获取卷状态"""
        pass
    
    @abstractmethod
    async def delete_volume(self, volume: str) -> None:
        """删除卷"""
        pass
    
    @abstractmethod
    async def walk_dir(self, opts: WalkDirOptions, writer: Any) -> None:
        """遍历目录"""
        pass
    
    # 元数据操作
    @abstractmethod
    async def delete_version(
        self,
        volume: str,
        path: str,
        fi: Any,  # FileInfo类型
        force_del_marker: bool,
        opts: DeleteOptions
    ) -> None:
        """删除版本"""
        pass
    
    @abstractmethod
    async def delete_versions(
        self,
        volume: str,
        versions: List[FileInfoVersions],
        opts: DeleteOptions
    ) -> List[Optional[Exception]]:
        """批量删除版本"""
        pass
    
    @abstractmethod
    async def delete_paths(self, volume: str, paths: List[str]) -> None:
        """删除路径"""
        pass
    
    @abstractmethod
    async def write_metadata(
        self,
        org_volume: str,
        volume: str,
        path: str,
        fi: Any  # FileInfo类型
    ) -> None:
        """写入元数据"""
        pass
    
    @abstractmethod
    async def update_metadata(
        self,
        volume: str,
        path: str,
        fi: Any,  # FileInfo类型
        opts: UpdateMetadataOpts
    ) -> None:
        """更新元数据"""
        pass
    
    @abstractmethod
    async def read_version(
        self,
        org_volume: str,
        volume: str,
        path: str,
        version_id: str,
        opts: ReadOptions
    ) -> Any:  # FileInfo类型
        """读取版本"""
        pass
    
    @abstractmethod
    async def read_xl(self, volume: str, path: str, read_data: bool) -> Any:  # RawFileInfo类型
        """读取XL元数据"""
        pass
    
    @abstractmethod
    async def rename_data(
        self,
        src_volume: str,
        src_path: str,
        file_info: Any,  # FileInfo类型
        dst_volume: str,
        dst_path: str
    ) -> RenameDataResp:
        """重命名数据"""
        pass
    
    # 文件操作
    @abstractmethod
    async def list_dir(
        self,
        origvolume: str,
        volume: str,
        dir_path: str,
        count: int
    ) -> List[str]:
        """列出目录内容"""
        pass
    
    @abstractmethod
    async def read_file(self, volume: str, path: str) -> Any:  # FileReader类型
        """读取文件"""
        pass
    
    @abstractmethod
    async def read_file_stream(
        self,
        volume: str,
        path: str,
        offset: int,
        length: int
    ) -> Any:  # FileReader类型
        """流式读取文件"""
        pass
    
    @abstractmethod
    async def append_file(self, volume: str, path: str) -> Any:  # FileWriter类型
        """追加文件"""
        pass
    
    @abstractmethod
    async def create_file(
        self,
        origvolume: str,
        volume: str,
        path: str,
        file_size: int
    ) -> Any:  # FileWriter类型
        """创建文件"""
        pass
    
    @abstractmethod
    async def rename_file(
        self,
        src_volume: str,
        src_path: str,
        dst_volume: str,
        dst_path: str
    ) -> None:
        """重命名文件"""
        pass
    
    @abstractmethod
    async def rename_part(
        self,
        src_volume: str,
        src_path: str,
        dst_volume: str,
        dst_path: str,
        meta: bytes
    ) -> None:
        """重命名部分"""
        pass
    
    @abstractmethod
    async def delete(self, volume: str, path: str, opt: DeleteOptions) -> None:
        """删除"""
        pass
    
    @abstractmethod
    async def verify_file(self, volume: str, path: str, fi: Any) -> CheckPartsResp:
        """验证文件"""
        pass
    
    @abstractmethod
    async def check_parts(self, volume: str, path: str, fi: Any) -> CheckPartsResp:
        """检查部分"""
        pass
    
    @abstractmethod
    async def read_parts(self, bucket: str, paths: List[str]) -> List[Any]:  # ObjectPartInfo类型
        """读取部分"""
        pass
    
    @abstractmethod
    async def read_multiple(self, req: ReadMultipleReq) -> List[ReadMultipleResp]:
        """批量读取"""
        pass
    
    @abstractmethod
    async def write_all(self, volume: str, path: str, data: bytes) -> None:
        """写入所有数据"""
        pass
    
    @abstractmethod
    async def read_all(self, volume: str, path: str) -> bytes:
        """读取所有数据"""
        pass
    
    @abstractmethod
    async def disk_info(self, opts: DiskInfoOptions) -> DiskInfo:
        """获取磁盘信息"""
        pass


class Disk:
    """磁盘实现类"""
    
    def __init__(self, impl: Union['LocalDisk', 'RemoteDisk']):
        self._impl = impl
        self._is_local = isinstance(impl, LocalDisk)
    
    def to_string(self) -> str:
        return self._impl.to_string()
    
    async def is_online(self) -> bool:
        return await self._impl.is_online()
    
    def is_local(self) -> bool:
        return self._is_local
    
    def host_name(self) -> str:
        return self._impl.host_name()
    
    def endpoint(self) -> Any:
        return self._impl.endpoint()
    
    async def close(self) -> None:
        await self._impl.close()
    
    async def get_disk_id(self) -> Optional[uuid.UUID]:
        return await self._impl.get_disk_id()
    
    async def set_disk_id(self, id: Optional[uuid.UUID]) -> None:
        await self._impl.set_disk_id(id)
    
    def path(self) -> Path:
        return self._impl.path()
    
    def get_disk_location(self) -> DiskLocation:
        return self._impl.get_disk_location()
    
    async def make_volume(self, volume: str) -> None:
        await self._impl.make_volume(volume)
    
    async def make_volumes(self, volumes: List[str]) -> None:
        await self._impl.make_volumes(volumes)
    
    async def list_volumes(self) -> List[VolumeInfo]:
        return await self._impl.list_volumes()
    
    async def stat_volume(self, volume: str) -> VolumeInfo:
        return await self._impl.stat_volume(volume)
    
    async def delete_volume(self, volume: str) -> None:
        await self._impl.delete_volume(volume)
    
    async def walk_dir(self, opts: WalkDirOptions, writer: Any) -> None:
        await self._impl.walk_dir(opts, writer)
    
    async def delete_version(
        self,
        volume: str,
        path: str,
        fi: Any,
        force_del_marker: bool,
        opts: DeleteOptions
    ) -> None:
        await self._impl.delete_version(volume, path, fi, force_del_marker, opts)
    
    async def delete_versions(
        self,
        volume: str,
        versions: List[FileInfoVersions],
        opts: DeleteOptions
    ) -> List[Optional[Exception]]:
        return await self._impl.delete_versions(volume, versions, opts)
    
    async def delete_paths(self, volume: str, paths: List[str]) -> None:
        await self._impl.delete_paths(volume, paths)
    
    async def write_metadata(
        self,
        org_volume: str,
        volume: str,
        path: str,
        fi: Any
    ) -> None:
        await self._impl.write_metadata(org_volume, volume, path, fi)
    
    async def update_metadata(
        self,
        volume: str,
        path: str,
        fi: Any,
        opts: UpdateMetadataOpts
    ) -> None:
        await self._impl.update_metadata(volume, path, fi, opts)
    
    async def read_version(
        self,
        org_volume: str,
        volume: str,
        path: str,
        version_id: str,
        opts: ReadOptions
    ) -> Any:
        return await self._impl.read_version(org_volume, volume, path, version_id, opts)
    
    async def read_xl(self, volume: str, path: str, read_data: bool) -> Any:
        return await self._impl.read_xl(volume, path, read_data)
    
    async def rename_data(
        self,
        src_volume: str,
        src_path: str,
        file_info: Any,
        dst_volume: str,
        dst_path: str
    ) -> RenameDataResp:
        return await self._impl.rename_data(src_volume, src_path, file_info, dst_volume, dst_path)
    
    async def list_dir(
        self,
        origvolume: str,
        volume: str,
        dir_path: str,
        count: int
    ) -> List[str]:
        return await self._impl.list_dir(origvolume, volume, dir_path, count)
    
    async def read_file(self, volume: str, path: str) -> Any:
        return await self._impl.read_file(volume, path)
    
    async def read_file_stream(
        self,
        volume: str,
        path: str,
        offset: int,
        length: int
    ) -> Any:
        return await self._impl.read_file_stream(volume, path, offset, length)
    
    async def append_file(self, volume: str, path: str) -> Any:
        return await self._impl.append_file(volume, path)
    
    async def create_file(
        self,
        origvolume: str,
        volume: str,
        path: str,
        file_size: int
    ) -> Any:
        return await self._impl.create_file(origvolume, volume, path, file_size)
    
    async def rename_file(
        self,
        src_volume: str,
        src_path: str,
        dst_volume: str,
        dst_path: str
    ) -> None:
        await self._impl.rename_file(src_volume, src_path, dst_volume, dst_path)
    
    async def rename_part(
        self,
        src_volume: str,
        src_path: str,
        dst_volume: str,
        dst_path: str,
        meta: bytes
    ) -> None:
        await self._impl.rename_part(src_volume, src_path, dst_volume, dst_path, meta)
    
    async def delete(self, volume: str, path: str, opt: DeleteOptions) -> None:
        await self._impl.delete(volume, path, opt)
    
    async def verify_file(self, volume: str, path: str, fi: Any) -> CheckPartsResp:
        return await self._impl.verify_file(volume, path, fi)
    
    async def check_parts(self, volume: str, path: str, fi: Any) -> CheckPartsResp:
        return await self._impl.check_parts(volume, path, fi)
    
    async def read_parts(self, bucket: str, paths: List[str]) -> List[Any]:
        return await self._impl.read_parts(bucket, paths)
    
    async def read_multiple(self, req: ReadMultipleReq) -> List[ReadMultipleResp]:
        return await self._impl.read_multiple(req)
    
    async def write_all(self, volume: str, path: str, data: bytes) -> None:
        await self._impl.write_all(volume, path, data)
    
    async def read_all(self, volume: str, path: str) -> bytes:
        return await self._impl.read_all(volume, path)
    
    async def disk_info(self, opts: DiskInfoOptions) -> DiskInfo:
        return await self._impl.disk_info(opts)


# 需要从其他模块导入的类型占位符
class LocalDisk:
    """本地磁盘实现占位符"""
    pass


class RemoteDisk:
    """远程磁盘实现占位符"""
    pass


async def new_disk(ep: Any, opt: DiskOption) -> Disk:
    """创建新的磁盘实例"""
    if ep.is_local:
        from .local import LocalDisk
        local_disk = await LocalDisk.new(ep, opt.cleanup)
        return Disk(local_disk)
    else:
        from ..rpc import RemoteDisk
        remote_disk = await RemoteDisk.new(ep, opt)
        return Disk(remote_disk)


def conv_part_err_to_int(err: Optional[Exception]) -> int:
    """将错误转换为整数状态码"""
    if err is None:
        return CheckPartStatus.SUCCESS
    
    # 需要根据实际的错误类型进行映射
    err_type = type(err).__name__
    if err_type in ['FileNotFound', 'FileVersionNotFound']:
        return CheckPartStatus.FILE_NOT_FOUND
    elif err_type == 'FileCorrupt':
        return CheckPartStatus.FILE_CORRUPT
    elif err_type == 'VolumeNotFound':
        return CheckPartStatus.VOLUME_NOT_FOUND
    elif err_type == 'DiskNotFound':
        return CheckPartStatus.DISK_NOT_FOUND
    else:
        return CheckPartStatus.UNKNOWN


def has_part_err(part_errs: List[int]) -> bool:
    """检查是否有部分错误"""
    return any(err != CheckPartStatus.SUCCESS for err in part_errs)


# 测试代码
if __name__ == "__main__":
    import unittest
    
    class TestDiskModule(unittest.TestCase):
        """磁盘模块测试类"""
        
        def test_disk_location_valid(self):
            """测试磁盘位置验证"""
            valid_location = DiskLocation(pool_idx=0, set_idx=1, disk_idx=2)
            self.assertTrue(valid_location.valid())
            
            invalid_location = DiskLocation()
            self.assertFalse(invalid_location.valid())
            
            partial_valid_location = DiskLocation(pool_idx=0, disk_idx=2)
            self.assertFalse(partial_valid_location.valid())
        
        def test_file_info_versions_find_version_index(self):
            """测试文件版本查找"""
            # 创建模拟的FileInfo对象
            class MockFileInfo:
                def __init__(self, version_id):
                    self.version_id = version_id
            
            v1_uuid = uuid.uuid4()
            v2_uuid = uuid.uuid4()
            
            fiv = FileInfoVersions(
                volume="test-bucket",
                name="test-object",
                versions=[
                    MockFileInfo(v1_uuid),
                    MockFileInfo(v2_uuid)
                ]
            )
            
            self.assertEqual(fiv.find_version_index(str(v1_uuid)), 0)
            self.assertEqual(fiv.find_version_index(str(v2_uuid)), 1)
            self.assertIsNone(fiv.find_version_index("non-existent"))
            self.assertIsNone(fiv.find_version_index(""))
        
        def test_conv_part_err_to_int(self):
            """测试错误转换"""
            self.assertEqual(conv_part_err_to_int(None), CheckPartStatus.SUCCESS)
            # 需要实际的错误类型来进行更多测试
        
        def test_has_part_err(self):
            """测试错误检查"""
            self.assertFalse(has_part_err([]))
            self.assertFalse(has_part_err([CheckPartStatus.SUCCESS]))
            self.assertFalse(has_part_err([CheckPartStatus.SUCCESS, CheckPartStatus.SUCCESS]))
            
            self.assertTrue(has_part_err([CheckPartStatus.FILE_NOT_FOUND]))
            self.assertTrue(has_part_err([CheckPartStatus.SUCCESS, CheckPartStatus.FILE_CORRUPT]))
            self.assertTrue(has_part_err([CheckPartStatus.DISK_NOT_FOUND, CheckPartStatus.VOLUME_NOT_FOUND]))
        
        def test_constants(self):
            """测试常量值"""
            self.assertEqual(RUSTFS_META_BUCKET, ".rustfs.sys")
            self.assertEqual(RUSTFS_META_MULTIPART_BUCKET, ".rustfs.sys/multipart")
            self.assertEqual(RUSTFS_META_TMP_BUCKET, ".rustfs.sys/tmp")
            self.assertEqual(RUSTFS_META_TMP_DELETED_BUCKET, ".rustfs.sys/tmp/.trash")
            self.assertEqual(BUCKET_META_PREFIX, "buckets")
            self.assertEqual(FORMAT_CONFIG_FILE, "format.json")
            self.assertEqual(STORAGE_FORMAT_FILE, "xl.meta")
            self.assertEqual(STORAGE_FORMAT_FILE_BACKUP, "xl.meta.bkp")
            
            self.assertEqual(CheckPartStatus.UNKNOWN, 0)
            self.assertEqual(CheckPartStatus.SUCCESS, 1)
            self.assertEqual(CheckPartStatus.DISK_NOT_FOUND, 2)
            self.assertEqual(CheckPartStatus.VOLUME_NOT_FOUND, 3)
            self.assertEqual(CheckPartStatus.FILE_NOT_FOUND, 4)
            self.assertEqual(CheckPartStatus.FILE_CORRUPT, 5)
    
    # 运行测试
    unittest.main()