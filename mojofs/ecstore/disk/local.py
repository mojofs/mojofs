import os
import asyncio
import uuid
import time
import shutil
import json
import struct
import hashlib
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Union, Any, BinaryIO
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import aiofiles
import aiofiles.os
from asyncio import Lock, Event
import logging

from mojofs.ecstore.disk.error import Error, DiskError
from mojofs.ecstore.disk.os import is_root_disk, rename_all
from mojofs.ecstore.disk import (
    BUCKET_META_PREFIX, CheckPartsResp, DeleteOptions, DiskAPI, DiskInfo, DiskInfoOptions, DiskLocation, DiskMetrics,
    FileInfoVersions, RUSTFS_META_BUCKET, ReadMultipleReq, ReadMultipleResp, ReadOptions, RenameDataResp,
    STORAGE_FORMAT_FILE_BACKUP, UpdateMetadataOpts, VolumeInfo, WalkDirOptions
)
from mojofs.ecstore.disk.endpoint import Endpoint
from mojofs.ecstore.disk.format import FormatV3
from mojofs.ecstore.disk.error_conv import to_access_error, to_file_error, to_unformatted_disk_error, to_volume_error
from mojofs.ecstore.disk.fs import O_APPEND, O_CREATE, O_RDONLY, O_TRUNC, O_WRONLY, access, lstat, lstat_std, remove, remove_all_std, remove_std, rename
from mojofs.ecstore.disk.os import check_path_length, is_empty_dir
from mojofs.ecstore import (
    CHECK_PART_FILE_CORRUPT, CHECK_PART_FILE_NOT_FOUND, CHECK_PART_SUCCESS, CHECK_PART_UNKNOWN, CHECK_PART_VOLUME_NOT_FOUND,
    FileReader, RUSTFS_META_TMP_DELETED_BUCKET, conv_part_err_to_int, FileWriter, STORAGE_FORMAT_FILE
)
from mojofs.ecstore.global_var import GLOBAL_IsErasureSD, GLOBAL_RootDiskThreshold
from mojofs.utils.path import (
    GLOBAL_DIR_SUFFIX, GLOBAL_DIR_SUFFIX_WITH_SLASH, SLASH_SEPARATOR, clean, decode_dir_object, encode_dir_object, has_suffix,
    path_join, path_join_buf
)
from mojofs.ecstore.erasure_coding import bitrot_verify
from mojofs.filemeta import (
    Cache, FileInfo, FileInfoOpts, FileMeta, MetaCacheEntry, MetacacheWriter, ObjectPartInfo, Opts, RawFileInfo, UpdateFn,
    get_file_info, read_xl_meta_no_data
)
from mojofs.utils import HashAlgorithm
from mojofs.utils.os import get_info

logger = logging.getLogger(__name__)

@dataclass
class FormatInfo:
    id: Optional[uuid.UUID] = None
    data: bytes = b''
    file_info: Optional[os.stat_result] = None
    last_check: Optional[datetime] = None
    
    def last_check_valid(self) -> bool:
        now = datetime.now(timezone.utc)
        return (self.file_info is not None and 
                self.id is not None and 
                self.last_check is not None and
                (now.timestamp() - self.last_check.timestamp() <= 1))

class InternalBuf:
    """Helper class to handle internal buffer types for writing data."""
    def __init__(self, data: Union[bytes, memoryview]):
        self.data = data
        self.is_owned = isinstance(data, bytes)

class LocalDisk:
    def __init__(self, root: Path, format_path: Path, format_info: FormatInfo, endpoint: Endpoint,
                 disk_info_cache: Cache, rotational: bool = False, fstype: str = "", 
                 major: int = 0, minor: int = 0, nrrequests: int = 0):
        self.root = root
        self.format_path = format_path
        self.format_info = asyncio.Lock()
        self._format_info = format_info
        self.endpoint = endpoint
        self.disk_info_cache = disk_info_cache
        self.scanning = 0
        self.rotational = rotational
        self.fstype = fstype
        self.major = major
        self.minor = minor
        self.nrrequests = nrrequests
        self.exit_signal = None
        self._cleanup_task = None

    def __del__(self):
        if self.exit_signal:
            self.exit_signal.set()
        if self._cleanup_task:
            self._cleanup_task.cancel()

    def __repr__(self):
        return f"LocalDisk(root={self.root}, endpoint={self.endpoint})"

    @classmethod
    async def new(cls, ep: Endpoint, cleanup: bool = False) -> 'LocalDisk':
        logger.debug("Creating local disk")
        try:
            root = Path(ep.get_file_path()).resolve()
        except FileNotFoundError:
            raise DiskError.VolumeNotFound
        except Exception as e:
            raise to_file_error(e)

        if cleanup:
            # TODO: Delete tmp data
            pass

        format_path = root / RUSTFS_META_BUCKET / "format.json"
        logger.debug(f"format_path: {format_path}")
        
        format_data, format_meta = await read_file_exists(format_path)
        
        disk_id = None
        format_last_check = None
        
        if format_data:
            try:
                fm = FormatV3.from_bytes(format_data)
                set_idx, disk_idx = fm.find_disk_index_by_disk_id(fm.erasure.this)
                
                if set_idx != ep.set_idx or disk_idx != ep.disk_idx:
                    raise DiskError.InconsistentDisk
                
                disk_id = fm.erasure.this
                format_last_check = datetime.now(timezone.utc)
            except Exception as e:
                raise Error.other(e)
        
        format_info = FormatInfo(
            id=disk_id,
            data=format_data,
            file_info=format_meta,
            last_check=format_last_check
        )
        
        async def update_disk_info():
            disk_id_str = str(disk_id) if disk_id else ""
            try:
                info, is_root = await get_disk_info(root)
                disk_info = DiskInfo(
                    total=info.total,
                    free=info.free,
                    used=info.used,
                    used_inodes=info.files - info.ffree,
                    free_inodes=info.ffree,
                    major=info.major,
                    minor=info.minor,
                    fs_type=info.fstype,
                    root_disk=is_root,
                    id=disk_id_str
                )
                return disk_info
            except Exception as e:
                raise e
        
        cache = Cache(update_disk_info, 1.0, Opts())
        
        disk = cls(
            root=root,
            format_path=format_path,
            format_info=format_info,
            endpoint=ep,
            disk_info_cache=cache
        )
        
        info, _ = await get_disk_info(root)
        disk.major = info.major
        disk.minor = info.minor
        disk.fstype = info.fstype
        
        if info.nrrequests > 0:
            disk.nrrequests = info.nrrequests
        
        if info.rotational:
            disk.rotational = True
        
        await disk.make_meta_volumes()
        
        disk.exit_signal = Event()
        disk._cleanup_task = asyncio.create_task(disk._cleanup_deleted_objects_loop())
        
        logger.debug(f"LocalDisk created: {disk}")
        return disk

    async def _cleanup_deleted_objects_loop(self):
        while not self.exit_signal.is_set():
            try:
                await asyncio.wait_for(self.exit_signal.wait(), timeout=300)  # 5 minutes
            except asyncio.TimeoutError:
                try:
                    await self._cleanup_deleted_objects()
                except Exception as e:
                    logger.error(f"cleanup_deleted_objects error: {e}")

    async def _cleanup_deleted_objects(self):
        trash = self.root / RUSTFS_META_TMP_DELETED_BUCKET
        try:
            for entry in trash.iterdir():
                if entry.name in (".", ".."):
                    continue
                
                if entry.is_dir():
                    shutil.rmtree(entry, ignore_errors=True)
                else:
                    try:
                        entry.unlink()
                    except FileNotFoundError:
                        pass
        except FileNotFoundError:
            pass

    @staticmethod
    def is_valid_volname(volname: str) -> bool:
        if len(volname) < 3:
            return False
        
        if os.name == 'nt':  # Windows
            invalid_chars = '|<>?*:"\\'
            if any(char in volname for char in invalid_chars):
                return False
        
        return True

    async def check_format_json(self) -> os.stat_result:
        try:
            return os.stat(self.format_path)
        except Exception as e:
            raise to_unformatted_disk_error(e)

    async def make_meta_volumes(self):
        buckets = f"{RUSTFS_META_BUCKET}/{BUCKET_META_PREFIX}"
        multipart = f"{RUSTFS_META_BUCKET}/multipart"
        config = f"{RUSTFS_META_BUCKET}/config"
        tmp = f"{RUSTFS_META_BUCKET}/tmp"
        
        defaults = [
            buckets,
            multipart,
            config,
            tmp,
            RUSTFS_META_TMP_DELETED_BUCKET
        ]
        
        await self.make_volumes(defaults)

    def resolve_abs_path(self, path: Union[str, Path]) -> Path:
        return (self.root / path).resolve()

    def get_object_path(self, bucket: str, key: str) -> Path:
        return self.resolve_abs_path(Path(bucket) / key)

    def get_bucket_path(self, bucket: str) -> Path:
        return self.resolve_abs_path(bucket)

    async def move_to_trash(self, delete_path: Path, recursive: bool, immediate_purge: bool):
        trash_path = self.get_object_path(RUSTFS_META_TMP_DELETED_BUCKET, str(uuid.uuid4()))
        
        try:
            if recursive:
                await rename_all(delete_path, trash_path, self.get_bucket_path(RUSTFS_META_TMP_DELETED_BUCKET))
            else:
                await rename(delete_path, trash_path)
        except Error as e:
            if e == Error.DiskFull:
                if recursive:
                    remove_all_std(delete_path)
                else:
                    remove_std(delete_path)
            return
        
        if immediate_purge or str(delete_path).endswith(SLASH_SEPARATOR):
            trash_path2 = self.get_object_path(RUSTFS_META_TMP_DELETED_BUCKET, str(uuid.uuid4()))
            try:
                await rename_all(
                    encode_dir_object(str(delete_path)),
                    trash_path2,
                    self.get_bucket_path(RUSTFS_META_TMP_DELETED_BUCKET)
                )
            except:
                pass

    async def delete_file(self, base_path: Path, delete_path: Path, recursive: bool, immediate_purge: bool):
        if is_root_path(base_path) or is_root_path(delete_path):
            return
        
        if not str(delete_path).startswith(str(base_path)) or base_path == delete_path:
            return
        
        if recursive:
            await self.move_to_trash(delete_path, recursive, immediate_purge)
        elif delete_path.is_dir():
            try:
                delete_path.rmdir()
            except (FileNotFoundError, OSError):
                pass
        else:
            try:
                delete_path.unlink()
            except FileNotFoundError:
                pass
        
        if delete_path.parent != base_path:
            await self.delete_file(base_path, delete_path.parent, False, False)

    async def read_raw(self, bucket: str, volume_dir: Path, file_path: Path, read_data: bool) -> Tuple[bytes, Optional[datetime]]:
        if not file_path.name:
            raise DiskError.FileNotFound
        
        meta_path = file_path / STORAGE_FORMAT_FILE
        
        try:
            if read_data:
                return await self.read_all_data_with_dmtime(bucket, volume_dir, meta_path)
            else:
                result = await self.read_metadata_with_dmtime(meta_path)
                if result[0] and not skip_access_checks(bucket):
                    try:
                        await access(volume_dir)
                    except FileNotFoundError:
                        raise DiskError.VolumeNotFound
                return result
        except Error as e:
            if e == Error.FileNotFound and not skip_access_checks(bucket):
                try:
                    await access(volume_dir)
                except FileNotFoundError:
                    raise DiskError.VolumeNotFound
            raise

    async def read_metadata(self, file_path: Path) -> bytes:
        data, _ = await self.read_metadata_with_dmtime(file_path)
        return data

    async def read_metadata_with_dmtime(self, file_path: Path) -> Tuple[bytes, Optional[datetime]]:
        check_path_length(str(file_path))
        
        try:
            async with aiofiles.open(file_path, 'rb') as f:
                stat = await aiofiles.os.stat(file_path)
                if os.path.isdir(file_path):
                    raise Error.FileNotFound
                
                data = await read_xl_meta_no_data(f, stat.st_size)
                modtime = datetime.fromtimestamp(stat.st_mtime, timezone.utc)
                return data, modtime
        except FileNotFoundError:
            raise to_file_error(FileNotFoundError())

    async def read_all_data(self, volume: str, volume_dir: Path, file_path: Path) -> bytes:
        data, _ = await self.read_all_data_with_dmtime(volume, volume_dir, file_path)
        return data

    async def read_all_data_with_dmtime(self, volume: str, volume_dir: Path, file_path: Path) -> Tuple[bytes, Optional[datetime]]:
        try:
            async with aiofiles.open(file_path, 'rb') as f:
                stat = await aiofiles.os.stat(file_path)
                if os.path.isdir(file_path):
                    raise DiskError.FileNotFound
                
                data = await f.read()
                modtime = datetime.fromtimestamp(stat.st_mtime, timezone.utc)
                return data, modtime
        except FileNotFoundError as e:
            if not skip_access_checks(volume):
                try:
                    await access(volume_dir)
                except FileNotFoundError:
                    raise DiskError.VolumeNotFound
            raise to_file_error(e)

    async def delete_versions_internal(self, volume: str, path: str, fis: List[FileInfo]):
        volume_dir = self.get_bucket_path(volume)
        xlpath = self.get_object_path(volume, f"{path}/{STORAGE_FORMAT_FILE}")
        
        data, _ = await self.read_all_data_with_dmtime(volume, volume_dir, xlpath)
        
        if not data:
            raise DiskError.FileNotFound
        
        fm = FileMeta()
        fm.unmarshal_msg(data)
        
        for fi in fis:
            try:
                data_dir = fm.delete_version(fi)
            except Error as err:
                if not fi.deleted and err in (DiskError.FileNotFound, DiskError.FileVersionNotFound):
                    continue
                raise
            
            if data_dir:
                vid = fi.version_id or ""
                fm.data.remove([vid, data_dir])
                
                dir_path = self.get_object_path(volume, f"{path}/{data_dir}")
                try:
                    await self.move_to_trash(dir_path, True, False)
                except Error as err:
                    if err not in (DiskError.FileNotFound, DiskError.VolumeNotFound):
                        raise
        
        if not fm.versions:
            await self.delete_file(volume_dir, xlpath, True, False)
            return
        
        buf = fm.marshal_msg()
        await self.write_all_private(volume, f"{path}/{STORAGE_FORMAT_FILE}", buf, True, volume_dir)

    async def write_all_meta(self, volume: str, path: str, buf: bytes, sync: bool):
        volume_dir = self.get_bucket_path(volume)
        file_path = volume_dir / path
        check_path_length(str(file_path))
        
        tmp_volume_dir = self.get_bucket_path(RUSTFS_META_TMP_BUCKET)
        tmp_file_path = tmp_volume_dir / str(uuid.uuid4())
        
        await self.write_all_internal(tmp_file_path, InternalBuf(buf), sync, tmp_volume_dir)
        await rename_all(tmp_file_path, file_path, volume_dir)

    async def write_all_public(self, volume: str, path: str, data: bytes):
        if volume == RUSTFS_META_BUCKET and path == "format.json":
            async with self.format_info:
                self._format_info.data = data
        
        volume_dir = self.get_bucket_path(volume)
        await self.write_all_private(volume, path, data, True, volume_dir)

    async def write_all_private(self, volume: str, path: str, buf: bytes, sync: bool, skip_parent: Path):
        volume_dir = self.get_bucket_path(volume)
        file_path = volume_dir / path
        check_path_length(str(file_path))
        
        await self.write_all_internal(file_path, InternalBuf(buf), sync, skip_parent)

    async def write_all_internal(self, file_path: Path, data: InternalBuf, sync: bool, skip_parent: Path):
        if file_path.parent != skip_parent:
            file_path.parent.mkdir(parents=True, exist_ok=True)
        
        mode = 'wb'
        async with aiofiles.open(file_path, mode) as f:
            await f.write(data.data)
            if sync:
                await f.flush()
                os.fsync(f.fileno())

    async def open_file(self, path: Path, mode: int, skip_parent: Path) -> BinaryIO:
        if not skip_parent:
            skip_parent = self.root
        
        if path.parent != skip_parent:
            path.parent.mkdir(parents=True, exist_ok=True)
        
        mode_str = 'rb' if mode & O_RDONLY else 'wb'
        if mode & O_APPEND:
            mode_str = 'ab'
        
        return await aiofiles.open(path, mode_str)

    def get_metrics(self) -> DiskMetrics:
        return DiskMetrics()

    async def bitrot_verify(self, part_path: Path, part_size: int, algo: HashAlgorithm, 
                          sum: bytes, shard_size: int):
        async with aiofiles.open(part_path, 'rb') as f:
            stat = await aiofiles.os.stat(part_path)
            file_size = stat.st_size
            
            await bitrot_verify(f, file_size, part_size, algo, sum, shard_size)

    async def scan_dir(self, current: str, opts: WalkDirOptions, out: MetacacheWriter, objs_returned: int) -> int:
        forward = None
        if opts.forward_to and opts.forward_to.startswith(current):
            forward = opts.forward_to[len(current):]
            if '/' in forward:
                forward = forward[:forward.index('/')]
        
        if opts.limit > 0 and objs_returned >= opts.limit:
            return objs_returned
        
        try:
            entries = await self.list_dir("", opts.bucket, current, -1)
        except Error as e:
            if e not in (DiskError.VolumeNotFound, Error.FileNotFound):
                logger.debug(f"scan list_dir {current}, err {e}")
            
            if opts.report_notfound and e == Error.FileNotFound and current == opts.base_dir:
                raise DiskError.FileNotFound
            
            return objs_returned
        
        if not entries:
            return objs_returned
        
        current = current.strip(SLASH_SEPARATOR)
        bucket = opts.bucket
        dir_objes = set()
        
        # First pass filtering
        filtered_entries = []
        for entry in entries:
            if opts.limit > 0 and objs_returned >= opts.limit:
                return objs_returned
            
            if opts.filter_prefix and not entry.startswith(opts.filter_prefix):
                continue
            
            if forward and entry < forward:
                continue
            
            if entry.endswith(SLASH_SEPARATOR):
                if entry.endswith(GLOBAL_DIR_SUFFIX_WITH_SLASH):
                    entry = entry[:-len(GLOBAL_DIR_SUFFIX_WITH_SLASH)] + SLASH_SEPARATOR
                    dir_objes.add(entry)
                else:
                    entry = entry.rstrip(SLASH_SEPARATOR)
            elif entry.endswith(STORAGE_FORMAT_FILE):
                metadata = await self.read_metadata(
                    self.get_object_path(bucket, f"{current}/{entry}")
                )
                
                entry = entry[:-len(STORAGE_FORMAT_FILE)]
                name = entry.rstrip(SLASH_SEPARATOR)
                name = decode_dir_object(f"{current}/{name}")
                
                await out.write_obj(MetaCacheEntry(
                    name=name,
                    metadata=metadata
                ))
                objs_returned += 1
                return objs_returned
            
            filtered_entries.append(entry)
        
        filtered_entries.sort()
        
        if forward:
            for i, entry in enumerate(filtered_entries):
                if entry >= forward or forward.startswith(entry):
                    filtered_entries = filtered_entries[i:]
                    break
        
        dir_stack = []
        
        for entry in filtered_entries:
            if opts.limit > 0 and objs_returned >= opts.limit:
                return objs_returned
            
            if not entry:
                continue
            
            name = path_join_buf([current, entry])
            
            while dir_stack and dir_stack[-1] < name:
                pop = dir_stack.pop()
                await out.write_obj(MetaCacheEntry(name=pop))
                objs_returned += 1
                
                if opts.recursive:
                    new_opts = opts.copy()
                    new_opts.filter_prefix = None
                    objs_returned = await self.scan_dir(pop, new_opts, out, objs_returned)
            
            meta = MetaCacheEntry(name=name)
            is_dir_obj = entry in dir_objes
            
            if is_dir_obj:
                meta.name = meta.name[:-1] + GLOBAL_DIR_SUFFIX_WITH_SLASH
            
            fname = f"{meta.name}/{STORAGE_FORMAT_FILE}"
            
            try:
                metadata = await self.read_metadata(self.get_object_path(opts.bucket, fname))
                if is_dir_obj:
                    meta.name = meta.name[:-len(GLOBAL_DIR_SUFFIX_WITH_SLASH)] + SLASH_SEPARATOR
                
                meta.metadata = metadata
                await out.write_obj(meta)
                objs_returned += 1
            except Error as err:
                if err in (Error.FileNotFound, Error.IsNotRegular):
                    if not is_dir_obj and not await is_empty_dir(self.get_object_path(opts.bucket, meta.name)):
                        meta.name += SLASH_SEPARATOR
                        dir_stack.append(meta.name)
        
        while dir_stack:
            if opts.limit > 0 and objs_returned >= opts.limit:
                break
            
            dir_name = dir_stack.pop()
            await out.write_obj(MetaCacheEntry(name=dir_name))
            objs_returned += 1
            
            if opts.recursive:
                new_opts = opts.copy()
                new_opts.filter_prefix = None
                objs_returned = await self.scan_dir(dir_name, new_opts, out, objs_returned)
        
        return objs_returned

    # DiskAPI implementation
    def to_string(self) -> str:
        return str(self.root)
    
    def is_local(self) -> bool:
        return True
    
    def host_name(self) -> str:
        return self.endpoint.host_port()
    
    async def is_online(self) -> bool:
        try:
            await self.check_format_json()
            return True
        except:
            return False
    
    def endpoint(self) -> Endpoint:
        return self.endpoint
    
    async def close(self):
        if self.exit_signal:
            self.exit_signal.set()
        if self._cleanup_task:
            self._cleanup_task.cancel()
    
    def path(self) -> Path:
        return self.root
    
    def get_disk_location(self) -> DiskLocation:
        return DiskLocation(
            pool_idx=self.endpoint.pool_idx if self.endpoint.pool_idx >= 0 else None,
            set_idx=self.endpoint.set_idx if self.endpoint.set_idx >= 0 else None,
            disk_idx=self.endpoint.disk_idx if self.endpoint.disk_idx >= 0 else None
        )
    
    async def get_disk_id(self) -> Optional[uuid.UUID]:
        async with self.format_info:
            if self._format_info.last_check_valid():
                return self._format_info.id
            
            file_meta = await self.check_format_json()
            
            if self._format_info.file_info and same_file(file_meta, self._format_info.file_info):
                self._format_info.last_check = datetime.now(timezone.utc)
                return self._format_info.id
            
            with open(self.format_path, 'rb') as f:
                data = f.read()
            
            fm = FormatV3.from_bytes(data)
            m, n = fm.find_disk_index_by_disk_id(fm.erasure.this)
            
            if m != self.endpoint.set_idx or n != self.endpoint.disk_idx:
                raise DiskError.InconsistentDisk
            
            self._format_info.id = fm.erasure.this
            self._format_info.file_info = file_meta
            self._format_info.data = data
            self._format_info.last_check = datetime.now(timezone.utc)
            
            return fm.erasure.this
    
    async def set_disk_id(self, disk_id: Optional[uuid.UUID]):
        async with self.format_info:
            self._format_info.id = disk_id
    
    async def read_all(self, volume: str, path: str) -> bytes:
        if volume == RUSTFS_META_BUCKET and path == "format.json":
            async with self.format_info:
                if self._format_info.data:
                    return self._format_info.data
        
        p = self.get_object_path(volume, path)
        data, _ = await read_file_all(p)
        return data
    
    async def write_all(self, volume: str, path: str, data: bytes):
        await self.write_all_public(volume, path, data)
    
    async def delete(self, volume: str, path: str, opt: DeleteOptions):
        volume_dir = self.get_bucket_path(volume)
        if not skip_access_checks(volume):
            try:
                await access(volume_dir)
            except Exception as e:
                raise to_access_error(e, DiskError.VolumeAccessDenied)
        
        file_path = volume_dir / path
        check_path_length(str(file_path))
        
        await self.delete_file(volume_dir, file_path, opt.recursive, opt.immediate)
    
    async def verify_file(self, volume: str, path: str, fi: FileInfo) -> CheckPartsResp:
        volume_dir = self.get_bucket_path(volume)
        if not skip_access_checks(volume):
            try:
                await access(volume_dir)
            except Exception as e:
                raise to_access_error(e, DiskError.VolumeAccessDenied)
        
        resp = CheckPartsResp(results=[0] * len(fi.parts))
        
        erasure = fi.erasure
        for i, part in enumerate(fi.parts):
            checksum_info = erasure.get_checksum_info(part.number)
            part_path = volume_dir / path / (fi.data_dir or "") / f"part.{part.number}"
            
            try:
                await self.bitrot_verify(
                    part_path,
                    erasure.shard_file_size(part.size),
                    checksum_info.algorithm,
                    checksum_info.hash,
                    erasure.shard_size()
                )
                resp.results[i] = CHECK_PART_SUCCESS
            except Exception as err:
                resp.results[i] = conv_part_err_to_int(err)
                if resp.results[i] == CHECK_PART_UNKNOWN:
                    if err != DiskError.FileAccessDenied:
                        logger.info(f"part unknown, disk: {self.to_string()}, path: {part_path}")
        
        return resp
    
    async def read_parts(self, bucket: str, paths: List[str]) -> List[ObjectPartInfo]:
        volume_dir = self.get_bucket_path(bucket)
        ret = [ObjectPartInfo() for _ in paths]
        
        for i, path_str in enumerate(paths):
            path = Path(path_str)
            file_name = path.name
            
            try:
                num = int(file_name.replace("part.", "").replace(".meta", ""))
            except:
                num = 0
            
            part_path = volume_dir / path.parent / f"part.{num}"
            
            try:
                await access(part_path)
            except Exception as err:
                ret[i] = ObjectPartInfo(number=num, error=str(err))
                continue
            
            try:
                data = await self.read_all_data(bucket, volume_dir, volume_dir / path)
                ret[i] = ObjectPartInfo.unmarshal(data)
            except Exception as err:
                ret[i] = ObjectPartInfo(number=num, error=str(err))
        
        return ret
    
    async def check_parts(self, volume: str, path: str, fi: FileInfo) -> CheckPartsResp:
        volume_dir = self.get_bucket_path(volume)
        check_path_length(str(volume_dir / path))
        
        resp = CheckPartsResp(results=[0] * len(fi.parts))
        
        for i, part in enumerate(fi.parts):
            file_path = volume_dir / path / (fi.data_dir or "") / f"part.{part.number}"
            
            try:
                st = await lstat(file_path)
                if st.is_dir():
                    resp.results[i] = CHECK_PART_FILE_NOT_FOUND
                elif st.st_size < fi.erasure.shard_file_size(part.size):
                    resp.results[i] = CHECK_PART_FILE_CORRUPT
                else:
                    resp.results[i] = CHECK_PART_SUCCESS
            except FileNotFoundError:
                if not skip_access_checks(volume):
                    try:
                        await access(volume_dir)
                    except FileNotFoundError:
                        resp.results[i] = CHECK_PART_VOLUME_NOT_FOUND
                        continue
                resp.results[i] = CHECK_PART_FILE_NOT_FOUND
        
        return resp
    
    async def rename_part(self, src_volume: str, src_path: str, dst_volume: str, dst_path: str, meta: bytes):
        src_volume_dir = self.get_bucket_path(src_volume)
        dst_volume_dir = self.get_bucket_path(dst_volume)
        
        if not skip_access_checks(src_volume):
            access_std(src_volume_dir)
        if not skip_access_checks(dst_volume):
            access_std(dst_volume_dir)
        
        src_is_dir = has_suffix(src_path, SLASH_SEPARATOR)
        dst_is_dir = has_suffix(dst_path, SLASH_SEPARATOR)
        
        if (not src_is_dir and dst_is_dir) or (src_is_dir and not dst_is_dir):
            logger.warning(f"rename_part src and dst must be both dir or file src_is_dir:{src_is_dir}, dst_is_dir:{dst_is_dir}")
            raise DiskError.FileAccessDenied
        
        src_file_path = src_volume_dir / src_path
        dst_file_path = dst_volume_dir / dst_path
        
        check_path_length(str(src_file_path))
        check_path_length(str(dst_file_path))
        
        if src_is_dir:
            try:
                meta_stat = lstat_std(src_file_path)
                if not meta_stat.is_dir():
                    logger.warning(f"rename_part src is not dir {src_file_path}")
                    raise DiskError.FileAccessDenied
            except FileNotFoundError:
                pass
            
            remove_std(dst_file_path)
        
        await rename_all(src_file_path, dst_file_path, dst_volume_dir)
        await self.write_all(dst_volume, f"{dst_path}.meta", meta)
        
        if src_file_path.parent:
            await self.delete_file(src_volume_dir, src_file_path.parent, False, False)
    
    async def rename_file(self, src_volume: str, src_path: str, dst_volume: str, dst_path: str):
        src_volume_dir = self.get_bucket_path(src_volume)
        dst_volume_dir = self.get_bucket_path(dst_volume)
        
        if not skip_access_checks(src_volume):
            await access(src_volume_dir)
        if not skip_access_checks(dst_volume):
            await access(dst_volume_dir)
        
        src_is_dir = has_suffix(src_path, SLASH_SEPARATOR)
        dst_is_dir = has_suffix(dst_path, SLASH_SEPARATOR)
        
        if (dst_is_dir or src_is_dir) and (not dst_is_dir or not src_is_dir):
            raise Error(DiskError.FileAccessDenied)
        
        src_file_path = src_volume_dir / src_path
        dst_file_path = dst_volume_dir / dst_path
        
        check_path_length(str(src_file_path))
        check_path_length(str(dst_file_path))
        
        if src_is_dir:
            try:
                meta = await lstat(src_file_path)
                if not meta.is_dir():
                    raise DiskError.FileAccessDenied
            except FileNotFoundError:
                pass
            
            await remove(dst_file_path)
        
        await rename_all(src_file_path, dst_file_path, dst_volume_dir)
        
        if src_file_path.parent:
            await self.delete_file(src_volume_dir, src_file_path.parent, False, False)
    
    async def create_file(self, origvolume: str, volume: str, path: str, file_size: int) -> FileWriter:
        if origvolume:
            origvolume_dir = self.get_bucket_path(origvolume)
            if not skip_access_checks(origvolume):
                await access(origvolume_dir)
        
        volume_dir = self.get_bucket_path(volume)
        file_path = volume_dir / path
        check_path_length(str(file_path))
        
        if file_path.parent:
            file_path.parent.mkdir(parents=True, exist_ok=True)
        
        f = await self.open_file(file_path, O_CREATE | O_WRONLY, volume_dir)
        return f
    
    async def append_file(self, volume: str, path: str) -> FileWriter:
        volume_dir = self.get_bucket_path(volume)
        if not skip_access_checks(volume):
            await access(volume_dir)
        
        file_path = volume_dir / path
        check_path_length(str(file_path))
        
        f = await self.open_file(file_path, O_CREATE | O_APPEND | O_WRONLY, volume_dir)
        return f
    
    async def read_file(self, volume: str, path: str) -> FileReader:
        volume_dir = self.get_bucket_path(volume)
        if not skip_access_checks(volume):
            await access(volume_dir)
        
        file_path = volume_dir / path
        check_path_length(str(file_path))
        
        f = await self.open_file(file_path, O_RDONLY, volume_dir)
        return f
    
    async def read_file_stream(self, volume: str, path: str, offset: int, length: int) -> FileReader:
        volume_dir = self.get_bucket_path(volume)
        if not skip_access_checks(volume):
            await access(volume_dir)
        
        file_path = volume_dir / path
        check_path_length(str(file_path))
        
        f = await self.open_file(file_path, O_RDONLY, volume_dir)
        
        stat = await aiofiles.os.stat(file_path)
        if stat.st_size < offset + length:
            logger.error(f"read_file_stream: file size is less than offset + length {offset} + {length} = {stat.st_size}")
            raise DiskError.FileCorrupt
        
        if offset > 0:
            await f.seek(offset)
        
        return f
    
    async def list_dir(self, origvolume: str, volume: str, dir_path: str, count: int) -> List[str]:
        if origvolume:
            origvolume_dir = self.get_bucket_path(origvolume)
            if not skip_access_checks(origvolume):
                try:
                    await access(origvolume_dir)
                except Exception as e:
                    raise to_access_error(e, DiskError.VolumeAccessDenied)
        
        volume_dir = self.get_bucket_path(volume)
        dir_path_abs = volume_dir / dir_path.lstrip(SLASH_SEPARATOR)
        
        try:
            entries = await read_dir(dir_path_abs, count)
        except FileNotFoundError as e:
            if not skip_access_checks(volume):
                try:
                    await access(volume_dir)
                except Exception as e:
                    raise to_access_error(e, DiskError.VolumeAccessDenied)
            raise to_file_error(e)
        
        return entries
    
    async def walk_dir(self, opts: WalkDirOptions, wr: BinaryIO):
        volume_dir = self.get_bucket_path(opts.bucket)
        
        if not skip_access_checks(opts.bucket):
            try:
                await access(volume_dir)
            except Exception as e:
                raise to_access_error(e, DiskError.VolumeAccessDenied)
        
        out = MetacacheWriter(wr)
        objs_returned = 0
        
        if opts.base_dir.endswith(SLASH_SEPARATOR):
            fpath = self.get_object_path(
                opts.bucket,
                path_join_buf([
                    f"{opts.base_dir.rstrip(SLASH_SEPARATOR)}{GLOBAL_DIR_SUFFIX}",
                    STORAGE_FORMAT_FILE
                ])
            )
            
            try:
                data = await self.read_metadata(fpath)
                meta = MetaCacheEntry(
                    name=opts.base_dir,
                    metadata=data
                )
                await out.write_obj(meta)
                objs_returned += 1
            except:
                fpath = self.get_object_path(
                    opts.bucket,
                    path_join_buf([opts.base_dir, STORAGE_FORMAT_FILE])
                )
                
                if fpath.exists() and fpath.is_file():
                    raise DiskError.FileNotFound
        
        current = opts.base_dir
        objs_returned = await self.scan_dir(current, opts, out, objs_returned)
    
    async def rename_data(self, src_volume: str, src_path: str, fi: FileInfo, 
                         dst_volume: str, dst_path: str) -> RenameDataResp:
        src_volume_dir = self.get_bucket_path(src_volume)
        if not skip_access_checks(src_volume):
            access_std(src_volume_dir)
        
        dst_volume_dir = self.get_bucket_path(dst_volume)
        if not skip_access_checks(dst_volume):
            access_std(dst_volume_dir)
        
        src_file_path = src_volume_dir / f"{src_path}/{STORAGE_FORMAT_FILE}"
        dst_file_path = dst_volume_dir / f"{dst_path}/{STORAGE_FORMAT_FILE}"
        
        has_data_dir_path = None
        if not fi.is_remote() and fi.data_dir:
            data_dir = str(fi.data_dir)
            src_data_path = src_volume_dir / f"{src_path}/{data_dir}"
            dst_data_path = dst_volume_dir / f"{dst_path}/{data_dir}"
            has_data_dir_path = (src_data_path, dst_data_path)
        
        check_path_length(str(src_file_path))
        check_path_length(str(dst_file_path))
        
        has_dst_buf = None
        try:
            has_dst_buf = await read_file(dst_file_path)
        except FileNotFoundError:
            pass
        
        xlmeta = FileMeta()
        if has_dst_buf and FileMeta.is_xl2_v1_format(has_dst_buf):
            xlmeta = FileMeta.load(has_dst_buf)
        
        skip_parent = dst_volume_dir
        if has_dst_buf:
            skip_parent = dst_file_path.parent
        
        has_old_data_dir = None
        try:
            _, ver = xlmeta.find_version(fi.version_id)
            data_dir = ver.get_data_dir()
            if data_dir and xlmeta.shard_data_dir_count(fi.version_id, data_dir) == 0:
                has_old_data_dir = data_dir
        except:
            pass
        
        xlmeta.add_version(fi)
        
        new_dst_buf = xlmeta.marshal_msg()
        await self.write_all(src_volume, f"{src_path}/{STORAGE_FORMAT_FILE}", new_dst_buf)
        
        if has_data_dir_path:
            src_data_path, dst_data_path = has_data_dir_path
            no_inline = fi.data is None and fi.size > 0
            if no_inline:
                try:
                    await rename_all(src_data_path, dst_data_path, skip_parent)
                except Exception as err:
                    await self.delete_file(dst_volume_dir, dst_data_path, False, False)
                    logger.info(f"rename all failed src_data_path: {src_data_path}, dst_data_path: {dst_data_path}, err: {err}")
                    raise
        
        if has_old_data_dir and has_dst_buf:
            try:
                await self.write_all_private(
                    dst_volume,
                    f"{dst_path}/{has_old_data_dir}/{STORAGE_FORMAT_FILE}",
                    has_dst_buf,
                    True,
                    skip_parent
                )
            except Exception as err:
                logger.info(f"write_all_private failed err: {err}")
                raise
        
        try:
            await rename_all(src_file_path, dst_file_path, skip_parent)
        except Exception as err:
            if has_data_dir_path:
                _, dst_data_path = has_data_dir_path
                await self.delete_file(dst_volume_dir, dst_data_path, False, False)
            logger.info(f"rename all failed err: {err}")
            raise
        
        if src_file_path.parent:
            if src_volume != "multipart":
                try:
                    remove_std(src_file_path.parent)
                except:
                    pass
            else:
                await self.delete_file(dst_volume_dir, src_file_path.parent, True, False)
        
        return RenameDataResp(old_data_dir=has_old_data_dir, sign=None)
    
    async def make_volumes(self, volumes: List[str]):
        for vol in volumes:
            try:
                await self.make_volume(vol)
            except Error as e:
                if e != DiskError.VolumeExists:
                    raise
    
    async def make_volume(self, volume: str):
        if not self.is_valid_volname(volume):
            raise Error.other("Invalid arguments specified")
        
        volume_dir = self.get_bucket_path(volume)
        
        try:
            await access(volume_dir)
            raise DiskError.VolumeExists
        except FileNotFoundError:
            volume_dir.mkdir(parents=True, exist_ok=True)
    
    async def list_volumes(self) -> List[VolumeInfo]:
        volumes = []
        
        entries = await read_dir(self.root, -1)
        
        for entry in entries:
            if not has_suffix(entry, SLASH_SEPARATOR) or not self.is_valid_volname(clean(entry)):
                continue
            
            volumes.append(VolumeInfo(
                name=clean(entry),
                created=None
            ))
        
        return volumes
    
    async def stat_volume(self, volume: str) -> VolumeInfo:
        volume_dir = self.get_bucket_path(volume)
        meta = await lstat(volume_dir)
        
        modtime = datetime.fromtimestamp(meta.st_mtime, timezone.utc)
        
        return VolumeInfo(
            name=volume,
            created=modtime
        )
    
    async def delete_paths(self, volume: str, paths: List[str]):
        volume_dir = self.get_bucket_path(volume)
        if not skip_access_checks(volume):
            await access(volume_dir)
        
        for path in paths:
            file_path = volume_dir / path
            check_path_length(str(file_path))
            await self.move_to_trash(file_path, False, False)
    
    async def update_metadata(self, volume: str, path: str, fi: FileInfo, opts: UpdateMetadataOpts):
        if not fi.metadata:
            raise Error.other("Invalid Argument")
        
        volume_dir = self.get_bucket_path(volume)
        file_path = volume_dir / path
        check_path_length(str(file_path))
        
        try:
            buf = await self.read_all(volume, f"{path}/{STORAGE_FORMAT_FILE}")
        except Error as e:
            if e == DiskError.FileNotFound and fi.version_id:
                raise DiskError.FileVersionNotFound
            raise
        
        if not FileMeta.is_xl2_v1_format(buf):
            raise DiskError.FileVersionNotFound
        
        xl_meta = FileMeta.load(buf)
        xl_meta.update_object_version(fi)
        
        wbuf = xl_meta.marshal_msg()
        await self.write_all_meta(volume, f"{path}/{STORAGE_FORMAT_FILE}", wbuf, not opts.no_persistence)
    
    async def write_metadata(self, org_volume: str, volume: str, path: str, fi: FileInfo):
        p = self.get_object_path(volume, f"{path}/{STORAGE_FORMAT_FILE}")
        
        meta = FileMeta()
        if not fi.fresh:
            buf, _ = await read_file_exists(p)
            if buf:
                try:
                    meta.unmarshal_msg(buf)
                except:
                    meta = FileMeta()
        
        meta.add_version(fi)
        fm_data = meta.marshal_msg()
        
        await self.write_all(volume, f"{path}/{STORAGE_FORMAT_FILE}", fm_data)
    
    async def read_version(self, org_volume: str, volume: str, path: str, 
                          version_id: str, opts: ReadOptions) -> FileInfo:
        file_path = self.get_object_path(volume, path)
        file_dir = self.get_bucket_path(volume)
        
        data, _ = await self.read_raw(volume, file_dir, file_path, opts.read_data)
        
        fi = await get_file_info(data, volume, path, version_id, 
                                FileInfoOpts(data=opts.read_data))
        return fi
    
    async def read_xl(self, volume: str, path: str, read_data: bool) -> RawFileInfo:
        file_path = self.get_object_path(volume, path)
        file_dir = self.get_bucket_path(volume)
        
        buf, _ = await self.read_raw(volume, file_dir, file_path, read_data)
        
        return RawFileInfo(buf=buf)
    
    async def delete_version(self, volume: str, path: str, fi: FileInfo, 
                           force_del_marker: bool, opts: DeleteOptions):
        if path.startswith(SLASH_SEPARATOR):
            return await self.delete(volume, path, DeleteOptions(recursive=False, immediate=False))
        
        volume_dir = self.get_bucket_path(volume)
        file_path = volume_dir / path
        check_path_length(str(file_path))
        
        xl_path = file_path / STORAGE_FORMAT_FILE
        
        try:
            buf = await self.read_all_data(volume, volume_dir, xl_path)
        except Error as err:
            if err != DiskError.FileNotFound:
                raise
            
            if fi.deleted and force_del_marker:
                return await self.write_metadata("", volume, path, fi)
            
            if fi.version_id:
                raise DiskError.FileVersionNotFound
            else:
                raise DiskError.FileNotFound
        
        meta = FileMeta.load(buf)
        old_dir = meta.delete_version(fi)
        
        if old_dir:
            vid = fi.version_id or ""
            meta.data.remove([vid, old_dir])
            
            old_path = file_path / str(old_dir)
            check_path_length(str(old_path))
            
            try:
                await self.move_to_trash(old_path, True, False)
            except Error as err:
                if err not in (DiskError.FileNotFound, DiskError.VolumeNotFound):
                    raise
        
        if meta.versions:
            buf = meta.marshal_msg()
            return await self.write_all_meta(volume, f"{path}/{STORAGE_FORMAT_FILE}", buf, True)
        
        if opts.old_data_dir and opts.undo_write:
            src_path = file_path / f"{opts.old_data_dir}/{STORAGE_FORMAT_FILE_BACKUP}"
            dst_path = file_path / f"{path}/{STORAGE_FORMAT_FILE}"
            return await rename_all(src_path, dst_path, file_path)
        
        await self.delete_file(volume_dir, xl_path, True, False)
    
    async def delete_versions(self, volume: str, versions: List[FileInfoVersions], 
                            opts: DeleteOptions) -> List[Optional[Error]]:
        errs = [None] * len(versions)
        
        for i, ver in enumerate(versions):
            try:
                await self.delete_versions_internal(volume, ver.name, ver.versions)
            except Error as e:
                errs[i] = e
        
        return errs
    
    async def read_multiple(self, req: ReadMultipleReq) -> List[ReadMultipleResp]:
        results = []
        found = 0
        
        for v in req.files:
            fpath = self.get_object_path(req.bucket, f"{req.prefix}/{v}")
            res = ReadMultipleResp(
                bucket=req.bucket,
                prefix=req.prefix,
                file=v
            )
            
            try:
                data, meta = await read_file_all(fpath)
                found += 1
                
                if req.max_size > 0 and len(data) > req.max_size:
                    res.exists = True
                    res.error = f"max size ({req.max_size}) exceeded: {len(data)}"
                    results.append(res)
                    break
                
                res.exists = True
                res.data = data
                res.mod_time = datetime.fromtimestamp(meta.st_mtime, timezone.utc)
                results.append(res)
                
                if req.max_results > 0 and found >= req.max_results:
                    break
            except Error as e:
                if e not in (DiskError.FileNotFound, DiskError.VolumeNotFound):
                    res.exists = True
                    res.error = str(e)
                
                if req.abort404 and not res.exists:
                    results.append(res)
                    break
                
                results.append(res)
        
        return results
    
    async def delete_volume(self, volume: str):
        p = self.get_bucket_path(volume)
        
        try:
            shutil.rmtree(p)
        except FileNotFoundError:
            pass
        except Exception as err:
            raise to_volume_error(err)
    
    async def disk_info(self, opts: DiskInfoOptions) -> DiskInfo:
        info = await self.disk_info_cache.get()
        info.nr_requests = self.nrrequests
        info.rotational = self.rotational
        info.mount_path = str(self.path())
        info.endpoint = str(self.endpoint)
        info.scanning = self.scanning == 1
        
        return info


def is_root_path(path: Path) -> bool:
    return len(path.parts) == 1 and path.is_absolute()


async def read_file_exists(path: Path) -> Tuple[bytes, Optional[os.stat_result]]:
    try:
        data, meta = await read_file_all(path)
        return data, meta
    except FileNotFoundError:
        return b'', None


async def read_file_all(path: Path) -> Tuple[bytes, os.stat_result]:
    try:
        async with aiofiles.open(path, 'rb') as f:
            data = await f.read()
        meta = await aiofiles.os.stat(path)
        return data, meta
    except FileNotFoundError:
        raise to_file_error(FileNotFoundError())


async def read_file_metadata(path: Path) -> os.stat_result:
    try:
        return await aiofiles.os.stat(path)
    except FileNotFoundError:
        raise to_file_error(FileNotFoundError())


def skip_access_checks(p: str) -> bool:
    vols = [
        RUSTFS_META_TMP_DELETED_BUCKET,
        ".minio.sys/tmp",
        ".minio.sys/multipart",
        RUSTFS_META_BUCKET
    ]
    
    return any(p.startswith(v) for v in vols)


async def get_disk_info(drive_path: Path) -> Tuple[Any, bool]:
    drive_path_str = str(drive_path)
    check_path_length(drive_path_str)
    
    disk_info = get_info(drive_path_str)
    
    is_erasure = await GLOBAL_IsErasureSD.read()
    root_disk_threshold = await GLOBAL_RootDiskThreshold.read()
    
    root_drive = False
    if not is_erasure:
        if root_disk_threshold > 0:
            root_drive = disk_info.total <= root_disk_threshold
        else:
            root_drive = is_root_disk(drive_path_str, SLASH_SEPARATOR) or False
    
    return disk_info, root_drive


def same_file(stat1: os.stat_result, stat2: os.stat_result) -> bool:
    return (stat1.st_ino == stat2.st_ino and 
            stat1.st_dev == stat2.st_dev and
            stat1.st_size == stat2.st_size and
            stat1.st_mtime == stat2.st_mtime)


async def read_dir(path: Path, count: int) -> List[str]:
    entries = []
    try:
        for entry in path.iterdir():
            if count > 0 and len(entries) >= count:
                break
            
            name = entry.name
            if entry.is_dir():
                name += SLASH_SEPARATOR
            entries.append(name)
    except Exception as e:
        raise to_volume_error(e)
    
    return entries


async def read_file(path: Path) -> bytes:
    try:
        async with aiofiles.open(path, 'rb') as f:
            return await f.read()
    except Exception as e:
        raise to_file_error(e)


def access_std(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Path not found: {path}")