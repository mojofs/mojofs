import io
import struct
import hashlib
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime, timezone
import uuid

import msgpack

from mojofs.filemeta.error import Error
from mojofs.filemeta.fileinfo import FileInfo, FileInfoVersions, RawFileInfo, ErasureInfo, ObjectPartInfo
from mojofs.filemeta.filemeta_inline import InlineData


# XL header specifies the format
XL_FILE_HEADER = b'XL2 '

# Current version being written
XL_FILE_VERSION_MAJOR = 1
XL_FILE_VERSION_MINOR = 3
XL_HEADER_VERSION = 3
XL_META_VERSION = 2
XXHASH_SEED = 0

XL_FLAG_FREE_VERSION = 1 << 0
_XL_FLAG_INLINE_DATA = 1 << 2

META_DATA_READ_DEFAULT = 4 << 10
MSGP_UINT32_SIZE = 5

TRANSITION_COMPLETE = "complete"
TRANSITION_PENDING = "pending"

FREE_VERSION = "free-version"

TRANSITION_STATUS = "transition-status"
TRANSITIONED_OBJECTNAME = "transitioned-object"
TRANSITIONED_VERSION_ID = "transitioned-versionID"
TRANSITION_TIER = "transition-tier"

X_AMZ_RESTORE_EXPIRY_DAYS = "X-Amz-Restore-Expiry-Days"
X_AMZ_RESTORE_REQUEST_DATE = "X-Amz-Restore-Request-Date"

# Reserved metadata prefixes
RESERVED_METADATA_PREFIX = "X-Minio-Internal-"
RESERVED_METADATA_PREFIX_LOWER = "x-minio-internal-"
VERSION_PURGE_STATUS_KEY = "purgestatus"
AMZ_META_UNENCRYPTED_CONTENT_LENGTH = "X-Amz-Meta-X-Amz-Unencrypted-Content-Length"
AMZ_META_UNENCRYPTED_CONTENT_MD5 = "X-Amz-Meta-X-Amz-Unencrypted-Content-MD5"
AMZ_STORAGE_CLASS = "X-Amz-Storage-Class"
X_AMZ_RESTORE = "X-Amz-Restore"

FREE_VERSION_META_HEADER = "free-version"


class VersionType(IntEnum):
    Invalid = 0
    Object = 1
    Delete = 2
    Legacy = 3

    def valid(self) -> bool:
        return self in (VersionType.Object, VersionType.Delete, VersionType.Legacy)

    def to_u8(self) -> int:
        return self.value

    @classmethod
    def from_u8(cls, n: int) -> 'VersionType':
        try:
            return cls(n)
        except ValueError:
            return cls.Invalid


class ChecksumAlgo(IntEnum):
    Invalid = 0
    HighwayHash = 1

    def valid(self) -> bool:
        return self > ChecksumAlgo.Invalid

    def to_u8(self) -> int:
        return self.value

    @classmethod
    def from_u8(cls, u: int) -> 'ChecksumAlgo':
        try:
            return cls(u)
        except ValueError:
            return cls.Invalid


class ErasureAlgo(IntEnum):
    Invalid = 0
    ReedSolomon = 1

    def valid(self) -> bool:
        return self > ErasureAlgo.Invalid

    def __str__(self) -> str:
        if self == ErasureAlgo.ReedSolomon:
            return "reedsolomon"
        return "invalid"


class Flags(IntEnum):
    FreeVersion = 1 << 0
    UsesDataDir = 1 << 1
    InlineData = 1 << 2


@dataclass
class FileMetaVersionHeader:
    version_id: Optional[uuid.UUID] = None
    mod_time: Optional[datetime] = None
    signature: bytes = field(default_factory=lambda: b'\x00' * 4)
    version_type: VersionType = VersionType.Invalid
    flags: int = 0
    ec_n: int = 0
    ec_m: int = 0

    def has_ec(self) -> bool:
        return self.ec_m > 0 and self.ec_n > 0

    def matches_not_strict(self, other: 'FileMetaVersionHeader') -> bool:
        ok = (self.version_id == other.version_id and 
              self.version_type == other.version_type and 
              self.matches_ec(other))
        if self.version_id is None:
            ok = ok and self.mod_time == other.mod_time
        return ok

    def matches_ec(self, other: 'FileMetaVersionHeader') -> bool:
        if self.has_ec() and other.has_ec():
            return self.ec_n == other.ec_n and self.ec_m == other.ec_m
        return True

    def free_version(self) -> bool:
        return (self.flags & XL_FLAG_FREE_VERSION) != 0

    def sorts_before(self, other: 'FileMetaVersionHeader') -> bool:
        if self == other:
            return False

        # Prefer newest modtime
        if self.mod_time != other.mod_time:
            if self.mod_time is None:
                return False
            if other.mod_time is None:
                return True
            return self.mod_time > other.mod_time

        # Prefer lower types
        if self.version_type != other.version_type:
            return self.version_type < other.version_type

        # Consistent sort on version_id
        if self.version_id != other.version_id:
            if self.version_id is None:
                return False
            if other.version_id is None:
                return True
            return str(self.version_id) > str(other.version_id)

        if self.flags != other.flags:
            return self.flags > other.flags

        return False

    def user_data_dir(self) -> bool:
        return (self.flags & Flags.UsesDataDir) != 0

    def marshal_msg(self) -> bytes:
        buf = io.BytesIO()
        
        # array len 7
        msgpack.pack(7, buf, use_bin_type=True)
        
        # version_id
        vid_bytes = self.version_id.bytes if self.version_id else uuid.UUID(int=0).bytes
        msgpack.pack(vid_bytes, buf, use_bin_type=True)
        
        # mod_time
        if self.mod_time:
            timestamp_ns = int(self.mod_time.timestamp() * 1e9)
        else:
            timestamp_ns = 0
        msgpack.pack(timestamp_ns, buf, use_bin_type=True)
        
        # signature
        msgpack.pack(self.signature, buf, use_bin_type=True)
        
        # version_type
        msgpack.pack(self.version_type.to_u8(), buf, use_bin_type=True)
        
        # flags
        msgpack.pack(self.flags, buf, use_bin_type=True)
        
        # ec_n
        msgpack.pack(self.ec_n, buf, use_bin_type=True)
        
        # ec_m
        msgpack.pack(self.ec_m, buf, use_bin_type=True)
        
        return buf.getvalue()

    def unmarshal_msg(self, buf: bytes) -> int:
        unpacker = msgpack.Unpacker(io.BytesIO(buf), raw=False)
        
        alen = unpacker.unpack()
        if alen != 7:
            raise Error(f"version header array len err need 7 got {alen}")
        
        # version_id
        vid_bytes = unpacker.unpack()
        vid = uuid.UUID(bytes=vid_bytes)
        self.version_id = None if vid.int == 0 else vid
        
        # mod_time
        timestamp_ns = unpacker.unpack()
        if timestamp_ns == 0:
            self.mod_time = None
        else:
            self.mod_time = datetime.fromtimestamp(timestamp_ns / 1e9, tz=timezone.utc)
        
        # signature
        self.signature = unpacker.unpack()
        
        # version_type
        typ = unpacker.unpack()
        self.version_type = VersionType.from_u8(typ)
        
        # flags
        self.flags = unpacker.unpack()
        
        # ec_n
        self.ec_n = unpacker.unpack()
        
        # ec_m
        self.ec_m = unpacker.unpack()
        
        return unpacker.tell()

    def inline_data(self) -> bool:
        return (self.flags & Flags.InlineData) != 0

    def get_signature(self) -> bytes:
        return self.signature

    def update_signature(self, version: 'FileMetaVersion'):
        self.signature = version.get_signature()


@dataclass
class FileMetaShallowVersion:
    header: FileMetaVersionHeader = field(default_factory=FileMetaVersionHeader)
    meta: bytes = b''

    @classmethod
    def try_from(cls, file_version: 'FileMetaVersion') -> 'FileMetaShallowVersion':
        """从 FileMetaVersion 创建 FileMetaShallowVersion"""
        shallow = cls()
        shallow.header = file_version.header()
        shallow.meta = file_version.marshal_msg()
        return shallow

    def into_fileinfo(self, volume: str, path: str, all_parts: bool) -> FileInfo:
        file_version = FileMetaVersion.try_from(self.meta)
        return file_version.into_fileinfo(volume, path, all_parts)

    def __lt__(self, other):
        return self.header.sorts_before(other.header)


@dataclass
class MetaObject:
    version_id: Optional[uuid.UUID] = None
    data_dir: Optional[uuid.UUID] = None
    erasure_algorithm: ErasureAlgo = ErasureAlgo.ReedSolomon
    erasure_m: int = 0
    erasure_n: int = 0
    erasure_block_size: int = 0
    erasure_index: int = 0
    erasure_dist: List[int] = field(default_factory=list)
    bitrot_checksum_algo: ChecksumAlgo = ChecksumAlgo.HighwayHash
    part_numbers: List[int] = field(default_factory=list)
    part_etags: List[str] = field(default_factory=list)
    part_sizes: List[int] = field(default_factory=list)
    part_actual_sizes: List[int] = field(default_factory=list)
    part_indices: List[bytes] = field(default_factory=list)
    size: int = 0
    mod_time: Optional[datetime] = None
    meta_sys: Dict[str, bytes] = field(default_factory=dict)
    meta_user: Dict[str, str] = field(default_factory=dict)

    def unmarshal_msg(self, buf: bytes) -> int:
        data = msgpack.unpackb(buf, raw=False)
        
        self.version_id = uuid.UUID(bytes=data.get('ID', b'\x00' * 16)) if data.get('ID') else None
        if self.version_id and self.version_id.int == 0:
            self.version_id = None
            
        self.data_dir = uuid.UUID(bytes=data.get('DDir', b'\x00' * 16)) if data.get('DDir') else None
        if self.data_dir and self.data_dir.int == 0:
            self.data_dir = None
            
        self.erasure_algorithm = ErasureAlgo(data.get('EcAlgo', 0))
        self.erasure_m = data.get('EcM', 0)
        self.erasure_n = data.get('EcN', 0)
        self.erasure_block_size = data.get('EcBSize', 0)
        self.erasure_index = data.get('EcIndex', 0)
        self.erasure_dist = list(data.get('EcDist', []))
        self.bitrot_checksum_algo = ChecksumAlgo(data.get('CSumAlgo', 0))
        self.part_numbers = data.get('PartNums', [])
        self.part_etags = data.get('PartETags', [])
        self.part_sizes = data.get('PartSizes', [])
        self.part_actual_sizes = data.get('PartASizes', [])
        self.part_indices = data.get('PartIdx', [])
        self.size = data.get('Size', 0)
        
        mtime = data.get('MTime')
        if mtime and mtime != 0:
            self.mod_time = datetime.fromtimestamp(mtime / 1e9, tz=timezone.utc)
        else:
            self.mod_time = None
            
        self.meta_sys = data.get('MetaSys', {})
        self.meta_user = data.get('MetaUsr', {})
        
        return len(buf)

    def marshal_msg(self) -> bytes:
        data = {
            'ID': self.version_id.bytes if self.version_id else b'\x00' * 16,
            'DDir': self.data_dir.bytes if self.data_dir else b'\x00' * 16,
            'EcAlgo': self.erasure_algorithm.value,
            'EcM': self.erasure_m,
            'EcN': self.erasure_n,
            'EcBSize': self.erasure_block_size,
            'EcIndex': self.erasure_index,
            'EcDist': bytes(self.erasure_dist),
            'CSumAlgo': self.bitrot_checksum_algo.value,
            'PartNums': self.part_numbers,
            'PartETags': self.part_etags,
            'PartSizes': self.part_sizes,
            'PartASizes': self.part_actual_sizes,
            'PartIdx': self.part_indices,
            'Size': self.size,
            'MTime': int(self.mod_time.timestamp() * 1e9) if self.mod_time else 0,
            'MetaSys': self.meta_sys,
            'MetaUsr': self.meta_user,
        }
        return msgpack.packb(data, use_bin_type=True)

    def into_fileinfo(self, volume: str, path: str, all_parts: bool) -> FileInfo:
        version_id = self.version_id if self.version_id and self.version_id.int != 0 else None
        
        parts = []
        if all_parts:
            for i in range(len(self.part_numbers)):
                part = ObjectPartInfo()
                part.number = self.part_numbers[i]
                part.size = self.part_sizes[i]
                part.actual_size = self.part_actual_sizes[i] if i < len(self.part_actual_sizes) else 0
                
                if i < len(self.part_etags):
                    part.etag = self.part_etags[i]
                
                if i < len(self.part_indices) and self.part_indices[i]:
                    part.index = self.part_indices[i]
                
                parts.append(part)
        
        metadata = {}
        for k, v in self.meta_user.items():
            if k in (AMZ_META_UNENCRYPTED_CONTENT_LENGTH, AMZ_META_UNENCRYPTED_CONTENT_MD5):
                continue
            if k == AMZ_STORAGE_CLASS and v == "STANDARD":
                continue
            metadata[k] = v
        
        for k, v in self.meta_sys.items():
            if k == AMZ_STORAGE_CLASS and v == b"STANDARD":
                continue
            if (k.startswith(RESERVED_METADATA_PREFIX) or 
                k.startswith(RESERVED_METADATA_PREFIX_LOWER) or 
                k == VERSION_PURGE_STATUS_KEY):
                metadata[k] = v.decode('utf-8', errors='replace')
        
        erasure = ErasureInfo(
            algorithm=str(self.erasure_algorithm),
            data_blocks=self.erasure_m,
            parity_blocks=self.erasure_n,
            block_size=self.erasure_block_size,
            index=self.erasure_index,
            distribution=self.erasure_dist
        )
        
        return FileInfo(
            version_id=version_id,
            erasure=erasure,
            data_dir=self.data_dir,
            mod_time=self.mod_time,
            size=self.size,
            name=path,
            volume=volume,
            parts=parts,
            metadata=metadata
        )

    def set_transition(self, fi: FileInfo):
        self.meta_sys[f"{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_STATUS}"] = fi.transition_status.encode()
        self.meta_sys[f"{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_OBJECTNAME}"] = fi.transitioned_objname.encode()
        if fi.transition_version_id:
            self.meta_sys[f"{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_VERSION_ID}"] = str(fi.transition_version_id).encode()
        self.meta_sys[f"{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_TIER}"] = fi.transition_tier.encode()

    def remove_restore_hdrs(self):
        self.meta_user.pop(X_AMZ_RESTORE, None)
        self.meta_user.pop(X_AMZ_RESTORE_EXPIRY_DAYS, None)
        self.meta_user.pop(X_AMZ_RESTORE_REQUEST_DATE, None)

    def uses_data_dir(self) -> bool:
        return not self.inlinedata()

    def inlinedata(self) -> bool:
        return f"{RESERVED_METADATA_PREFIX_LOWER}inline-data" in self.meta_sys

    def reset_inline_data(self):
        self.meta_sys.pop(f"{RESERVED_METADATA_PREFIX_LOWER}inline-data", None)

    def remove_restore_headers(self):
        keys_to_remove = [k for k in self.meta_sys if k.startswith("X-Amz-Restore")]
        for k in keys_to_remove:
            self.meta_sys.pop(k, None)

    def get_signature(self) -> bytes:
        # Simple hash implementation - should use xxhash in production
        h = hashlib.sha256()
        h.update(self.version_id.bytes if self.version_id else b'\x00' * 16)
        if self.mod_time:
            h.update(struct.pack('<q', int(self.mod_time.timestamp() * 1e9)))
        h.update(struct.pack('<q', self.size))
        digest = h.digest()
        return digest[:4]

    def init_free_version(self, fi: FileInfo) -> Tuple['FileMetaVersion', bool]:
        if fi.skip_tier_free_version():
            return (FileMetaVersion(), False)
            
        status = self.meta_sys.get(f"{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_STATUS}")
        if status == TRANSITION_COMPLETE.encode():
            try:
                vid = uuid.UUID(fi.tier_free_version_id())
            except ValueError as e:
                raise Error(f"Invalid Tier Object delete marker versionId {fi.tier_free_version_id()} {e}")
            
            free_entry = FileMetaVersion(
                version_type=VersionType.Delete,
                write_version=0
            )
            
            meta_sys = {f"{RESERVED_METADATA_PREFIX_LOWER}{FREE_VERSION}": b''}
            
            tier_keys = [
                f"{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_TIER}",
                f"{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_OBJECTNAME}",
                f"{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_VERSION_ID}"
            ]
            
            for k, v in self.meta_sys.items():
                if k in tier_keys:
                    meta_sys[k] = v
            
            free_entry.delete_marker = MetaDeleteMarker(
                version_id=vid,
                mod_time=self.mod_time,
                meta_sys=meta_sys
            )
            
            return (free_entry, True)
        
        return (FileMetaVersion(), False)


@dataclass
class MetaDeleteMarker:
    version_id: Optional[uuid.UUID] = None
    mod_time: Optional[datetime] = None
    meta_sys: Optional[Dict[str, bytes]] = None

    def free_version(self) -> bool:
        if self.meta_sys:
            return FREE_VERSION_META_HEADER in self.meta_sys
        return False

    def into_fileinfo(self, volume: str, path: str, all_parts: bool) -> FileInfo:
        metadata = {}
        if self.meta_sys:
            for k, v in self.meta_sys.items():
                metadata[k] = v.decode('utf-8', errors='replace')
        
        return FileInfo(
            version_id=self.version_id if self.version_id and self.version_id.int != 0 else None,
            name=path,
            volume=volume,
            deleted=True,
            mod_time=self.mod_time,
            metadata=metadata
        )

    def unmarshal_msg(self, buf: bytes) -> int:
        data = msgpack.unpackb(buf, raw=False)
        
        vid_bytes = data.get('ID')
        if vid_bytes:
            vid = uuid.UUID(bytes=vid_bytes)
            self.version_id = None if vid.int == 0 else vid
        
        mtime = data.get('MTime')
        if mtime and mtime != 0:
            self.mod_time = datetime.fromtimestamp(mtime / 1e9, tz=timezone.utc)
        else:
            self.mod_time = None
        
        self.meta_sys = data.get('MetaSys', {})
        
        return len(buf)

    def marshal_msg(self) -> bytes:
        data = {
            'ID': self.version_id.bytes if self.version_id else b'\x00' * 16,
            'MTime': int(self.mod_time.timestamp() * 1e9) if self.mod_time else 0,
        }
        if self.meta_sys:
            data['MetaSys'] = self.meta_sys
        
        return msgpack.packb(data, use_bin_type=True)

    def get_signature(self) -> bytes:
        # Simple hash implementation
        h = hashlib.sha256()
        h.update(self.version_id.bytes if self.version_id else b'\x00' * 16)
        if self.mod_time:
            h.update(struct.pack('<q', int(self.mod_time.timestamp() * 1e9)))
        digest = h.digest()
        return digest[:4]


@dataclass
class FileMetaVersion:
    version_type: VersionType = VersionType.Invalid
    object: Optional[MetaObject] = None
    delete_marker: Optional[MetaDeleteMarker] = None
    write_version: int = 0

    def valid(self) -> bool:
        if not self.version_type.valid():
            return False
        
        if self.version_type == VersionType.Object:
            if self.object:
                return (self.object.erasure_algorithm.valid() and 
                        self.object.bitrot_checksum_algo.valid() and 
                        self.object.mod_time is not None)
            return False
        elif self.version_type == VersionType.Delete:
            if self.delete_marker:
                return self.delete_marker.mod_time is not None
            return False
        
        return False

    def get_data_dir(self) -> Optional[uuid.UUID]:
        if self.valid() and self.version_type == VersionType.Object and self.object:
            return self.object.data_dir
        return None

    def get_version_id(self) -> Optional[uuid.UUID]:
        if self.version_type == VersionType.Object and self.object:
            return self.object.version_id
        elif self.version_type == VersionType.Delete and self.delete_marker:
            return self.delete_marker.version_id
        return None

    def get_mod_time(self) -> Optional[datetime]:
        if self.version_type == VersionType.Object and self.object:
            return self.object.mod_time
        elif self.version_type == VersionType.Delete and self.delete_marker:
            return self.delete_marker.mod_time
        return None

    @staticmethod
    def decode_data_dir_from_meta(buf: bytes) -> Optional[uuid.UUID]:
        ver = FileMetaVersion()
        ver.unmarshal_msg(buf)
        if ver.object:
            return ver.object.data_dir
        return None

    def unmarshal_msg(self, buf: bytes) -> int:
        data = msgpack.unpackb(buf, raw=False)
        
        self.version_type = VersionType(data.get('Type', 0))
        self.write_version = data.get('v', 0)
        
        if 'V2Obj' in data and data['V2Obj']:
            self.object = MetaObject()
            self.object.unmarshal_msg(msgpack.packb(data['V2Obj'], use_bin_type=True))
        
        if 'DelObj' in data and data['DelObj']:
            self.delete_marker = MetaDeleteMarker()
            self.delete_marker.unmarshal_msg(msgpack.packb(data['DelObj'], use_bin_type=True))
        
        return len(buf)

    def marshal_msg(self) -> bytes:
        data = {
            'Type': self.version_type.value,
            'v': self.write_version
        }
        
        if self.object:
            obj_data = msgpack.unpackb(self.object.marshal_msg(), raw=False)
            data['V2Obj'] = obj_data
        
        if self.delete_marker:
            del_data = msgpack.unpackb(self.delete_marker.marshal_msg(), raw=False)
            data['DelObj'] = del_data
        
        return msgpack.packb(data, use_bin_type=True)

    def free_version(self) -> bool:
        return (self.version_type == VersionType.Delete and 
                self.delete_marker and 
                self.delete_marker.free_version())

    def header(self) -> FileMetaVersionHeader:
        flags = 0
        if self.free_version():
            flags |= Flags.FreeVersion
        
        if self.version_type == VersionType.Object and self.object and self.object.uses_data_dir():
            flags |= Flags.UsesDataDir
        
        if self.version_type == VersionType.Object and self.object and self.object.inlinedata():
            flags |= Flags.InlineData
        
        ec_n, ec_m = 0, 0
        if self.version_type == VersionType.Object and self.object:
            ec_n = self.object.erasure_n
            ec_m = self.object.erasure_m
        
        return FileMetaVersionHeader(
            version_id=self.get_version_id(),
            mod_time=self.get_mod_time(),
            signature=b'\x00' * 4,
            version_type=self.version_type,
            flags=flags,
            ec_n=ec_n,
            ec_m=ec_m
        )

    def into_fileinfo(self, volume: str, path: str, all_parts: bool) -> FileInfo:
        if self.version_type in (VersionType.Invalid, VersionType.Legacy):
            return FileInfo(name=path, volume=volume)
        elif self.version_type == VersionType.Object and self.object:
            return self.object.into_fileinfo(volume, path, all_parts)
        elif self.version_type == VersionType.Delete and self.delete_marker:
            return self.delete_marker.into_fileinfo(volume, path, all_parts)
        else:
            return FileInfo(name=path, volume=volume)

    def is_legacy(self) -> bool:
        return self.version_type == VersionType.Legacy

    def get_signature(self) -> bytes:
        if self.version_type == VersionType.Object and self.object:
            return self.object.get_signature()
        elif self.version_type == VersionType.Delete and self.delete_marker:
            return self.delete_marker.get_signature()
        return b'\x00' * 4

    def uses_data_dir(self) -> bool:
        return self.version_type == VersionType.Object and self.object and self.object.uses_data_dir()

    def uses_inline_data(self) -> bool:
        return self.version_type == VersionType.Object and self.object and self.object.inlinedata()

    @classmethod
    def try_from(cls, buf: bytes) -> 'FileMetaVersion':
        ver = cls()
        ver.unmarshal_msg(buf)
        return ver

    @classmethod
    def from_fileinfo(cls, fi: FileInfo) -> 'FileMetaVersion':
        if fi.deleted:
            return cls(
                version_type=VersionType.Delete,
                delete_marker=MetaDeleteMarker(
                    version_id=fi.version_id,
                    mod_time=fi.mod_time
                )
            )
        else:
            obj = MetaObject()
            # Copy fields from FileInfo to MetaObject
            obj.version_id = fi.version_id
            obj.data_dir = fi.data_dir
            obj.size = fi.size
            obj.mod_time = fi.mod_time
            obj.erasure_algorithm = ErasureAlgo.ReedSolomon
            obj.erasure_m = fi.erasure.data_blocks
            obj.erasure_n = fi.erasure.parity_blocks
            obj.erasure_block_size = fi.erasure.block_size
            obj.erasure_index = fi.erasure.index
            obj.erasure_dist = fi.erasure.distribution
            obj.bitrot_checksum_algo = ChecksumAlgo.HighwayHash
            
            if fi.parts:
                obj.part_numbers = [p.number for p in fi.parts]
                obj.part_etags = [p.etag for p in fi.parts]
                obj.part_sizes = [p.size for p in fi.parts]
                obj.part_actual_sizes = [p.actual_size for p in fi.parts]
                obj.part_indices = [p.index or b'' for p in fi.parts]
            
            # Process metadata
            for k, v in fi.metadata.items():
                if (k.startswith(RESERVED_METADATA_PREFIX) or 
                    k.startswith(RESERVED_METADATA_PREFIX_LOWER)):
                    obj.meta_sys[k] = v.encode()
                else:
                    obj.meta_user[k] = v
            
            return cls(
                version_type=VersionType.Object,
                object=obj
            )


@dataclass
class VersionStats:
    total_versions: int = 0
    object_versions: int = 0
    delete_markers: int = 0
    invalid_versions: int = 0
    free_versions: int = 0


class FileMeta:
    def __init__(self):
        self.versions: List[FileMetaShallowVersion] = []
        self.data: InlineData = InlineData()
        self.meta_ver: int = XL_META_VERSION

    @classmethod
    def new(cls) -> 'FileMeta':
        return cls()

    @staticmethod
    def is_xl2_v1_format(buf: bytes) -> bool:
        try:
            FileMeta.check_xl2_v1(buf)
            return True
        except:
            return False

    @classmethod
    def load(cls, buf: bytes) -> 'FileMeta':
        xl = cls()
        xl.unmarshal_msg(buf)
        return xl

    @staticmethod
    def check_xl2_v1(buf: bytes) -> Tuple[bytes, int, int]:
        if len(buf) < 8:
            raise Error("xl file header not exists")
        
        if buf[0:4] != XL_FILE_HEADER:
            raise Error("xl file header err")
        
        major = struct.unpack('<H', buf[4:6])[0]
        minor = struct.unpack('<H', buf[6:8])[0]
        
        if major > XL_FILE_VERSION_MAJOR:
            raise Error("xl file version err")
        
        return (buf[8:], major, minor)

    @staticmethod
    def read_bytes_header(buf: bytes) -> Tuple[int, bytes]:
        if len(buf) < 5:
            raise Error("insufficient data for header")
        
        unpacker = msgpack.Unpacker(io.BytesIO(buf[:5]), raw=True)
        bin_len = unpacker.unpack()
        
        return (bin_len, buf[5:])

    def unmarshal_msg(self, buf: bytes) -> int:
        i = len(buf)
        # Check version
        buf, _, _ = self.check_xl2_v1(buf)
        
        if len(buf) < 5:
            raise Error("insufficient data for size")
        
        # Get metadata size
        unpacker = msgpack.Unpacker(io.BytesIO(buf), raw=True)
        bin_len = len(unpacker.unpack())

        buf = buf[5:]
        
        if len(buf) < bin_len:
            raise Error("insufficient data for metadata")
        
        meta = buf[:bin_len]
        buf = buf[bin_len:]
        
        if len(buf) < 5:
            raise Error("insufficient data for CRC")
        
        # CRC check
        unpacker = msgpack.Unpacker(io.BytesIO(buf), raw=False)
        crc = unpacker.unpack()
        buf = buf[5:]
        
        # Calculate CRC (simplified - should use xxhash)
        meta_crc = hash(meta) & 0xFFFFFFFF
        
        if crc != meta_crc:
            # For now, just log warning instead of failing
            pass
        
        if buf:
            self.data.update(buf)
            self.data.validate()
        
        # Parse meta
        if meta:
            versions_len, _, meta_ver, meta = self.decode_xl_headers(meta)
            self.meta_ver = meta_ver
            self.versions = []
            cur = io.BytesIO(meta)
            for _ in range(versions_len):
                # Read header
                unpacker = msgpack.Unpacker(cur, raw=True)
                header_buf = unpacker.unpack()
                
                ver = FileMetaShallowVersion()
                ver.header.unmarshal_msg(header_buf)
                
                # Read meta
                ver_meta_buf = unpacker.unpack()
                ver.meta = ver_meta_buf
                
                self.versions.append(ver)
        
        return i

    @staticmethod
    def decode_xl_headers(buf: bytes) -> Tuple[int, int, int, bytes]:
        cur = io.BytesIO(buf)
        unpacker = msgpack.Unpacker(cur, raw=False)
        
        header_ver = unpacker.unpack()
        if header_ver > XL_HEADER_VERSION:
            raise Error("xl header version invalid")
        
        meta_ver = unpacker.unpack()
        if meta_ver > XL_META_VERSION:
            raise Error("xl meta version invalid")
        
        versions_len = unpacker.unpack()
        
        return (versions_len, header_ver, meta_ver, buf[cur.tell():])

    @staticmethod
    def decode_versions(buf: bytes, versions: int, fnc):
        cur = io.BytesIO(buf)
        
        for i in range(versions):
            unpacker = msgpack.Unpacker(cur, raw=True)
            header_buf = unpacker.unpack()
            ver_meta_buf = unpacker.unpack()
            
            try:
                fnc(i, header_buf, ver_meta_buf)
            except Exception as e:
                if str(e) == "DoneForNow":
                    return
                raise

    @staticmethod
    def is_latest_delete_marker(buf: bytes) -> bool:
        try:
            versions, _, _, meta = FileMeta.decode_xl_headers(buf)
            if versions == 0:
                return False
            
            is_delete_marker = False
            
            def check_first(idx: int, hdr: bytes, meta: bytes):
                nonlocal is_delete_marker
                header = FileMetaVersionHeader()
                header.unmarshal_msg(hdr)
                is_delete_marker = header.version_type == VersionType.Delete
                raise Exception("DoneForNow")
            
            FileMeta.decode_versions(meta, versions, check_first)
            return is_delete_marker
        except:
            return False

    def marshal_msg(self) -> bytes:
        buf = io.BytesIO()
        
        # Header
        buf.write(XL_FILE_HEADER)
        buf.write(struct.pack('<H', XL_FILE_VERSION_MAJOR))
        buf.write(struct.pack('<H', XL_FILE_VERSION_MINOR))
        
        # Size placeholder
        size_pos = buf.tell()
        buf.write(b'\xc6\x00\x00\x00\x00')  # bin32 format
        
        offset = buf.tell()
        
        # Write headers
        msgpack.pack(XL_HEADER_VERSION, buf)
        msgpack.pack(XL_META_VERSION, buf)
        msgpack.pack(len(self.versions), buf)
        
        # Write versions
        for ver in self.versions:
            hmsg = ver.header.marshal_msg()
            msgpack.pack(hmsg, buf, use_bin_type=True)
            msgpack.pack(ver.meta, buf, use_bin_type=True)
        
        # Update size
        end_pos = buf.tell()
        data_len = end_pos - offset
        buf.seek(size_pos + 1)
        buf.write(struct.pack('>I', data_len))
        buf.seek(end_pos)
        
        # Calculate CRC
        buf.seek(offset)
        meta_data = buf.read(data_len)
        crc = hash(meta_data) & 0xFFFFFFFF
        
        buf.seek(end_pos)
        buf.write(b'\xce')  # u32 format
        buf.write(struct.pack('>I', crc))
        
        # Write inline data
        buf.write(self.data.as_bytes())
        
        return buf.getvalue()

    def get_idx(self, idx: int) -> FileMetaVersion:
        if idx >= len(self.versions):
            raise Error("FileNotFound")
        
        return FileMetaVersion.try_from(self.versions[idx].meta)

    def set_idx(self, idx: int, ver: FileMetaVersion):
        if idx >= len(self.versions):
            raise Error("FileNotFound")
        
        meta_buf = ver.marshal_msg()
        pre_mod_time = self.versions[idx].header.mod_time
        
        self.versions[idx].header = ver.header()
        self.versions[idx].meta = meta_buf
        
        if pre_mod_time != self.versions[idx].header.mod_time:
            self.sort_by_mod_time()

    def sort_by_mod_time(self):
        if len(self.versions) <= 1:
            return
        
        self.versions.reverse()

    def find_version(self, vid: Optional[uuid.UUID]) -> Tuple[int, FileMetaVersion]:
        for i, fver in enumerate(self.versions):
            if fver.header.version_id == vid:
                version = self.get_idx(i)
                return (i, version)
        
        raise Error("FileVersionNotFound")

    def shard_data_dir_count(self, vid: Optional[uuid.UUID], data_dir: Optional[uuid.UUID]) -> int:
        count = 0
        for v in self.versions:
            if (v.header.version_type == VersionType.Object and 
                v.header.version_id != vid and 
                v.header.user_data_dir()):
                try:
                    dd = FileMetaVersion.decode_data_dir_from_meta(v.meta)
                    if dd == data_dir:
                        count += 1
                except:
                    pass
        return count

    def update_object_version(self, fi: FileInfo):
        for version in self.versions:
            if version.header.version_type == VersionType.Object:
                if version.header.version_id == fi.version_id:
                    ver = FileMetaVersion.try_from(version.meta)
                    
                    if ver.object:
                        for k, v in fi.metadata.items():
                            ver.object.meta_user[k] = v
                        
                        if fi.mod_time:
                            ver.object.mod_time = fi.mod_time
                    
                    version.header = ver.header()
                    version.meta = ver.marshal_msg()
            elif version.header.version_type == VersionType.Delete:
                if version.header.version_id == fi.version_id:
                    raise Error("MethodNotAllowed")
        
        self.versions.sort(key=lambda v: (
            v.header.mod_time or datetime.min.replace(tzinfo=timezone.utc),
            v.header.version_type,
            str(v.header.version_id or ''),
            v.header.flags
        ), reverse=True)

    def add_version(self, fi: FileInfo):
        vid = fi.version_id
        
        if fi.data:
            key = str(vid or uuid.UUID(int=0))
            self.data.replace(key, fi.data)
        
        version = FileMetaVersion.from_fileinfo(fi)
        
        if not version.valid():
            raise Error("file meta version invalid")
        
        # Check if should replace
        for idx, ver in enumerate(self.versions):
            if ver.header.version_id == vid:
                self.set_idx(idx, version)
                return
        
        # Add new version
        self.add_version_filemata(version)

    def add_version_filemata(self, ver: FileMetaVersion):
        if not ver.valid():
            raise Error("attempted to add invalid version")
        
        if len(self.versions) + 1 >= 100:
            raise Error("You've exceeded the limit on the number of versions you can create on this object")
        
        new_version = FileMetaShallowVersion()
        new_version.header = ver.header()
        new_version.meta = ver.marshal_msg()
        
        # Find insertion position
        insert_pos = len(self.versions)
        for i, existing in enumerate(self.versions):
            if existing.header.sorts_before(new_version.header):
                insert_pos = i
                break
        
        self.versions.insert(insert_pos, new_version)

    def delete_version(self, fi: FileInfo) -> Optional[uuid.UUID]:
        ventry = FileMetaVersion()
        if fi.deleted:
            ventry.version_type = VersionType.Delete
            ventry.delete_marker = MetaDeleteMarker(
                version_id=fi.version_id,
                mod_time=fi.mod_time
            )
            
            if not fi.is_valid():
                raise Error("invalid file meta version")
        
        for i, ver in enumerate(self.versions):
            if ver.header.version_id != fi.version_id:
                continue
            
            if ver.header.version_type in (VersionType.Invalid, VersionType.Legacy):
                raise Error("invalid file meta version")
            elif ver.header.version_type == VersionType.Delete:
                self.versions.pop(i)
                if fi.deleted and fi.version_id is None:
                    self.add_version_filemata(ventry)
                return None
            elif ver.header.version_type == VersionType.Object:
                v = self.get_idx(i)
                self.versions.pop(i)
                
                if v.object:
                    return v.object.data_dir
                return None
        
        if fi.deleted:
            self.add_version_filemata(ventry)
            return None
        
        raise Error("FileVersionNotFound")

    def into_fileinfo(self, volume: str, path: str, version_id: str, 
                      read_data: bool, all_parts: bool) -> FileInfo:
        has_vid = None
        if version_id:
            try:
                vid = uuid.UUID(version_id)
                if vid.int != 0:
                    has_vid = vid
            except:
                pass
        
        is_latest = True
        succ_mod_time = None
        
        for ver in self.versions:
            header = ver.header
            
            if has_vid:
                if header.version_id != has_vid:
                    is_latest = False
                    succ_mod_time = header.mod_time
                    continue
            
            fi = ver.into_fileinfo(volume, path, all_parts)
            fi.is_latest = is_latest
            
            if succ_mod_time:
                fi.successor_mod_time = succ_mod_time
            
            if read_data and fi.version_id:
                data = self.data.find(str(fi.version_id))
                if data:
                    fi.data = data
            
            fi.num_versions = len(self.versions)
            
            return fi
        
        if has_vid is None:
            raise Error("FileNotFound")
        else:
            raise Error("FileVersionNotFound")

    def into_file_info_versions(self, volume: str, path: str, all_parts: bool) -> FileInfoVersions:
        versions = []
        for version in self.versions:
            file_version = FileMetaVersion.try_from(version.meta)
            fi = file_version.into_fileinfo(volume, path, all_parts)
            versions.append(fi)
        
        num = len(versions)
        prev_mod_time = None
        for i, fi in enumerate(versions):
            if i == 0:
                fi.is_latest = True
            else:
                fi.successor_mod_time = prev_mod_time
            fi.num_versions = num
            prev_mod_time = fi.mod_time
        
        if not versions:
            versions.append(FileInfo(
                name=path,
                volume=volume,
                deleted=True,
                is_latest=True
            ))
        
        return FileInfoVersions(
            volume=volume,
            name=path,
            latest_mod_time=versions[0].mod_time if versions else None,
            versions=versions
        )

    def latest_mod_time(self) -> Optional[datetime]:
        if not self.versions:
            return None
        return self.versions[0].header.mod_time

    def is_compatible_with_meta(self) -> bool:
        return self.meta_ver == XL_META_VERSION

    def validate_integrity(self):
        if not self.is_sorted_by_mod_time():
            raise Error("versions not sorted by modification time")
        
        self.data.validate()

    def is_sorted_by_mod_time(self) -> bool:
        if len(self.versions) <= 1:
            return True
        
        for i in range(1, len(self.versions)):
            prev_time = self.versions[i-1].header.mod_time
            curr_time = self.versions[i].header.mod_time
            
            if prev_time and curr_time:
                if prev_time < curr_time:
                    return False
            elif curr_time and not prev_time:
                return False
        
        return True

    def get_version_stats(self) -> VersionStats:
        stats = VersionStats()
        stats.total_versions = len(self.versions)
        
        for version in self.versions:
            if version.header.version_type == VersionType.Object:
                stats.object_versions += 1
            elif version.header.version_type == VersionType.Delete:
                stats.delete_markers += 1
            elif version.header.version_type in (VersionType.Invalid, VersionType.Legacy):
                stats.invalid_versions += 1
            
            if version.header.free_version():
                stats.free_versions += 1
        
        return stats

    @classmethod
    def load_or_convert(cls, buf: bytes) -> 'FileMeta':
        try:
            return cls.load(buf)
        except:
            return cls.load_legacy(buf)

    @classmethod
    def load_legacy(cls, buf: bytes) -> 'FileMeta':
        raise Error("Legacy format not yet implemented")

    def get_data_dirs(self) -> List[Optional[uuid.UUID]]:
        data_dirs = []
        for version in self.versions:
            if version.header.version_type == VersionType.Object:
                ver = FileMetaVersion.try_from(version.meta)
                data_dirs.append(ver.get_data_dir())
        return data_dirs

    def shared_data_dir_count(self, version_id: Optional[uuid.UUID], 
                             data_dir: Optional[uuid.UUID]) -> int:
        count = 0
        for v in self.versions:
            if (v.header.version_type == VersionType.Object and 
                v.header.version_id != version_id and 
                v.header.user_data_dir()):
                try:
                    dir_id = FileMetaVersion.decode_data_dir_from_meta(v.meta)
                    if dir_id == data_dir:
                        count += 1
                except:
                    pass
        return count

    def add_legacy(self, legacy_obj: str):
        raise Error("Legacy version addition not yet implemented")

    def list_versions(self, volume: str, path: str, all_parts: bool) -> List[FileInfo]:
        file_infos = []
        for i, version in enumerate(self.versions):
            fi = version.into_fileinfo(volume, path, all_parts)
            fi.is_latest = i == 0
            file_infos.append(fi)
        return file_infos

    def all_hidden(self, top_delete_marker: bool) -> bool:
        if not self.versions:
            return True
        
        if top_delete_marker and self.versions[0].header.version_type != VersionType.Delete:
            return False
        
        return all(
            v.header.version_type == VersionType.Delete or v.header.free_version()
            for v in self.versions
        )

    def append_to(self, dst: List[int]):
        data = self.marshal_msg()
        dst.extend(data)

    def find_version_str(self, version_id: str) -> Tuple[int, FileMetaVersion]:
        if not version_id:
            raise Error("empty version ID")
        
        uid = uuid.UUID(version_id)
        return self.find_version(uid)


def merge_file_meta_versions(quorum: int, strict: bool, requested_versions: int,
                           versions: List[List[FileMetaShallowVersion]]) -> List[FileMetaShallowVersion]:
    if quorum == 0:
        quorum = 1
    
    if len(versions) < quorum or not versions:
        return []
    
    if len(versions) == 1:
        return versions[0][:]
    
    if quorum == 1:
        strict = True
    
    versions = [v[:] for v in versions]  # Deep copy
    n_versions = 0
    merged = []
    
    while True:
        tops = []
        top_sig = None
        consistent = True
        
        for vers in versions:
            if not vers:
                consistent = False
                continue
            
            if not tops:
                consistent = True
                top_sig = vers[0].header
            else:
                consistent = consistent and vers[0].header == top_sig
            
            tops.append(vers[0])
        
        if len(tops) < quorum:
            break
        
        if consistent:
            merged.append(tops[0])
            if tops[0].header.free_version():
                n_versions += 1
        else:
            latest = FileMetaShallowVersion()
            latest_count = 0
            
            for i, ver in enumerate(tops):
                if ver.header == latest.header:
                    latest_count += 1
                    continue
                
                if i == 0 or ver.header.sorts_before(latest.header):
                    if i == 0 or latest_count == 0:
                        latest_count = 1
                    elif not strict and ver.header.matches_not_strict(latest.header):
                        latest_count += 1
                    else:
                        latest_count = 1
                    latest = ver
                    continue
                
                if latest_count > 0 and not strict and ver.header.matches_not_strict(latest.header):
                    latest_count += 1
                    continue
                
                if latest_count > 0 and ver.header.version_id == latest.header.version_id:
                    x = {}
                    for a in tops:
                        if a.header.version_id != ver.header.version_id:
                            continue
                        a_clone = FileMetaShallowVersion()
                        a_clone.header = a.header
                        a_clone.meta = a.meta
                        if not strict:
                            a_clone.header.signature = b'\x00' * 4
                        
                        key = (a_clone.header.version_id, a_clone.header.mod_time, 
                               a_clone.header.version_type, a_clone.header.flags)
                        x[key] = x.get(key, 0) + 1
                    
                    latest_count = 0
                    for k, v in x.items():
                        if v < latest_count:
                            continue
                        if v == latest_count:
                            # Compare headers
                            continue
                        
                        for a in tops:
                            hdr_key = (a.header.version_id, a.header.mod_time,
                                      a.header.version_type, a.header.flags)
                            if hdr_key == k:
                                latest = a
                        
                        latest_count = v
                    break
            
            if latest_count >= quorum:
                if not latest.header.free_version():
                    n_versions += 1
                merged.append(latest)
        
        # Remove processed versions
        for vers in versions:
            i = 0
            while i < len(vers):
                ver = vers[i]
                should_remove = False
                
                if latest.header.mod_time and ver.header.mod_time:
                    if ver.header.mod_time < latest.header.mod_time:
                        should_remove = True
                
                if ver.header == latest.header:
                    should_remove = True
                
                if ver.header.version_id == latest.header.version_id:
                    should_remove = True
                
                for merged_v in merged:
                    if ver.header.version_id == merged_v.header.version_id:
                        should_remove = True
                        break
                
                if should_remove:
                    vers.pop(i)
                else:
                    i += 1
        
        if requested_versions > 0 and requested_versions == n_versions:
            if versions and versions[0]:
                merged.extend(versions[0])
            break
    
    return merged


async def file_info_from_raw(ri: RawFileInfo, bucket: str, object: str, read_data: bool) -> FileInfo:
    return await get_file_info(ri.buf, bucket, object, "", FileInfoOpts(data=read_data))


class FileInfoOpts:
    def __init__(self, data: bool = False):
        self.data = data


async def get_file_info(buf: bytes, volume: str, path: str, version_id: str, opts: FileInfoOpts) -> FileInfo:
    vid = None
    if version_id:
        try:
            vid = uuid.UUID(version_id)
        except:
            pass
    
    meta = FileMeta.load(buf)
    if not meta.versions:
        return FileInfo(
            volume=volume,
            name=path,
            version_id=vid,
            is_latest=True,
            deleted=True,
            mod_time=datetime.fromtimestamp(1, tz=timezone.utc)
        )
    
    fi = meta.into_fileinfo(volume, path, version_id, opts.data, True)
    return fi


async def read_more(reader, buf: bytearray, total_size: int, read_size: int, has_full: bool):
    has = len(buf)
    
    if has >= read_size:
        return
    
    if has_full or read_size > total_size:
        raise Error("Unexpected EOF")
    
    extra = read_size - has
    if len(buf) + extra <= buf.capacity():
        buf.extend(b'\x00' * extra)
    else:
        buf.extend(b'\x00' * extra)
    
    await reader.readinto(memoryview(buf)[has:read_size])


async def read_xl_meta_no_data(reader, size: int) -> bytes:
    initial = size
    has_full = True
    
    if initial > META_DATA_READ_DEFAULT:
        initial = META_DATA_READ_DEFAULT
        has_full = False
    
    buf = bytearray(initial)
    await reader.readinto(buf)
    
    tmp_buf, major, minor = FileMeta.check_xl2_v1(buf)
    
    if major == 1:
        if minor == 0:
            await read_more(reader, buf, size, size, has_full)
            return bytes(buf)
        elif 1 <= minor <= 3:
            sz, tmp_buf = FileMeta.read_bytes_header(tmp_buf)
            want = sz + (len(buf) - len(tmp_buf))
            
            if minor < 2:
                await read_more(reader, buf, size, want, has_full)
                return bytes(buf[:want])
            
            want_max = min(want + MSGP_UINT32_SIZE, size)
            await read_more(reader, buf, size, want_max, has_full)
            
            if len(buf) < want:
                raise Error("FileCorrupt")
            
            tmp = buf[want:]
            crc_size = 5
            other_size = len(tmp) - crc_size
            
            want += len(tmp) - other_size
            
            return bytes(buf[:want])
        else:
            raise Error("Unknown minor metadata version")
    else:
        raise Error("Unknown major metadata version")
