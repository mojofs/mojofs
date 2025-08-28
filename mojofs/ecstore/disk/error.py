import enum
import typing
import hashlib
import os
from pathlib import Path

class DiskError(Exception):
    """磁盘错误类型定义"""

    class Kind(enum.Enum):
        MaxVersionsExceeded = 0x01
        Unexpected = 0x02
        CorruptedFormat = 0x03
        CorruptedBackend = 0x04
        UnformattedDisk = 0x05
        InconsistentDisk = 0x06
        UnsupportedDisk = 0x07
        DiskFull = 0x08
        DiskNotDir = 0x09
        DiskNotFound = 0x0A
        DiskOngoingReq = 0x0B
        DriveIsRoot = 0x0C
        FaultyRemoteDisk = 0x0D
        FaultyDisk = 0x0E
        DiskAccessDenied = 0x0F
        FileNotFound = 0x10
        FileVersionNotFound = 0x11
        TooManyOpenFiles = 0x12
        FileNameTooLong = 0x13
        VolumeExists = 0x14
        IsNotRegular = 0x15
        PathNotFound = 0x16
        VolumeNotFound = 0x17
        VolumeNotEmpty = 0x18
        VolumeAccessDenied = 0x19
        FileAccessDenied = 0x1A
        FileCorrupt = 0x1B
        BitrotHashAlgoInvalid = 0x1C
        CrossDeviceLink = 0x1D
        LessData = 0x1E
        MoreData = 0x1F
        OutdatedXLMeta = 0x20
        PartMissingOrCorrupt = 0x21
        NoHealRequired = 0x22
        MethodNotAllowed = 0x23
        Io = 0x24
        ErasureWriteQuorum = 0x25
        ErasureReadQuorum = 0x26
        ShortWrite = 0x27

    _messages = {
        Kind.MaxVersionsExceeded: "maximum versions exceeded, please delete few versions to proceed",
        Kind.Unexpected: "unexpected error",
        Kind.CorruptedFormat: "corrupted format",
        Kind.CorruptedBackend: "corrupted backend",
        Kind.UnformattedDisk: "unformatted disk error",
        Kind.InconsistentDisk: "inconsistent drive found",
        Kind.UnsupportedDisk: "drive does not support O_DIRECT",
        Kind.DiskFull: "drive path full",
        Kind.DiskNotDir: "disk not a dir",
        Kind.DiskNotFound: "disk not found",
        Kind.DiskOngoingReq: "drive still did not complete the request",
        Kind.DriveIsRoot: "drive is part of root drive, will not be used",
        Kind.FaultyRemoteDisk: "remote drive is faulty",
        Kind.FaultyDisk: "drive is faulty",
        Kind.DiskAccessDenied: "drive access denied",
        Kind.FileNotFound: "file not found",
        Kind.FileVersionNotFound: "file version not found",
        Kind.TooManyOpenFiles: "too many open files, please increase 'ulimit -n'",
        Kind.FileNameTooLong: "file name too long",
        Kind.VolumeExists: "volume already exists",
        Kind.IsNotRegular: "not of regular file type",
        Kind.PathNotFound: "path not found",
        Kind.VolumeNotFound: "volume not found",
        Kind.VolumeNotEmpty: "volume is not empty",
        Kind.VolumeAccessDenied: "volume access denied",
        Kind.FileAccessDenied: "disk access denied",
        Kind.FileCorrupt: "file is corrupted",
        Kind.ShortWrite: "short write",
        Kind.BitrotHashAlgoInvalid: "bit-rot hash algorithm is invalid",
        Kind.CrossDeviceLink: "Rename across devices not allowed, please fix your backend configuration",
        Kind.LessData: "less data available than what was requested",
        Kind.MoreData: "more data was sent than what was advertised",
        Kind.OutdatedXLMeta: "outdated XL meta",
        Kind.PartMissingOrCorrupt: "part missing or corrupt",
        Kind.NoHealRequired: "No healing is required",
        Kind.MethodNotAllowed: "method not allowed",
        Kind.ErasureWriteQuorum: "erasure write quorum",
        Kind.ErasureReadQuorum: "erasure read quorum",
        Kind.Io: "io error",
    }

    def __init__(self, kind: 'DiskError.Kind', detail: typing.Any = None):
        self.kind = kind
        self.detail = detail
        super().__init__(self._messages.get(kind, "unknown error") + (f": {detail}" if detail else ""))

    @classmethod
    def other(cls, error):
        return cls(cls.Kind.Io, str(error))

    @classmethod
    def is_all_not_found(cls, errs: typing.List[typing.Optional['DiskError']]) -> bool:
        if not errs:
            return False
        for err in errs:
            if err is None:
                return False
            if err.kind not in (cls.Kind.FileNotFound, cls.Kind.FileVersionNotFound):
                return False
        return True

    @classmethod
    def is_err_object_not_found(cls, err: 'DiskError') -> bool:
        return err.kind in (cls.Kind.FileNotFound, cls.Kind.VolumeNotFound)

    @classmethod
    def is_err_version_not_found(cls, err: 'DiskError') -> bool:
        return err.kind == cls.Kind.FileVersionNotFound

    def to_u32(self) -> int:
        return self.kind.value

    @classmethod
    def from_u32(cls, code: int) -> typing.Optional['DiskError']:
        for kind in cls.Kind:
            if kind.value == code:
                return cls(kind)
        return None

    def __eq__(self, other):
        if not isinstance(other, DiskError):
            return False
        if self.kind == self.Kind.Io and other.kind == self.Kind.Io:
            return str(self.detail) == str(other.detail)
        return self.kind == other.kind

    def __hash__(self):
        return hash(self.kind.value)

    def __str__(self):
        msg = self._messages.get(self.kind, "unknown error")
        if self.detail:
            return f"{msg}: {self.detail}"
        return msg

    def clone(self):
        return DiskError(self.kind, self.detail)

# Bitrot 错误类型
class BitrotErrorType(Exception):
    def __init__(self, expected: str, got: str):
        self.expected = expected
        self.got = got
        super().__init__(f"bitrot checksum verification failed: expected={expected}, got={got}")

    def __str__(self):
        return f"bitrot checksum verification failed: expected={self.expected}, got={self.got}"

    def to_disk_error(self):
        return DiskError.other(self)

# 文件访问拒绝带上下文
class FileAccessDeniedWithContext(Exception):
    def __init__(self, path: Path, source: Exception):
        self.path = path
        self.source = source
        super().__init__(f"file access denied for path: {path}")

    def __str__(self):
        return f"file access denied for path: {self.path}"

Error = DiskError