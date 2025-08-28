
from enum import Enum, auto
from mojofs.ecstore.disk.error import DiskError

# 定义ErrorKind枚举
class ErrorKind(Enum):
    NotFound = auto()
    PermissionDenied = auto()
    IsADirectory = auto()
    NotADirectory = auto()
    DirectoryNotEmpty = auto()
    UnexpectedEof = auto()
    TooManyLinks = auto()
    InvalidInput = auto()
    InvalidData = auto()
    StorageFull = auto()
    Other = auto()
    Interrupted = auto()

# 自定义IOError，带有kind和可选的disk_error
class IOError(Exception):
    def __init__(self, kind, disk_error=None):
        self._kind = kind
        self._disk_error = disk_error
        super().__init__(str(kind))

    def kind(self):
        return self._kind

    def downcast_disk_error(self):
        if self._disk_error is not None:
            return self._disk_error
        else:
            raise TypeError("Not a DiskError")

    def __eq__(self, other):
        if isinstance(other, IOError):
            return self._kind == other._kind and self._disk_error == other._disk_error
        return False

# DiskError转IOError
def disk_error_to_ioerror(disk_error):
    return IOError(ErrorKind.Other, disk_error)

# to_file_error函数
def to_file_error(io_err):
    kind = io_err.kind()
    if kind == ErrorKind.NotFound:
        return disk_error_to_ioerror(DiskError.FileNotFound)
    elif kind == ErrorKind.PermissionDenied:
        return disk_error_to_ioerror(DiskError.FileAccessDenied)
    elif kind == ErrorKind.IsADirectory:
        return disk_error_to_ioerror(DiskError.IsNotRegular)
    elif kind == ErrorKind.NotADirectory:
        return disk_error_to_ioerror(DiskError.FileAccessDenied)
    elif kind == ErrorKind.DirectoryNotEmpty:
        return disk_error_to_ioerror(DiskError.FileAccessDenied)
    elif kind == ErrorKind.UnexpectedEof:
        return disk_error_to_ioerror(DiskError.FaultyDisk)
    elif kind == ErrorKind.TooManyLinks:
        return disk_error_to_ioerror(DiskError.TooManyOpenFiles)
    elif kind == ErrorKind.InvalidInput:
        return disk_error_to_ioerror(DiskError.FileNotFound)
    elif kind == ErrorKind.InvalidData:
        return disk_error_to_ioerror(DiskError.FileCorrupt)
    elif kind == ErrorKind.StorageFull:
        return disk_error_to_ioerror(DiskError.DiskFull)
    else:
        return io_err

# to_volume_error函数
def to_volume_error(io_err):
    kind = io_err.kind()
    if kind == ErrorKind.NotFound:
        return disk_error_to_ioerror(DiskError.VolumeNotFound)
    elif kind == ErrorKind.PermissionDenied:
        return disk_error_to_ioerror(DiskError.DiskAccessDenied)
    elif kind == ErrorKind.DirectoryNotEmpty:
        return disk_error_to_ioerror(DiskError.VolumeNotEmpty)
    elif kind == ErrorKind.NotADirectory:
        return disk_error_to_ioerror(DiskError.IsNotRegular)
    elif kind == ErrorKind.Other:
        try:
            err = io_err.downcast_disk_error()
            if err == DiskError.FileNotFound:
                return disk_error_to_ioerror(DiskError.VolumeNotFound)
            elif err == DiskError.FileAccessDenied:
                return disk_error_to_ioerror(DiskError.DiskAccessDenied)
            else:
                return disk_error_to_ioerror(err)
        except TypeError:
            return to_file_error(io_err)
    else:
        return to_file_error(io_err)

# to_disk_error函数
def to_disk_error(io_err):
    kind = io_err.kind()
    if kind == ErrorKind.NotFound:
        return disk_error_to_ioerror(DiskError.DiskNotFound)
    elif kind == ErrorKind.PermissionDenied:
        return disk_error_to_ioerror(DiskError.DiskAccessDenied)
    elif kind == ErrorKind.Other:
        try:
            err = io_err.downcast_disk_error()
            if err == DiskError.FileNotFound:
                return disk_error_to_ioerror(DiskError.DiskNotFound)
            elif err == DiskError.VolumeNotFound:
                return disk_error_to_ioerror(DiskError.DiskNotFound)
            elif err == DiskError.FileAccessDenied:
                return disk_error_to_ioerror(DiskError.DiskAccessDenied)
            elif err == DiskError.VolumeAccessDenied:
                return disk_error_to_ioerror(DiskError.DiskAccessDenied)
            else:
                return disk_error_to_ioerror(err)
        except TypeError:
            return to_volume_error(io_err)
    else:
        return to_volume_error(io_err)

# to_access_error函数
def to_access_error(io_err, per_err):
    kind = io_err.kind()
    if kind == ErrorKind.PermissionDenied:
        return disk_error_to_ioerror(per_err)
    elif kind == ErrorKind.NotADirectory:
        return disk_error_to_ioerror(per_err)
    elif kind == ErrorKind.NotFound:
        return disk_error_to_ioerror(DiskError.VolumeNotFound)
    elif kind == ErrorKind.UnexpectedEof:
        return disk_error_to_ioerror(DiskError.FaultyDisk)
    elif kind == ErrorKind.Other:
        try:
            err = io_err.downcast_disk_error()
            if err == DiskError.DiskAccessDenied:
                return disk_error_to_ioerror(per_err)
            elif err == DiskError.FileAccessDenied:
                return disk_error_to_ioerror(per_err)
            elif err == DiskError.FileNotFound:
                return disk_error_to_ioerror(DiskError.VolumeNotFound)
            else:
                return disk_error_to_ioerror(err)
        except TypeError:
            return to_volume_error(io_err)
    else:
        return to_volume_error(io_err)

# to_unformatted_disk_error函数
def to_unformatted_disk_error(io_err):
    kind = io_err.kind()
    if kind == ErrorKind.NotFound:
        return disk_error_to_ioerror(DiskError.UnformattedDisk)
    elif kind == ErrorKind.PermissionDenied:
        return disk_error_to_ioerror(DiskError.DiskAccessDenied)
    elif kind == ErrorKind.Other:
        try:
            err = io_err.downcast_disk_error()
            if err == DiskError.FileNotFound:
                return disk_error_to_ioerror(DiskError.UnformattedDisk)
            elif err == DiskError.DiskNotFound:
                return disk_error_to_ioerror(DiskError.UnformattedDisk)
            elif err == DiskError.VolumeNotFound:
                return disk_error_to_ioerror(DiskError.UnformattedDisk)
            elif err == DiskError.FileAccessDenied:
                return disk_error_to_ioerror(DiskError.DiskAccessDenied)
            elif err == DiskError.DiskAccessDenied:
                return disk_error_to_ioerror(DiskError.DiskAccessDenied)
            else:
                return disk_error_to_ioerror(DiskError.CorruptedBackend)
        except TypeError:
            return disk_error_to_ioerror(DiskError.CorruptedBackend)
    else:
        return disk_error_to_ioerror(DiskError.CorruptedBackend)