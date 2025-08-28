import asyncio
import struct
from typing import List, Optional, Any, Callable, Tuple, Dict, Union
from dataclasses import dataclass, field
import time
import threading
from mojofs.filemeta.error import Error
from mojofs.filemeta import FileInfo, FileInfoVersions, FileMeta, FileMetaShallowVersion, VersionType, merge_file_meta_versions

SLASH_SEPARATOR = "/"

@dataclass
class MetadataResolutionParams:
    dir_quorum: int = 0
    obj_quorum: int = 0
    requested_versions: int = 0
    bucket: str = ""
    strict: bool = False
    candidates: List[List[Any]] = field(default_factory=list)  # List[List[FileMetaShallowVersion]]

@dataclass
class MetaCacheEntry:
    name: str
    metadata: bytes
    cached: Optional[Any] = None  # FileMeta
    reusable: bool = False

    def marshal_msg(self) -> bytes:
        # 使用msgpack序列化
        import msgpack
        return msgpack.packb([True, self.name, self.metadata])

    def is_dir(self) -> bool:
        return not self.metadata and self.name.endswith('/')

    def is_in_dir(self, dir: str, separator: str) -> bool:
        if not dir:
            idx = self.name.find(separator)
            return idx == -1 or idx == len(self.name) - len(separator)
        ext = self.name[len(dir):] if self.name.startswith(dir) else self.name
        if len(ext) != len(self.name):
            idx = ext.find(separator)
            return idx == -1 or idx == len(ext) - len(separator)
        return False

    def is_object(self) -> bool:
        return bool(self.metadata)

    def is_object_dir(self) -> bool:
        return bool(self.metadata) and self.name.endswith(SLASH_SEPARATOR)

    def is_latest_delete_marker(self) -> bool:
        # 需要FileMeta和VersionType的实现
        if self.cached is not None:
            if not getattr(self.cached, "versions", []):
                return True
            return getattr(self.cached.versions[0].header, "version_type", None) == "Delete"
        # 省略FileMeta相关的复杂逻辑
        return False

    def to_fileinfo(self, bucket: str) -> Any:
        # 需要FileInfo和FileMeta的实现
        if self.is_dir():
            return FileInfo(volume=bucket, name=self.name)
        if self.cached is not None:
            fm = self.cached
            if not getattr(fm, "versions", []):
                return FileInfo(volume=bucket, name=self.name, deleted=True, is_latest=True, mod_time=0)
            return fm.into_fileinfo(bucket, self.name, "", False, False)
        fm = FileMeta()
        fm.unmarshal_msg(self.metadata)
        return fm.into_fileinfo(bucket, self.name, "", False, False)

    def file_info_versions(self, bucket: str) -> Any:
        if self.is_dir():
            return FileInfoVersions(
                volume=bucket,
                name=self.name,
                versions=[FileInfo(volume=bucket, name=self.name)]
            )
        fm = FileMeta()
        fm.unmarshal_msg(self.metadata)
        return fm.into_file_info_versions(bucket, self.name, False)

    def matches(self, other: Optional['MetaCacheEntry'], strict: bool) -> Tuple[Optional['MetaCacheEntry'], bool]:
        if other is None:
            return None, False
        if self.name != other.name:
            if self.name < other.name:
                return self, False
            return other, False
        if other.is_dir() or self.is_dir():
            if self.is_dir():
                return self, other.is_dir() == self.is_dir()
            return other, other.is_dir() == self.is_dir()
        # 省略复杂的版本比较逻辑
        return self, True

    def xl_meta(self) -> Any:
        if self.is_dir():
            raise FileNotFoundError()
        if self.cached is not None:
            return self.cached
        if not self.metadata:
            raise FileNotFoundError()
        meta = FileMeta.load(self.metadata)
        self.cached = meta
        return meta

@dataclass
class MetaCacheEntries:
    entries: List[Optional[MetaCacheEntry]] = field(default_factory=list)

    def as_ref(self) -> List[Optional[MetaCacheEntry]]:
        return self.entries

    def resolve(self, params: MetadataResolutionParams) -> Optional[MetaCacheEntry]:
        if not self.entries:
            print("decommission_pool: entries resolve empty")
            return None
        dir_exists = 0
        selected = None
        params.candidates.clear()
        objs_agree = 0
        objs_valid = 0
        for entry in filter(None, self.entries):
            entry = entry  # type: MetaCacheEntry
            print(f"decommission_pool: entries resolve entry {entry.name}")
            if not entry.name:
                continue
            if entry.is_dir():
                dir_exists += 1
                selected = entry
                print(f"decommission_pool: entries resolve entry dir {entry.name}")
                continue
            try:
                xl = entry.xl_meta()
            except Exception as e:
                print(f"decommission_pool: entries resolve entry xl_meta {e}")
                continue
            objs_valid += 1
            params.candidates.append(getattr(xl, "versions", []))
            if selected is None:
                selected = entry
                objs_agree = 1
                print(f"decommission_pool: entries resolve entry selected {entry.name}")
                continue
            prefer, agree = entry.matches(selected, params.strict)
            if agree:
                selected = prefer
                objs_agree += 1
                print(f"decommission_pool: entries resolve entry prefer {entry.name}")
                continue
        if selected is None:
            print("decommission_pool: entries resolve entry no selected")
            return None
        if selected.is_dir() and dir_exists >= params.dir_quorum:
            print(f"decommission_pool: entries resolve entry dir selected {selected.name}")
            return selected
        if objs_valid < params.obj_quorum:
            print(f"decommission_pool: entries resolve entry not enough objects {objs_valid} < {params.obj_quorum}")
            return None
        if objs_agree == objs_valid:
            print(f"decommission_pool: entries resolve entry all agree {objs_agree} == {objs_valid}")
            return selected
        cached = selected.cached
        if cached is None:
            print("decommission_pool: entries resolve entry no cached")
            return None
        versions = merge_file_meta_versions(params.obj_quorum, params.strict, params.requested_versions, params.candidates)
        if not versions:
            print("decommission_pool: entries resolve entry no versions")
            return None
        try:
            metadata = cached.marshal_msg()
        except Exception as e:
            print(f"decommission_pool: entries resolve entry marshal_msg {e}")
            return None
        new_selected = MetaCacheEntry(
            name=selected.name,
            cached=FileMeta(meta_ver=cached.meta_ver, versions=versions),
            reusable=True,
            metadata=metadata
        )
        print(f"decommission_pool: entries resolve entry selected {new_selected.name}")
        return new_selected

    def first_found(self) -> Tuple[Optional[MetaCacheEntry], int]:
        for x in self.entries:
            if x is not None:
                return x, len(self.entries)
        return None, len(self.entries)

@dataclass
class MetaCacheEntriesSortedResult:
    entries: Optional['MetaCacheEntriesSorted'] = None
    err: Optional[Exception] = None

@dataclass
class MetaCacheEntriesSorted:
    o: MetaCacheEntries = field(default_factory=MetaCacheEntries)
    list_id: Optional[str] = None
    reuse: bool = False
    last_skipped_entry: Optional[str] = None

    def entries_list(self) -> List[MetaCacheEntry]:
        return [e for e in self.o.entries if e is not None]

    def forward_past(self, marker: Optional[str]):
        if marker:
            idx = next((i for i, v in enumerate(self.o.entries) if v and v.name > marker), None)
            if idx is not None:
                self.o.entries = self.o.entries[idx:]

METACACHE_STREAM_VERSION = 2

class MetacacheWriter:
    def __init__(self, wr):
        self.wr = wr
        self.created = False
        self.buf = bytearray()

    async def flush(self):
        if self.buf:
            await self.wr.write(self.buf)
            self.buf.clear()

    async def init(self):
        if not self.created:
            self.buf += struct.pack("B", METACACHE_STREAM_VERSION)
            await self.flush()
            self.created = True

    async def write(self, objs: List[MetaCacheEntry]):
        if not objs:
            return
        await self.init()
        for obj in objs:
            if not obj.name:
                raise Exception("metacacheWriter: no name")
            await self.write_obj(obj)

    async def write_obj(self, obj: MetaCacheEntry):
        await self.init()
        import msgpack
        self.buf += msgpack.packb([True, obj.name, obj.metadata])
        await self.flush()

    async def close(self):
        import msgpack
        self.buf += msgpack.packb([False])
        await self.flush()

class MetacacheReader:
    def __init__(self, rd):
        self.rd = rd
        self.init = False
        self.err = None
        self.buf = bytearray()
        self.offset = 0
        self.current = None

    async def read_more(self, read_size: int) -> bytes:
        # 假设rd有read方法
        data = await self.rd.read(read_size)
        self.buf += data
        self.offset += len(data)
        return data

    def reset(self):
        self.buf.clear()
        self.offset = 0

    async def check_init(self):
        if not self.init:
            ver = (await self.read_more(1))[0]
            if ver not in (1, 2):
                self.err = Exception("invalid version")
            self.init = True

    async def skip(self, size: int):
        await self.check_init()
        if self.err:
            raise self.err
        n = size
        if self.current is not None:
            n -= 1
            self.current = None
        import msgpack
        while n > 0:
            # 读取一个对象
            obj = await self.peek()
            if obj is None:
                return
            n -= 1

    async def peek(self) -> Optional[MetaCacheEntry]:
        await self.check_init()
        if self.err:
            raise self.err
        import msgpack
        # 读取一个对象
        # 假设rd有read方法
        # 这里简化为一次性读取
        data = await self.rd.read(4096)
        if not data:
            return None
        unpacker = msgpack.Unpacker()
        unpacker.feed(data)
        try:
            arr = next(unpacker)
        except StopIteration:
            return None
        if not arr or arr[0] is False:
            return None
        name = arr[1]
        metadata = arr[2]
        entry = MetaCacheEntry(name=name, metadata=metadata, cached=None, reusable=False)
        self.current = entry
        return entry

    async def read_all(self) -> List[MetaCacheEntry]:
        ret = []
        while True:
            entry = await self.peek()
            if entry is not None:
                ret.append(entry)
                continue
            break
        return ret

class Opts:
    def __init__(self, return_last_good: bool = False, no_wait: bool = False):
        self.return_last_good = return_last_good
        self.no_wait = no_wait

class Cache:
    def __init__(self, update_fn: Callable[[], Any], ttl: float, opts: Opts):
        self.update_fn = update_fn
        self.ttl = ttl
        self.opts = opts
        self.val = None
        self.last_update_ms = 0
        self.updating = threading.Lock()

    async def get(self):
        now = time.time()
        if self.val is not None and now - self.last_update_ms < self.ttl:
            return self.val
        if self.opts.no_wait and self.val is not None and now - self.last_update_ms < self.ttl * 2:
            if self.updating.acquire(blocking=False):
                asyncio.create_task(self.update())
            return self.val
        with self.updating:
            if self.val is not None and now - self.last_update_ms < self.ttl:
                return self.val
            await self.update()
            return self.val

    async def update(self):
        try:
            val = await self.update_fn()
            self.val = val
            self.last_update_ms = time.time()
        except Exception as err:
            if self.opts.return_last_good and self.val is not None:
                return
            raise err

# 测试代码
if __name__ == "__main__":
    import io

    class DummyAsyncWriter:
        def __init__(self):
            self.data = bytearray()
        async def write(self, b):
            self.data += b

    class DummyAsyncReader:
        def __init__(self, data):
            self.data = data
            self.offset = 0
        async def read(self, n):
            if self.offset >= len(self.data):
                return b''
            d = self.data[self.offset:self.offset+n]
            self.offset += n
            return d

    async def test_writer():
        f = DummyAsyncWriter()
        w = MetacacheWriter(f)
        objs = []
        for i in range(10):
            info = MetaCacheEntry(
                name=f"item{i}",
                metadata=bytes([0, 10]),
                cached=None,
                reusable=False
            )
            objs.append(info)
        await w.write(objs)
        await w.close()
        data = f.data
        nf = DummyAsyncReader(data)
        r = MetacacheReader(nf)
        nobjs = await r.read_all()
        assert len(nobjs) == 10

    asyncio.run(test_writer())