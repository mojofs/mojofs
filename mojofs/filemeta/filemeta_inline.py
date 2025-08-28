import io
import msgpack
import uuid
from mojofs.filemeta.error import Error

from typing import List, Optional, Dict, Union


class InlineData:
    INLINE_DATA_VER: int = 1

    def __init__(self, data: bytes = b""):
        self.data: bytearray = bytearray(data)

    def new(self) -> "InlineData":
        return InlineData()

    def update(self, buf: bytes):
        self.data = bytearray(buf)

    def as_slice(self) -> bytes:
        return bytes(self.data)

    def as_bytes(self) -> bytes:
        return bytes(self.data)

    def version_ok(self) -> bool:
        if not self.data:
            return True
        return 0 < self.data[0] <= InlineData.INLINE_DATA_VER

    def after_version(self) -> bytes:
        if not self.data:
            return bytes(self.data)
        else:
            return bytes(self.data[1:])

    def find(self, key: str) -> Optional[bytes]:
        if not self.data or not self.version_ok():
            return None

        buf = self.after_version()
        unpacker = msgpack.Unpacker(io.BytesIO(buf), raw=False)

        try:
            data = unpacker.unpack()
            if isinstance(data, dict):
                return data.get(key)
        except:
            pass

        return None

    def validate(self) -> None:
        if not self.data:
            return

        buf = self.after_version()
        unpacker = msgpack.Unpacker(io.BytesIO(buf), raw=False)

        try:
            data = unpacker.unpack()
            if not isinstance(data, dict):
                raise Error("InlineData not a map")
            
            for key in data.keys():
                if not key:
                    raise Error("InlineData key empty")
        except msgpack.exceptions.UnpackException:
            raise Error("InlineData invalid msgpack")

    def replace(self, key: str, value: bytes) -> None:
        if not self.after_version():
            data = {key: value}
            self.serialize_dict(data)
            return

        buf = self.after_version()
        unpacker = msgpack.Unpacker(io.BytesIO(buf), raw=False)

        try:
            data = unpacker.unpack()
            if isinstance(data, dict):
                data[key] = value
                self.serialize_dict(data)
            else:
                # 如果不是字典，创建新的
                data = {key: value}
                self.serialize_dict(data)
        except:
            # 解析失败，创建新的
            data = {key: value}
            self.serialize_dict(data)

    def remove(self, remove_keys: List[uuid.UUID]) -> bool:
        buf = self.after_version()
        if not buf:
            return False

        unpacker = msgpack.Unpacker(io.BytesIO(buf), raw=False)

        try:
            data = unpacker.unpack()
            if not isinstance(data, dict):
                return False

            found = False
            for key in remove_keys:
                key_str = str(key)
                if key_str in data:
                    del data[key_str]
                    found = True

            if not found:
                return False

            if not data:
                self.data = bytearray()
            else:
                self.serialize_dict(data)

            return True
        except:
            return False

    def serialize(self, keys: List[str], values: List[bytes]) -> None:
        assert len(keys) == len(values), "InlineData serialize: keys/values not match"

        if not keys:
            self.data = bytearray()
            return

        data = {}
        for i in range(len(keys)):
            data[keys[i]] = values[i]

        self.serialize_dict(data)

    def serialize_dict(self, data: Dict[str, bytes]) -> None:
        if not data:
            self.data = bytearray()
            return

        wr = bytearray()
        wr.append(InlineData.INLINE_DATA_VER)
        
        packed = msgpack.packb(data, use_bin_type=True)
        wr.extend(packed)

        self.data = wr