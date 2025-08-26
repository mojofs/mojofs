import asyncio

async def write_all(writer, buf: bytes) -> int:
    """
    将buf中的所有字节写入writer，返回写入的总字节数。
    writer: 必须实现async def write(data: bytes) -> int
    """
    total = 0
    length = len(buf)
    while total < length:
        n = await writer.write(buf[total:])
        if n == 0:
            break
        total += n
    return total

async def read_full(reader, buf: bytearray) -> int:
    """
    读取正好len(buf)字节到buf中，如果提前EOF则抛出异常。
    reader: 必须实现async def read(n: int) -> bytes
    返回实际读取的字节数。
    """
    total = 0
    length = len(buf)
    view = memoryview(buf)
    while total < length:
        chunk = await reader.read(length - total)
        if not chunk:
            if total == 0:
                raise EOFError("early EOF")
            else:
                return total
        n = len(chunk)
        view[total:total+n] = chunk
        total += n
    return total

def put_uvarint(buf: bytearray, x: int) -> int:
    """
    将uvarint编码写入buf，返回写入的字节数。
    buf: 必须有足够空间
    """
    i = 0
    while x >= 0x80:
        buf[i] = (x & 0xFF) | 0x80
        x >>= 7
        i += 1
    buf[i] = x & 0xFF
    return i + 1

def put_uvarint_len(x: int) -> int:
    """
    返回uvarint编码x所需的字节数。
    """
    i = 0
    while x >= 0x80:
        x >>= 7
        i += 1
    return i + 1

def uvarint(buf: bytes) -> tuple[int, int]:
    """
    从buf解码uvarint，返回(值, 读取的字节数)。
    如果buf太短，返回(0, 0)。
    如果溢出，返回(0, -n)。
    """
    x = 0
    s = 0
    for i, b in enumerate(buf):
        if i == 10:
            return (0, -(i + 1))
        if b < 0x80:
            if i == 9 and b > 1:
                return (0, -(i + 1))
            return (x | (b << s), i + 1)
        x |= ((b & 0x7F) << s)
        s += 7
    return (0, 0)

import unittest

class DummyAsyncWriter:
    def __init__(self):
        self.data = bytearray()
        self.max_write = None  # None表示不限制

    async def write(self, buf: bytes) -> int:
        if self.max_write is not None:
            n = min(len(buf), self.max_write)
        else:
            n = len(buf)
        self.data.extend(buf[:n])
        await asyncio.sleep(0)  # 模拟异步
        return n

class DummyAsyncReader:
    def __init__(self, data: bytes):
        self.data = bytearray(data)
        self.pos = 0

    async def read(self, n: int) -> bytes:
        if self.pos >= len(self.data):
            await asyncio.sleep(0)
            return b''
        end = min(self.pos + n, len(self.data))
        chunk = self.data[self.pos:end]
        self.pos = end
        await asyncio.sleep(0)
        return bytes(chunk)

class TestIOUtils(unittest.IsolatedAsyncioTestCase):
    async def test_write_all_basic(self):
        data = b"hello world!"
        writer = DummyAsyncWriter()
        n = await write_all(writer, data)
        self.assertEqual(n, len(data))
        self.assertEqual(writer.data, data)

    async def test_write_all_partial(self):
        data = b"abcdefghijklmnopqrstuvwxyz"
        writer = DummyAsyncWriter()
        writer.max_write = 5
        n = await write_all(writer, data)
        self.assertEqual(n, len(data))
        self.assertEqual(writer.data, data)

    async def test_read_full_exact(self):
        data = b"channel async callback test data!"
        reader = DummyAsyncReader(data)
        buf = bytearray(len(data))
        n = await read_full(reader, buf)
        self.assertEqual(n, len(data))
        self.assertEqual(buf, data)

    async def test_read_full_short(self):
        data = b"abc"
        reader = DummyAsyncReader(data)
        buf = bytearray(6)
        n = await read_full(reader, buf)
        self.assertEqual(n, 3)
        self.assertEqual(buf[:n], data)

    async def test_read_full_1m(self):
        size = 1024 * 1024
        data = bytes([42]) * size
        reader = DummyAsyncReader(data)
        buf = bytearray(size // 3)
        n = await read_full(reader, buf)
        self.assertEqual(buf, data[:size // 3])

class TestUVarint(unittest.TestCase):
    def test_put_uvarint_and_uvarint_zero(self):
        buf = bytearray(16)
        n = put_uvarint(buf, 0)
        decoded, m = uvarint(buf[:n])
        self.assertEqual(decoded, 0)
        self.assertEqual(m, n)

    def test_put_uvarint_and_uvarint_max(self):
        import sys
        maxval = (1 << 64) - 1
        buf = bytearray(16)
        n = put_uvarint(buf, maxval)
        decoded, m = uvarint(buf[:n])
        self.assertEqual(decoded, maxval)
        self.assertEqual(m, n)

    def test_put_uvarint_and_uvarint_various(self):
        vals = [1, 127, 128, 255, 300, 16384, (1 << 32) - 1]
        for v in vals:
            buf = bytearray(16)
            n = put_uvarint(buf, v)
            decoded, m = uvarint(buf[:n])
            self.assertEqual(decoded, v, f"decode mismatch for {v}")
            self.assertEqual(m, n, f"length mismatch for {v}")

    def test_uvarint_incomplete(self):
        buf = bytes([0x80, 0x80, 0x80])
        v, n = uvarint(buf)
        self.assertEqual(v, 0)
        self.assertEqual(n, 0)

    def test_uvarint_overflow_case(self):
        buf = bytes([0xFF] * 11)
        v, n = uvarint(buf)
        self.assertEqual(v, 0)
        self.assertTrue(n < 0)

if __name__ == "__main__":
    unittest.main()