import uuid
from mojofs.utils import HashAlgorithm
from enum import Enum

class BitrotReader:
    """BitrotReader reads (hash+data) blocks from a reader and verifies hash integrity."""

    def __init__(self, inner, shard_size: int, algo: HashAlgorithm):
        self.inner = inner
        self.hash_algo = algo
        self.shard_size = shard_size
        self.hash_buf = bytearray(algo.size())
        self.id = uuid.uuid4()

    def read(self, out: bytearray) -> int:
        """Read a single (hash+data) block, verify hash, and return the number of bytes read into `out`.
        Returns an error if hash verification fails or data exceeds shard_size.
        """
        if len(out) > self.shard_size:
            raise ValueError(f"data size {len(out)} exceeds shard size {self.shard_size}")

        hash_size = self.hash_algo.size()

        # Read hash
        if hash_size > 0:
            read_bytes = self.inner.read(hash_size)
            if not read_bytes:
                raise IOError("bitrot reader read hash error: expected {} bytes, got 0".format(hash_size))
            if len(read_bytes) != hash_size:
                raise IOError("bitrot reader read hash error: expected {} bytes, got {}".format(hash_size, len(read_bytes)))
            self.hash_buf[:hash_size] = read_bytes

        # Read data
        data_len = 0
        while data_len < len(out):
            read_bytes = self.inner.read(len(out) - data_len)
            if not read_bytes:
                break
            out[data_len:data_len + len(read_bytes)] = read_bytes
            data_len += len(read_bytes)

        if hash_size > 0:
            actual_hash = self.hash_algo.hash_encode(out[:data_len])
            if actual_hash != bytes(self.hash_buf[:hash_size]):
                print(f"bitrot reader hash mismatch, id={self.id} data_len={data_len}, out_len={len(out)}")
                raise IOError("bitrot hash mismatch")

        return data_len


class BitrotWriter:
    """BitrotWriter writes (hash+data) blocks to a writer."""

    def __init__(self, inner, shard_size: int, algo: HashAlgorithm):
        self.inner = inner
        self.hash_algo = algo
        self.shard_size = shard_size
        self.buf = bytearray()
        self.finished = False

    def write(self, buf: bytes) -> int:
        """Write a (hash+data) block. Returns the number of data bytes written.
        Returns an error if called after a short write or if data exceeds shard_size.
        """
        if not buf:
            return 0

        if self.finished:
            raise ValueError("bitrot writer already finished")

        if len(buf) > self.shard_size:
            raise ValueError(f"data size {len(buf)} exceeds shard size {self.shard_size}")

        if len(buf) < self.shard_size:
            self.finished = True

        hash_algo = self.hash_algo

        if hash_algo.size() > 0:
            hash_value = hash_algo.hash_encode(buf)
            self.buf.extend(hash_value)

        self.buf.extend(buf)
        self.inner.write(bytes(self.buf))
        self.buf.clear()

        return len(buf)

    def shutdown(self):
        pass  # Placeholder for shutdown


def bitrot_shard_file_size(size: int, shard_size: int, algo: HashAlgorithm) -> int:
    if algo != HashAlgorithm.SHA256:  # Assuming SHA256 is similar to HighwayHash256S
        return size
    return (size + shard_size - 1) // shard_size * (algo.size() + shard_size)


def bitrot_verify(r, want_size: int, part_size: int, algo: HashAlgorithm, shard_size: int):
    hash_buf = bytearray(algo.size())
    left = want_size
    hash_size = algo.size()

    if left != bitrot_shard_file_size(part_size, shard_size, algo):
        raise IOError("bitrot shard file size mismatch")

    while left > 0:
        n = r.read(hash_size)
        if not n or len(n) != hash_size:
            raise IOError("read hash failed")
        hash_buf[:hash_size] = n
        left -= hash_size

        if left < shard_size:
            shard_size = left

        buf = bytearray(shard_size)
        read = r.read(shard_size)
        if not read or len(read) != shard_size:
            raise IOError("read data failed")
        buf[:len(read)] = read
        
        actual_hash = algo.hash_encode(bytes(buf))
        if actual_hash != bytes(hash_buf[:hash_size]):
            raise IOError("bitrot hash mismatch")

        left -= len(read)

class CustomWriter:
    """Custom writer enum that supports inline buffer storage"""

    def __init__(self, inline_buffer=None, other=None):
        if inline_buffer is not None and other is not None:
            raise ValueError("Cannot specify both inline_buffer and other")

        self.inline_buffer = inline_buffer
        self.other = other

    def read(self, size: int) -> bytes:
        """Read data from the inline buffer, if available."""
        if self.inline_buffer is not None:
            data = bytes(self.inline_buffer[:size])
            self.inline_buffer = self.inline_buffer[size:]
            return data
        elif self.other is not None:
            return self.other.read(size)
        else:
            raise ValueError("No reader specified")

    @staticmethod
    def new_inline_buffer():
        """Create a new inline buffer writer"""
        return CustomWriter(inline_buffer=bytearray())

    @staticmethod
    def new_tokio_writer(writer):
        """Create a new disk writer from any writer implementation"""
        return CustomWriter(other=writer)

    def get_inline_data(self):
        """Get the inline buffer data if this is an inline buffer writer"""
        return bytes(self.inline_buffer) if self.inline_buffer is not None else None

    def into_inline_data(self):
        """Extract the inline buffer data, consuming the writer"""
        return bytes(self.inline_buffer) if self.inline_buffer is not None else None

    def write(self, buf):
        if self.inline_buffer is not None:
            self.inline_buffer.extend(buf)
            return len(buf)
        elif self.other is not None:
            return self.other.write(buf)
        else:
            raise ValueError("No writer specified")

    def flush(self):
        if self.other is not None and hasattr(self.other, "flush"):
            self.other.flush()

    def shutdown(self):
        if self.other is not None and hasattr(self.other, "close"):
            self.other.close()


class WriterType(Enum):
    INLINE_BUFFER = 1
    OTHER = 2


class BitrotWriterWrapper:
    """Wrapper around BitrotWriter that uses our custom writer"""

    def __init__(self, writer: CustomWriter, shard_size: int, checksum_algo: HashAlgorithm):
        self.bitrot_writer = BitrotWriter(writer, shard_size, checksum_algo)
        self.writer_type = (
            WriterType.INLINE_BUFFER
            if writer.inline_buffer is not None
            else WriterType.OTHER
        )

    def write(self, buf: bytes) -> int:
        """Write data to the bitrot writer"""
        return self.bitrot_writer.write(buf)

    def shutdown(self):
        self.bitrot_writer.shutdown()

    def into_inline_data(self):
        """Extract the inline buffer data, consuming the wrapper"""
        if self.writer_type == WriterType.INLINE_BUFFER:
            writer = self.bitrot_writer.inner
            return writer.into_inline_data()
        return None

    def __repr__(self):
        writer_type_str = (
            "InlineBuffer" if self.writer_type == WriterType.INLINE_BUFFER else "Other"
        )
        return f"BitrotWriterWrapper(writer_type={writer_type_str})"


# Example Usage (Tests)
if __name__ == '__main__':
    # Test 1: Read/Write OK
    data = b"hello world! this is a test shard."
    data_size = len(data)
    shard_size = 8

    buf = bytearray()
    writer = CustomWriter.new_inline_buffer()
    bitrot_writer = BitrotWriter(writer, shard_size, HashAlgorithm.SHA256)

    n = 0
    for chunk in [data[i:i + shard_size] for i in range(0, len(data), shard_size)]:
        n += bitrot_writer.write(chunk)
    assert n == len(data)

    reader_data = writer.get_inline_data()
    reader = CustomWriter.new_inline_buffer()
    reader.write(reader_data)
    bitrot_reader = BitrotReader(reader, shard_size, HashAlgorithm.SHA256)

    out = bytearray()
    n = 0
    while n < data_size:
        buf = bytearray(shard_size)
        m = bitrot_reader.read(buf)
        assert buf[:m] == data[n:n + m]
        out.extend(buf[:m])
        n += m

    assert n == data_size
    assert bytes(out) == data

    # Test 2: Hash Mismatch
    data = b"test data for bitrot"
    data_size = len(data)
    shard_size = 8

    writer = CustomWriter.new_inline_buffer()
    bitrot_writer = BitrotWriter(writer, shard_size, HashAlgorithm.SHA256)

    for chunk in [data[i:i + shard_size] for i in range(0, len(data), shard_size)]:
        bitrot_writer.write(chunk)

    written = writer.get_inline_data()
    written_list = list(written)
    pos = len(written) - 1
    written_list[pos] ^= 0xFF
    written = bytes(written_list)

    reader = CustomWriter.new_inline_buffer()
    reader.write(written)
    bitrot_reader = BitrotReader(reader, shard_size, HashAlgorithm.SHA256)

    count = (data_size + shard_size - 1) // shard_size
    idx = 0
    n = 0
    try:
        while n < data_size:
            buf = bytearray(shard_size)
            m = bitrot_reader.read(buf)
            if idx == count - 1:
                assert False, "Expected an error"
            assert buf[:m] == data[n:n + m]
            n += m
            idx += 1
    except IOError as e:
        assert "bitrot hash mismatch" in str(e)
    else:
        assert False, "Expected an IOError"

    # Test 3: None Hash
    data = b"bitrot none hash test data!"
    data_size = len(data)
    shard_size = 8

    writer = CustomWriter.new_inline_buffer()
    bitrot_writer = BitrotWriter(writer, shard_size, HashAlgorithm.NONE)

    n = 0
    for chunk in [data[i:i + shard_size] for i in range(0, len(data), shard_size)]:
        n += bitrot_writer.write(chunk)
    assert n == len(data)

    reader_data = writer.get_inline_data()
    reader = CustomWriter.new_inline_buffer()
    reader.write(reader_data)
    bitrot_reader = BitrotReader(reader, shard_size, HashAlgorithm.NONE)

    out = bytearray()
    n = 0
    while n < data_size:
        buf = bytearray(shard_size)
        m = bitrot_reader.read(buf)
        assert buf[:m] == data[n:n + m]
        out.extend(buf[:m])
        n += m

    assert n == data_size
    assert bytes(out) == data
    
    # Test 4: Empty Data
    data = b""
    data_size = len(data)
    shard_size = 8

    writer = CustomWriter.new_inline_buffer()
    bitrot_writer = BitrotWriter(writer, shard_size, HashAlgorithm.SHA256)

    n = 0
    for chunk in [data[i:i + shard_size] for i in range(0, len(data), shard_size)]:
        n += bitrot_writer.write(chunk)
    assert n == len(data)

    reader_data = writer.get_inline_data()
    reader = CustomWriter.new_inline_buffer()
    reader.write(reader_data)
    bitrot_reader = BitrotReader(reader, shard_size, HashAlgorithm.SHA256)

    out = bytearray()
    n = 0
    while n < data_size:
        buf = bytearray(shard_size)
        m = bitrot_reader.read(buf)
        out.extend(buf[:m])
        n += m

    assert n == data_size
    assert bytes(out) == data