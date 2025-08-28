import hashlib
import binascii

try:
    import blake3
except ImportError:
    blake3 = None

try:
    import highwayhash
except ImportError:
    highwayhash = None

from enum import Enum

# HighwayHash256的固定key，兼容性要求不可更改
HIGHWAY_HASH256_KEY = b'\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00'

class HashAlgorithm(Enum):
    SHA256 = "sha256"
    HighwayHash256 = "highwayhash256"
    HighwayHash256S = "highwayhash256s"
    BLAKE2b512 = "blake2b512"
    Md5 = "md5"
    NONE = "none"

    def hash_encode(self, data: bytes):
        if self == HashAlgorithm.Md5:
            return hashlib.md5(data).digest()
        elif self == HashAlgorithm.SHA256:
            return hashlib.sha256(data).digest()
        elif self == HashAlgorithm.BLAKE2b512:
            if blake3 is not None:
                return blake3.blake3(data).digest()
            else:
                # 兼容性处理，使用hashlib的blake2b，输出32字节
                return hashlib.blake2b(data, digest_size=32).digest()
        elif self == HashAlgorithm.HighwayHash256 or self == HashAlgorithm.HighwayHash256S:
            if highwayhash is not None:
                return highwayhash.highwayhash_256(HIGHWAY_HASH256_KEY, data)
            else:
                raise NotImplementedError("需要安装highwayhash库以支持HighwayHash256")
        elif self == HashAlgorithm.NONE:
            return b''
        else:
            raise ValueError("未知的Hash算法")

    def size(self):
        if self == HashAlgorithm.Md5:
            return 16
        elif self in (HashAlgorithm.SHA256, HashAlgorithm.HighwayHash256, HashAlgorithm.HighwayHash256S, HashAlgorithm.BLAKE2b512):
            return 32
        elif self == HashAlgorithm.NONE:
            return 0
        else:
            raise ValueError("未知的Hash算法")

EMPTY_STRING_SHA256_HASH = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

def sip_hash(key: str, cardinality: int, id_bytes: bytes) -> int:
    """
    SipHash分桶，id_bytes应为16字节
    """
    try:
        from siphashc import siphash
    except ImportError:
        raise ImportError("需要安装siphashc库以支持sip_hash")
    if len(id_bytes) != 16:
        raise ValueError("id必须为16字节")
    # siphashc的key需要16字节
    result = siphash(id_bytes, key.encode('utf-8'))
    return result % cardinality

def crc_hash(key: str, cardinality: int) -> int:
    """
    CRC32分桶
    """
    crc = binascii.crc32(key.encode('utf-8')) & 0xffffffff
    return crc % cardinality

import unittest

class TestHashAlgorithm(unittest.TestCase):
    def test_hash_algorithm_sizes(self):
        self.assertEqual(HashAlgorithm.Md5.size(), 16)
        self.assertEqual(HashAlgorithm.HighwayHash256.size(), 32)
        self.assertEqual(HashAlgorithm.HighwayHash256S.size(), 32)
        self.assertEqual(HashAlgorithm.SHA256.size(), 32)
        self.assertEqual(HashAlgorithm.BLAKE2b512.size(), 32)
        self.assertEqual(HashAlgorithm.NONE.size(), 0)

    def test_hash_encode_none(self):
        data = b"test data"
        hashv = HashAlgorithm.NONE.hash_encode(data)
        self.assertEqual(len(hashv), 0)

    def test_hash_encode_md5(self):
        data = b"test data"
        hash1 = HashAlgorithm.Md5.hash_encode(data)
        self.assertEqual(len(hash1), 16)
        hash2 = HashAlgorithm.Md5.hash_encode(data)
        self.assertEqual(hash1, hash2)

    def test_hash_encode_sha256(self):
        data = b"test data"
        hash1 = HashAlgorithm.SHA256.hash_encode(data)
        self.assertEqual(len(hash1), 32)
        hash2 = HashAlgorithm.SHA256.hash_encode(data)
        self.assertEqual(hash1, hash2)

    def test_hash_encode_blake2b512(self):
        data = b"test data"
        hash1 = HashAlgorithm.BLAKE2b512.hash_encode(data)
        self.assertEqual(len(hash1), 32)
        hash2 = HashAlgorithm.BLAKE2b512.hash_encode(data)
        self.assertEqual(hash1, hash2)

    def test_different_data_different_hashes(self):
        data1 = b"test data 1"
        data2 = b"test data 2"
        md5_hash1 = HashAlgorithm.Md5.hash_encode(data1)
        md5_hash2 = HashAlgorithm.Md5.hash_encode(data2)
        self.assertNotEqual(md5_hash1, md5_hash2)
        sha256_hash1 = HashAlgorithm.SHA256.hash_encode(data1)
        sha256_hash2 = HashAlgorithm.SHA256.hash_encode(data2)
        self.assertNotEqual(sha256_hash1, sha256_hash2)
        blake_hash1 = HashAlgorithm.BLAKE2b512.hash_encode(data1)
        blake_hash2 = HashAlgorithm.BLAKE2b512.hash_encode(data2)
        self.assertNotEqual(blake_hash1, blake_hash2)

    def test_hash_encode_highway(self):
        if highwayhash is None:
            self.skipTest("未安装highwayhash库")
        data = b"test data"
        hash1 = HashAlgorithm.HighwayHash256.hash_encode(data)
        self.assertEqual(len(hash1), 32)
        hash2 = HashAlgorithm.HighwayHash256.hash_encode(data)
        self.assertEqual(hash1, hash2)
        # 不同数据不同
        hash3 = HashAlgorithm.HighwayHash256.hash_encode(b"test data 2")
        self.assertNotEqual(hash1, hash3)

if __name__ == "__main__":
    unittest.main()