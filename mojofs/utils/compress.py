import io
import gzip
import zstd
import lz4.block
import brotli
import snappy
from enum import Enum
import time

class CompressionAlgorithm(Enum):
    NONE = "none"
    GZIP = "gzip"
    ZSTD = "zstd"
    LZ4 = "lz4"
    BROTLI = "brotli"
    SNAPPY = "snappy"

    def __str__(self):
        return self.value

    @staticmethod
    def from_string(s):
        s = s.lower()
        if s == "gzip":
            return CompressionAlgorithm.GZIP
        elif s == "zstd":
            return CompressionAlgorithm.ZSTD
        elif s == "lz4":
            return CompressionAlgorithm.LZ4
        elif s == "brotli":
            return CompressionAlgorithm.BROTLI
        elif s == "snappy":
            return CompressionAlgorithm.SNAPPY
        elif s == "none":
            return CompressionAlgorithm.NONE
        else:
            raise ValueError(f"Unsupported compression algorithm: {s}")

def compress_block(input_data, algorithm):
    # 如果是bytearray，转换为bytes
    if isinstance(input_data, bytearray):
        input_data = bytes(input_data)
    
    if algorithm == CompressionAlgorithm.GZIP:
        return gzip.compress(input_data)
    elif algorithm == CompressionAlgorithm.ZSTD:
        return zstd.compress(input_data)
    elif algorithm == CompressionAlgorithm.LZ4:
        return lz4.block.compress(input_data)
    elif algorithm == CompressionAlgorithm.BROTLI:
        return brotli.compress(input_data)
    elif algorithm == CompressionAlgorithm.SNAPPY:
        return snappy.compress(input_data)
    elif algorithm == CompressionAlgorithm.NONE:
        return input_data
    else:
        raise ValueError(f"Unsupported compression algorithm: {algorithm}")

def decompress_block(compressed_data, algorithm):
    # 如果是bytearray，转换为bytes
    if isinstance(compressed_data, bytearray):
        compressed_data = bytes(compressed_data)
    
    if algorithm == CompressionAlgorithm.GZIP:
        return gzip.decompress(compressed_data)
    elif algorithm == CompressionAlgorithm.ZSTD:
        return zstd.decompress(compressed_data)
    elif algorithm == CompressionAlgorithm.LZ4:
        return lz4.block.decompress(compressed_data)
    elif algorithm == CompressionAlgorithm.BROTLI:
        return brotli.decompress(compressed_data)
    elif algorithm == CompressionAlgorithm.SNAPPY:
        return snappy.decompress(compressed_data)
    elif algorithm == CompressionAlgorithm.NONE:
        return compressed_data
    else:
        raise ValueError(f"Unsupported compression algorithm: {algorithm}")

if __name__ == '__main__':
    import unittest

    class TestCompression(unittest.TestCase):
        def test_compress_decompress_gzip(self):
            data = b"hello gzip compress"
            compressed = compress_block(data, CompressionAlgorithm.GZIP)
            decompressed = decompress_block(compressed, CompressionAlgorithm.GZIP)
            self.assertEqual(decompressed, data)

        def test_compress_decompress_zstd(self):
            data = b"hello zstd compress"
            compressed = compress_block(data, CompressionAlgorithm.ZSTD)
            decompressed = decompress_block(compressed, CompressionAlgorithm.ZSTD)
            self.assertEqual(decompressed, data)

        def test_compress_decompress_lz4(self):
            data = b"hello lz4 compress"
            compressed = compress_block(data, CompressionAlgorithm.LZ4)
            decompressed = decompress_block(compressed, CompressionAlgorithm.LZ4)
            self.assertEqual(decompressed, data)

        def test_compress_decompress_brotli(self):
            data = b"hello brotli compress"
            compressed = compress_block(data, CompressionAlgorithm.BROTLI)
            decompressed = decompress_block(compressed, CompressionAlgorithm.BROTLI)
            self.assertEqual(decompressed, data)

        def test_compress_decompress_snappy(self):
            data = b"hello snappy compress"
            compressed = compress_block(data, CompressionAlgorithm.SNAPPY)
            decompressed = decompress_block(compressed, CompressionAlgorithm.SNAPPY)
            self.assertEqual(decompressed, data)

        def test_from_string(self):
            self.assertEqual(CompressionAlgorithm.from_string("gzip"), CompressionAlgorithm.GZIP)
            self.assertEqual(CompressionAlgorithm.from_string("zstd"), CompressionAlgorithm.ZSTD)
            self.assertEqual(CompressionAlgorithm.from_string("lz4"), CompressionAlgorithm.LZ4)
            self.assertEqual(CompressionAlgorithm.from_string("brotli"), CompressionAlgorithm.BROTLI)
            self.assertEqual(CompressionAlgorithm.from_string("snappy"), CompressionAlgorithm.SNAPPY)
            with self.assertRaises(ValueError):
                CompressionAlgorithm.from_string("unknown")

        def test_compare_compression_algorithms(self):
            import random
            data = bytearray([42] * 1024 * 100)  # 100KB 重复数据

            start = time.time()
            times = [("original", time.time() - start, len(data))]

            start = time.time()
            gzip_compressed = compress_block(data, CompressionAlgorithm.GZIP)
            gzip_time = time.time() - start
            times.append(("gzip", gzip_time, len(gzip_compressed)))

            start = time.time()
            zstd_compressed = compress_block(data, CompressionAlgorithm.ZSTD)
            zstd_time = time.time() - start
            times.append(("zstd", zstd_time, len(zstd_compressed)))

            start = time.time()
            lz4_compressed = compress_block(data, CompressionAlgorithm.LZ4)
            lz4_time = time.time() - start
            times.append(("lz4", lz4_time, len(lz4_compressed)))

            start = time.time()
            brotli_compressed = compress_block(data, CompressionAlgorithm.BROTLI)
            brotli_time = time.time() - start
            times.append(("brotli", brotli_time, len(brotli_compressed)))

            start = time.time()
            snappy_compressed = compress_block(data, CompressionAlgorithm.SNAPPY)
            snappy_time = time.time() - start
            times.append(("snappy", snappy_time, len(snappy_compressed)))

            print("Compression results:")
            for name, dur, size in times:
                print(f"{name}: {size} bytes, {dur:.4f} seconds")

            self.assertEqual(decompress_block(gzip_compressed, CompressionAlgorithm.GZIP), data)
            self.assertEqual(decompress_block(zstd_compressed, CompressionAlgorithm.ZSTD), data)
            self.assertEqual(decompress_block(lz4_compressed, CompressionAlgorithm.LZ4), data)
            self.assertEqual(decompress_block(brotli_compressed, CompressionAlgorithm.BROTLI), data)
            self.assertEqual(decompress_block(snappy_compressed, CompressionAlgorithm.SNAPPY), data)

            # 修复：原本写成了len(x[0])，但x本身就是bytes对象，直接len(x)即可
            self.assertTrue(all(len(x) > 0 for x in [gzip_compressed, zstd_compressed, lz4_compressed, brotli_compressed, snappy_compressed]))

        def test_compression_benchmark(self):
            sizes = [128 * 1024, 512 * 1024, 1024 * 1024]
            algorithms = [
                CompressionAlgorithm.GZIP,
                CompressionAlgorithm.ZSTD,
                CompressionAlgorithm.LZ4,
                CompressionAlgorithm.BROTLI,
                CompressionAlgorithm.SNAPPY,
            ]

            print("\nCompression algorithm benchmark results:")
            print(
                "{:<10} {:<10} {:<15} {:<15} {:<15}".format(
                    "Data Size", "Algorithm", "Compress Time(ms)", "Compressed Size", "Compression Ratio"
                )
            )

            for size in sizes:
                # 生成可压缩数据（重复文本模式）
                pattern = b"Hello, this is a test pattern that will be repeated multiple times to create compressible data. "
                data = pattern * (size // len(pattern))

                for algo in algorithms:
                    # 压缩测试
                    start = time.time()
                    compressed = compress_block(data, algo)
                    compression_time = time.time() - start

                    # 解压测试
                    start = time.time()
                    decompressed = decompress_block(compressed, algo)
                    decompression_time = time.time() - start

                    # 计算压缩比
                    compression_ratio = size / len(compressed)

                    print(
                        "{:<10} {:<10} {:<15.2f} {:<15} {:<15.2f}x".format(
                            "{}KB".format(size // 1024),
                            str(algo),
                            compression_time * 1000,
                            len(compressed),
                            compression_ratio,
                        )
                    )

                    # 校验解压结果
                    self.assertEqual(decompressed, data)
                print()

    unittest.main()