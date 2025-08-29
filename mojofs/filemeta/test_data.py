from mojofs.filemeta.error import Error
from mojofs.filemeta.filemeta import FileMeta, MetaObject, FileMetaVersion, FileMetaShallowVersion, VersionType, MetaDeleteMarker, XL_META_VERSION, ErasureAlgo, ChecksumAlgo
import uuid
import datetime
import struct

def create_real_xlmeta():
    """
    创建一个真实的 xl.meta 文件数据用于测试
    """
    fm = FileMeta.new()

    # 创建一个真实的对象版本
    version_id = uuid.UUID("01234567-89ab-cdef-0123-456789abcdef")
    data_dir = uuid.UUID("fedcba98-7654-3210-fedc-ba9876543210")

    metadata = {
        "Content-Type": "text/plain",
        "X-Amz-Meta-Author": "test-user",
        "X-Amz-Meta-Created": "2024-01-15T10:30:00Z"
    }

    object_version = MetaObject(
        version_id=version_id,
        data_dir=data_dir,
        erasure_algorithm=ErasureAlgo.ReedSolomon,
        erasure_m=4,
        erasure_n=2,
        erasure_block_size=1024 * 1024,
        erasure_index=1,
        erasure_dist=[0, 1, 2, 3, 4, 5],
        bitrot_checksum_algo=ChecksumAlgo.HighwayHash,
        part_numbers=[1],
        part_etags=["d41d8cd98f00b204e9800998ecf8427e"],
        part_sizes=[1024],
        part_actual_sizes=[1024],
        part_indices=[],
        size=1024,
        mod_time=datetime.datetime.utcfromtimestamp(1705312200),
        meta_sys={},
        meta_user=metadata
    )

    file_version = FileMetaVersion(
        version_type=VersionType.Object,
        object=object_version,
        delete_marker=None,
        write_version=1
    )

    shallow_version = FileMetaShallowVersion.try_from(file_version)
    fm.versions.append(shallow_version)

    # 添加一个删除标记版本
    delete_version_id = uuid.UUID("11111111-2222-3333-4444-555555555555")
    delete_marker = MetaDeleteMarker(
        version_id=delete_version_id,
        mod_time=datetime.datetime.utcfromtimestamp(1705312260),
        meta_sys=None
    )

    delete_file_version = FileMetaVersion(
        version_type=VersionType.Delete,
        object=None,
        delete_marker=delete_marker,
        write_version=2
    )

    delete_shallow_version = FileMetaShallowVersion.try_from(delete_file_version)
    fm.versions.append(delete_shallow_version)

    # 添加一个 Legacy 版本用于测试
    legacy_version_id = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    legacy_version = FileMetaVersion(
        version_type=VersionType.Legacy,
        object=None,
        delete_marker=None,
        write_version=3
    )

    legacy_shallow = FileMetaShallowVersion.try_from(legacy_version)
    legacy_shallow.header.version_id = legacy_version_id
    legacy_shallow.header.mod_time = datetime.datetime.utcfromtimestamp(1705312140)
    fm.versions.append(legacy_shallow)

    # 按修改时间排序（最新的在前）
    fm.versions.sort(key=lambda v: v.header.mod_time, reverse=True)

    return fm.marshal_msg()

def create_complex_xlmeta():
    """
    创建一个包含多个版本的复杂 xl.meta 文件
    """
    fm = FileMeta.new()

    for i in range(10):
        version_id = uuid.uuid4()
        data_dir = uuid.uuid4() if i % 3 == 0 else None

        metadata = {
            "Content-Type": "application/octet-stream",
            "X-Amz-Meta-Version": str(i),
            "X-Amz-Meta-Test": f"test-value-{i}"
        }

        object_version = MetaObject(
            version_id=version_id,
            data_dir=data_dir,
            erasure_algorithm=ErasureAlgo.ReedSolomon,
            erasure_m=4,
            erasure_n=2,
            erasure_block_size=1024 * 1024,
            erasure_index=i % 6,
            erasure_dist=[0, 1, 2, 3, 4, 5],
            bitrot_checksum_algo=ChecksumAlgo.HighwayHash,
            part_numbers=[1],
            part_etags=[f"etag-{i:08x}"],
            part_sizes=[1024 * (i + 1)],
            part_actual_sizes=[1024 * (i + 1)],
            part_indices=[],
            size=1024 * (i + 1),
            mod_time=datetime.datetime.utcfromtimestamp(1705312200 + i * 60),
            meta_sys={},
            meta_user=metadata
        )

        file_version = FileMetaVersion(
            version_type=VersionType.Object,
            object=object_version,
            delete_marker=None,
            write_version=i + 1
        )

        shallow_version = FileMetaShallowVersion.try_from(file_version)
        fm.versions.append(shallow_version)

        # 每隔3个版本添加一个删除标记
        if i % 3 == 2:
            delete_version_id = uuid.uuid4()
            delete_marker = MetaDeleteMarker(
                version_id=delete_version_id,
                mod_time=datetime.datetime.utcfromtimestamp(1705312200 + i * 60 + 30),
                meta_sys=None
            )

            delete_file_version = FileMetaVersion(
                version_type=VersionType.Delete,
                object=None,
                delete_marker=delete_marker,
                write_version=i + 100
            )

            delete_shallow_version = FileMetaShallowVersion.try_from(delete_file_version)
            fm.versions.append(delete_shallow_version)

    # 按修改时间排序（最新的在前）
    fm.versions.sort(key=lambda v: v.header.mod_time, reverse=True)

    return fm.marshal_msg()

def create_corrupted_xlmeta():
    """
    创建一个损坏的 xl.meta 文件用于错误处理测试
    """
    # 正确的文件头
    data = bytearray()
    data.extend(b'XL2 ')
    data.extend([1, 0, 3, 0])
    data.extend([0xc6, 0x00, 0x00, 0x00, 0x10])
    # 添加不足的数据（少于声明的长度）
    data.extend([0x42] * 8)
    return bytes(data)

def create_empty_xlmeta():
    """
    创建一个空的 xl.meta 文件
    """
    fm = FileMeta.new()
    return fm.marshal_msg()

def verify_parsed_metadata(fm, expected_versions):
    """
    验证解析结果的辅助函数
    """
    assert len(fm.versions) == expected_versions, "版本数量不匹配"
    assert fm.meta_ver == XL_META_VERSION, "元数据版本不匹配"

    # 验证版本是否按修改时间排序
    for i in range(1, len(fm.versions)):
        prev_time = fm.versions[i - 1].header.mod_time
        curr_time = fm.versions[i].header.mod_time
        if prev_time is not None and curr_time is not None:
            assert prev_time >= curr_time, "版本未按修改时间正确排序"
    return True

def create_xlmeta_with_inline_data():
    """
    创建一个包含内联数据的 xl.meta 文件
    """
    fm = FileMeta.new()

    # 添加内联数据
    inline_data = b"This is inline data for testing purposes"
    version_id = uuid.uuid4()
    fm.data.replace(str(version_id), list(inline_data))

    object_version = MetaObject(
        version_id=version_id,
        data_dir=None,
        erasure_algorithm=ErasureAlgo.ReedSolomon,
        erasure_m=1,
        erasure_n=1,
        erasure_block_size=64 * 1024,
        erasure_index=0,
        erasure_dist=[0, 1],
        bitrot_checksum_algo=ChecksumAlgo.HighwayHash,
        part_numbers=[1],
        part_etags=[],
        part_sizes=[len(inline_data)],
        part_actual_sizes=[],
        part_indices=[],
        size=len(inline_data),
        mod_time=datetime.datetime.utcnow(),
        meta_sys={},
        meta_user={}
    )

    file_version = FileMetaVersion(
        version_type=VersionType.Object,
        object=object_version,
        delete_marker=None,
        write_version=1
    )

    shallow_version = FileMetaShallowVersion.try_from(file_version)
    fm.versions.append(shallow_version)

    return fm.marshal_msg()

import unittest

class TestFileMetaData(unittest.TestCase):
    def test_create_real_xlmeta(self):
        data = create_real_xlmeta()
        self.assertTrue(data, "生成的数据不应为空")
        # 验证文件头
        self.assertEqual(data[:4], b"XL2 ", "文件头不正确")
        # 尝试解析
        fm = FileMeta.load(data)
        self.assertTrue(verify_parsed_metadata(fm, 3))

    def test_create_complex_xlmeta(self):
        data = create_complex_xlmeta()
        self.assertTrue(data, "生成的数据不应为空")
        fm = FileMeta.load(data)
        self.assertGreaterEqual(len(fm.versions), 10, "应该有至少10个版本")

    def test_create_xlmeta_with_inline_data(self):
        data = create_xlmeta_with_inline_data()
        self.assertTrue(data, "生成的数据不应为空")
        fm = FileMeta.load(data)
        self.assertEqual(len(fm.versions), 1, "应该有1个版本")
        self.assertTrue(fm.data.as_slice(), "应该包含内联数据")

    def test_corrupted_xlmeta_handling(self):
        data = create_corrupted_xlmeta()
        with self.assertRaises(Exception):
            FileMeta.load(data)

    def test_empty_xlmeta(self):
        data = create_empty_xlmeta()
        fm = FileMeta.load(data)
        self.assertEqual(len(fm.versions), 0, "空文件应该没有版本")

if __name__ == "__main__":
    unittest.main()