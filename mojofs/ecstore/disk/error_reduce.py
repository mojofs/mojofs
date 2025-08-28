from mojofs.ecstore.disk.error import DiskError

# 定义常用的忽略错误集合
OBJECT_OP_IGNORED_ERRS = [
    DiskError(DiskError.Kind.DiskNotFound),
    DiskError(DiskError.Kind.FaultyDisk),
    DiskError(DiskError.Kind.FaultyRemoteDisk),
    DiskError(DiskError.Kind.DiskAccessDenied),
    DiskError(DiskError.Kind.DiskOngoingReq),
    DiskError(DiskError.Kind.UnformattedDisk),
]

BUCKET_OP_IGNORED_ERRS = [
    DiskError(DiskError.Kind.DiskNotFound),
    DiskError(DiskError.Kind.FaultyDisk),
    DiskError(DiskError.Kind.FaultyRemoteDisk),
    DiskError(DiskError.Kind.DiskAccessDenied),
    DiskError(DiskError.Kind.UnformattedDisk),
]

BASE_IGNORED_ERRS = [
    DiskError(DiskError.Kind.DiskNotFound),
    DiskError(DiskError.Kind.FaultyDisk),
    DiskError(DiskError.Kind.FaultyRemoteDisk),
]

def reduce_write_quorum_errs(errors, ignored_errs, quorum):
    return reduce_quorum_errs(errors, ignored_errs, quorum, DiskError(DiskError.Kind.ErasureWriteQuorum))

def reduce_read_quorum_errs(errors, ignored_errs, quorum):
    return reduce_quorum_errs(errors, ignored_errs, quorum, DiskError(DiskError.Kind.ErasureReadQuorum))

def reduce_quorum_errs(errors, ignored_errs, quorum, quorum_err):
    max_count, err = reduce_errs(errors, ignored_errs)
    if max_count >= quorum:
        return err
    else:
        return quorum_err

def reduce_errs(errors, ignored_errs):
    """
    :param errors: List[Optional[DiskError]]
    :param ignored_errs: List[DiskError]
    :return: (int, Optional[DiskError])
    """
    from collections import Counter

    # 统计None的数量（视为nil错误）
    nil_count = sum(1 for e in errors if e is None)

    # 统计非None且不在ignored_errs中的错误
    filtered = [e for e in errors if e is not None and not is_ignored_err(ignored_errs, e)]
    err_counts = Counter(filtered)

    if err_counts:
        best_err, best_count = err_counts.most_common(1)[0]
    else:
        best_err, best_count = None, 0

    # 优先选择nil错误
    if nil_count > best_count or (nil_count == best_count and nil_count > 0):
        return nil_count, None
    else:
        return best_count, best_err

def is_ignored_err(ignored_errs, err):
    return any(e == err for e in ignored_errs)

def count_errs(errors, err):
    return sum(1 for e in errors if e == err)

def is_all_buckets_not_found(errs):
    """
    判断errs中是否所有错误都是DiskNotFound或VolumeNotFound
    """
    if not errs:
        return False
    for err in errs:
        if err is not None:
            if err.kind == DiskError.Kind.DiskNotFound or err.kind == DiskError.Kind.VolumeNotFound:
                continue
            return False
        return False
    return True

# 单元测试
import unittest

class TestErrorReduce(unittest.TestCase):
    def err_io(self, msg):
        # 这里模拟Io错误，实际可根据DiskError实现调整
        return DiskError(DiskError.Kind.Io, detail=msg)

    def test_reduce_errs_basic(self):
        e1 = self.err_io("a")
        e2 = self.err_io("b")
        errors = [e1, e1, e2, None]
        ignored = []
        count, err = reduce_errs(errors, ignored)
        self.assertEqual(count, 2)
        self.assertEqual(err, e1)

    def test_reduce_errs_ignored(self):
        e1 = self.err_io("a")
        e2 = self.err_io("b")
        errors = [e1, e2, e1, e2, None]
        ignored = [e2]
        count, err = reduce_errs(errors, ignored)
        self.assertEqual(count, 2)
        self.assertEqual(err, e1)

    def test_reduce_quorum_errs(self):
        e1 = self.err_io("a")
        e2 = self.err_io("b")
        errors = [e1, e1, e2, None]
        ignored = []
        quorum_err = DiskError(DiskError.Kind.FaultyDisk)
        # quorum = 2, 应返回e1
        res = reduce_quorum_errs(errors, ignored, 2, quorum_err)
        self.assertEqual(res, e1)
        # quorum = 3, 应返回quorum_err
        res = reduce_quorum_errs(errors, ignored, 3, quorum_err)
        self.assertEqual(res, quorum_err)

    def test_count_errs(self):
        e1 = self.err_io("a")
        e2 = self.err_io("b")
        errors = [e1, e2, e1, None]
        self.assertEqual(count_errs(errors, e1), 2)
        self.assertEqual(count_errs(errors, e2), 1)

    def test_is_ignored_err(self):
        e1 = self.err_io("a")
        e2 = self.err_io("b")
        ignored = [e1]
        self.assertTrue(is_ignored_err(ignored, e1))
        self.assertFalse(is_ignored_err(ignored, e2))

    def test_reduce_errs_nil_tiebreak(self):
        # None和另一个错误数量相同，应优先返回None
        e1 = self.err_io("a")
        errors = [e1, None, e1, None]  # e1:2, None:2
        ignored = []
        count, err = reduce_errs(errors, ignored)
        self.assertEqual(count, 2)
        self.assertIsNone(err)

if __name__ == "__main__":
    unittest.main()