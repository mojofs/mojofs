import os
import logging

from mojofs.ecstore.config.notify import KV

logger = logging.getLogger("ecstore.config.storageclass")

# 默认校验盘数量，根据磁盘总数分配
def default_parity_count(drive: int) -> int:
    if drive == 1:
        return 0
    elif drive in (2, 3):
        return 1
    elif drive in (4, 5):
        return 2
    elif drive in (6, 7):
        return 3
    else:
        return 4

# 存储类型常量
RRS = "REDUCED_REDUNDANCY"
STANDARD = "STANDARD"

# 配置存储类型常量
CLASS_STANDARD = "standard"
CLASS_RRS = "rrs"
OPTIMIZE = "optimize"
INLINE_BLOCK = "inline_block"

# 环境变量名
RRS_ENV = "RUSTFS_STORAGE_CLASS_RRS"
STANDARD_ENV = "RUSTFS_STORAGE_CLASS_STANDARD"
OPTIMIZE_ENV = "RUSTFS_STORAGE_CLASS_OPTIMIZE"
INLINE_BLOCK_ENV = "RUSTFS_STORAGE_CLASS_INLINE_BLOCK"

# 支持的存储类型前缀
SCHEME_PREFIX = "EC"

# 最小校验盘数
MIN_PARITY_DRIVES = 0

# RRS默认校验盘
DEFAULT_RRS_PARITY = 1

DEFAULT_INLINE_BLOCK = 128 * 1024

DEFAULT_KVS = [
    KV(CLASS_STANDARD, "", False),
    KV(CLASS_RRS, "EC:1", False),
    KV(OPTIMIZE, "availability", False),
    KV(INLINE_BLOCK, "", True),
]

class StorageClass:
    def __init__(self, parity: int = 0):
        self.parity = parity

    def __repr__(self):
        return f"StorageClass(parity={self.parity})"

class Config:
    def __init__(self, standard=None, rrs=None, optimize=None, inline_block=DEFAULT_INLINE_BLOCK, initialized=False):
        self.standard = standard if standard else StorageClass()
        self.rrs = rrs if rrs else StorageClass()
        self.optimize = optimize
        self.inline_block = inline_block
        self.initialized = initialized

    def get_parity_for_sc(self, sc: str):
        sc = sc.strip()
        if sc == RRS:
            return self.rrs.parity if self.initialized else None
        else:
            return self.standard.parity if self.initialized else None

    def should_inline(self, shard_size: int, versioned: bool) -> bool:
        if shard_size < 0:
            return False
        inline_block = self.inline_block if self.initialized else DEFAULT_INLINE_BLOCK
        if versioned:
            return shard_size <= inline_block // 8
        else:
            return shard_size <= inline_block

    def get_inline_block(self) -> int:
        return self.inline_block if self.initialized else DEFAULT_INLINE_BLOCK

    def capacity_optimized(self) -> bool:
        if not self.initialized:
            return False
        return self.optimize == "capacity"

def lookup_config(kvs, set_drive_count: int):
    # 获取standard配置
    ssc_str = os.environ.get(STANDARD_ENV) or _get_kv_value(kvs, CLASS_STANDARD)
    if ssc_str:
        standard = parse_storage_class(ssc_str)
    else:
        standard = StorageClass(parity=default_parity_count(set_drive_count))

    # 获取rrs配置
    rrs_str = os.environ.get(RRS_ENV) or _get_kv_value(kvs, RRS)
    if rrs_str:
        rrs = parse_storage_class(rrs_str)
    else:
        rrs = StorageClass(parity=0 if set_drive_count == 1 else DEFAULT_RRS_PARITY)

    # 校验
    validate_parity_inner(standard.parity, rrs.parity, set_drive_count)

    # optimize
    optimize = os.environ.get(OPTIMIZE_ENV, None)

    # inline_block
    inline_block = DEFAULT_INLINE_BLOCK
    ev = os.environ.get(INLINE_BLOCK_ENV)
    if ev:
        try:
            block = _parse_bytesize(ev)
            if block > DEFAULT_INLINE_BLOCK:
                logger.warning(
                    f"inline block value bigger than recommended max of 128KiB -> {block}, 性能可能下降，请进行基准测试"
                )
            inline_block = block
        except Exception:
            raise ValueError(f"解析 {INLINE_BLOCK_ENV} 格式失败")
    return Config(
        standard=standard,
        rrs=rrs,
        optimize=optimize,
        inline_block=inline_block,
        initialized=True
    )

def parse_storage_class(env: str) -> StorageClass:
    s = env.split(":")
    if len(s) != 2:
        raise ValueError(f"无效的存储类型格式: {env}，期望 'Scheme:校验盘数'")
    if s[0] != SCHEME_PREFIX:
        raise ValueError(f"不支持的scheme {s[0]}，只支持EC")
    try:
        parity_drives = int(s[1])
    except Exception:
        raise ValueError(f"无法解析校验盘数: {s[1]}")
    return StorageClass(parity=parity_drives)

def validate_parity(ss_parity: int, set_drive_count: int):
    # if ss_parity > 0 and ss_parity < MIN_PARITY_DRIVES:
    #     raise ValueError(f"parity {ss_parity} 应该大于等于 {MIN_PARITY_DRIVES}")
    if ss_parity > set_drive_count // 2:
        raise ValueError(f"parity {ss_parity} 应该小于等于 {set_drive_count // 2}")

def validate_parity_inner(ss_parity: int, rrs_parity: int, set_drive_count: int):
    # if ss_parity > 0 and ss_parity < MIN_PARITY_DRIVES:
    #     raise ValueError(f"Standard storage class parity {ss_parity} 应该大于等于 {MIN_PARITY_DRIVES}")
    # if rrs_parity > 0 and rrs_parity < MIN_PARITY_DRIVES:
    #     raise ValueError(f"Reduced redundancy storage class parity {rrs_parity} 应该大于等于 {MIN_PARITY_DRIVES}")
    if set_drive_count > 2:
        if ss_parity > set_drive_count // 2:
            raise ValueError(f"Standard storage class parity {ss_parity} 应该小于等于 {set_drive_count // 2}")
        if rrs_parity > set_drive_count // 2:
            raise ValueError(f"Reduced redundancy storage class parity {rrs_parity} 应该小于等于 {set_drive_count // 2}")
    if ss_parity > 0 and rrs_parity > 0 and ss_parity < rrs_parity:
        raise ValueError(
            f"Standard storage class parity drives {ss_parity} 应该大于等于 Reduced redundancy storage class parity drives {rrs_parity}"
        )

def _get_kv_value(kvs, key):
    # kvs为KV对象列表
    for kv in kvs:
        if getattr(kv, "key", None) == key:
            return getattr(kv, "value", "")
    return ""

def _parse_bytesize(s):
    """
    支持如"128KiB"、"1MiB"、"1024"等格式，返回字节数
    """
    s = s.strip().lower()
    if s.endswith("kib"):
        return int(float(s[:-3]) * 1024)
    if s.endswith("kb"):
        return int(float(s[:-2]) * 1000)
    if s.endswith("mib"):
        return int(float(s[:-3]) * 1024 * 1024)
    if s.endswith("mb"):
        return int(float(s[:-2]) * 1000 * 1000)
    if s.endswith("gib"):
        return int(float(s[:-3]) * 1024 * 1024 * 1024)
    if s.endswith("gb"):
        return int(float(s[:-2]) * 1000 * 1000 * 1000)
    return int(float(s))