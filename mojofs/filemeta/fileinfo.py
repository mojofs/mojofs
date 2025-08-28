import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Union

ERASURE_ALGORITHM = "rs-vandermonde"
BLOCK_SIZE_V2 = 1024 * 1024  # 1M

# Additional constants from Go version
NULL_VERSION_ID = "null"
# RUSTFS_ERASURE_UPGRADED = "x-rustfs-internal-erasure-upgraded"

TIER_FV_ID = "tier-free-versionID"
TIER_FV_MARKER = "tier-free-marker"
TIER_SKIP_FV_ID = "tier-skip-fvid"


@dataclass
class ObjectPartInfo:
    etag: str = ""
    number: int = 0
    size: int = 0
    actual_size: int = 0  # Original data size
    mod_time: Optional[float] = None  # time.OffsetDateTime
    index: Optional[bytes] = None
    checksums: Optional[Dict[str, str]] = None
    error: Optional[str] = None

    def marshal_msg(self) -> bytes:
        # Simplified serialization, replace with actual msgpack if needed
        return str(self).encode('utf-8')

    @staticmethod
    def unmarshal(buf: bytes) -> "ObjectPartInfo":
        # Simplified deserialization, replace with actual msgpack if needed
        # This is a placeholder, you'll need to implement proper deserialization
        return eval(buf.decode('utf-8'))


@dataclass
class ChecksumInfo:
    part_number: int = 0
    algorithm: str = ""  # HashAlgorithm
    hash: bytes = b""


class ErasureAlgo(Enum):
    Invalid = 0
    ReedSolomon = 1

    def valid(self) -> bool:
        return self.value > ErasureAlgo.Invalid.value

    def to_u8(self) -> int:
        return self.value

    @staticmethod
    def from_u8(u: int) -> "ErasureAlgo":
        if u == 1:
            return ErasureAlgo.ReedSolomon
        return ErasureAlgo.Invalid

    def __str__(self) -> str:
        if self == ErasureAlgo.Invalid:
            return "Invalid"
        return ERASURE_ALGORITHM


def calc_shard_size(block_size: int, data_shards: int) -> int:
    return (block_size // data_shards + (1 if block_size % data_shards else 0) + 1) & ~1


@dataclass
class ErasureInfo:
    algorithm: str = ""
    data_blocks: int = 0
    parity_blocks: int = 0
    block_size: int = 0
    index: int = 0
    distribution: List[int] = field(default_factory=list)
    checksums: List[ChecksumInfo] = field(default_factory=list)

    def get_checksum_info(self, part_number: int) -> ChecksumInfo:
        for sum_info in self.checksums:
            if sum_info.part_number == part_number:
                return sum_info
        return ChecksumInfo(algorithm="HighwayHash256S")

    def shard_size(self) -> int:
        return calc_shard_size(self.block_size, self.data_blocks)

    def shard_file_size(self, total_length: int) -> int:
        if total_length == 0:
            return 0
        if total_length < 0:
            return total_length

        num_shards = total_length // self.block_size
        last_block_size = total_length % self.block_size
        last_shard_size = calc_shard_size(last_block_size, self.data_blocks)
        return num_shards * self.shard_size() + last_shard_size

    def equals(self, other: "ErasureInfo") -> bool:
        return (
            self.algorithm == other.algorithm
            and self.data_blocks == other.data_blocks
            and self.parity_blocks == other.parity_blocks
            and self.block_size == other.block_size
            and self.index == other.index
            and self.distribution == other.distribution
        )


@dataclass
class FileInfo:
    volume: str = ""
    name: str = ""
    version_id: Optional[uuid.UUID] = None
    is_latest: bool = False
    deleted: bool = False
    transition_status: str = ""
    transitioned_objname: str = ""
    transition_tier: str = ""
    transition_version_id: Optional[uuid.UUID] = None
    expire_restored: bool = False
    data_dir: Optional[uuid.UUID] = None
    mod_time: Optional[float] = None  # time.OffsetDateTime
    size: int = 0
    mode: Optional[int] = None
    written_by_version: Optional[int] = None
    metadata: Dict[str, str] = field(default_factory=dict)
    parts: List[ObjectPartInfo] = field(default_factory=list)
    erasure: ErasureInfo = field(default_factory=ErasureInfo)
    mark_deleted: bool = False
    data: Optional[bytes] = None
    num_versions: int = 0
    successor_mod_time: Optional[float] = None # time.OffsetDateTime
    fresh: bool = False
    idx: int = 0
    checksum: Optional[bytes] = None
    versioned: bool = False

    def __post_init__(self):
        if not self.erasure:
            self.erasure = ErasureInfo()

    @staticmethod
    def new(object_name: str, data_blocks: int, parity_blocks: int) -> "FileInfo":
        cardinality = data_blocks + parity_blocks
        nums = [0] * cardinality
        key_crc = hash(object_name.encode('utf-8'))  # Using Python's hash function

        start = key_crc % cardinality
        for i in range(cardinality):
            nums[i] = 1 + ((start + i + 1) % cardinality)

        return FileInfo(
            erasure=ErasureInfo(
                algorithm=ERASURE_ALGORITHM,
                data_blocks=data_blocks,
                parity_blocks=parity_blocks,
                block_size=BLOCK_SIZE_V2,
                distribution=nums,
            )
        )

    def is_valid(self) -> bool:
        if self.deleted:
            return True

        data_blocks = self.erasure.data_blocks
        parity_blocks = self.erasure.parity_blocks

        return (
            data_blocks >= parity_blocks
            and data_blocks > 0
            and self.erasure.index > 0
            and self.erasure.index <= data_blocks + parity_blocks
            and len(self.erasure.distribution) == data_blocks + parity_blocks
        )

    def get_etag(self) -> Optional[str]:
        return self.metadata.get("etag")

    def write_quorum(self, quorum: int) -> int:
        if self.deleted:
            return quorum

        if self.erasure.data_blocks == self.erasure.parity_blocks:
            return self.erasure.data_blocks + 1

        return self.erasure.data_blocks

    def marshal_msg(self) -> bytes:
        # Simplified serialization, replace with actual msgpack if needed
        return str(self).encode('utf-8')

    @staticmethod
    def unmarshal(buf: bytes) -> "FileInfo":
        # Simplified deserialization, replace with actual msgpack if needed
        # This is a placeholder, you'll need to implement proper deserialization
        return eval(buf.decode('utf-8'))

    def add_object_part(
        self,
        num: int,
        etag: str,
        part_size: int,
        mod_time: Optional[float],
        actual_size: int,
        index: Optional[bytes],
    ) -> None:
        part = ObjectPartInfo(
            etag=etag,
            number=num,
            size=part_size,
            mod_time=mod_time,
            actual_size=actual_size,
            index=index,
        )

        for i, p in enumerate(self.parts):
            if p.number == num:
                self.parts[i] = part
                return

        self.parts.append(part)
        self.parts.sort(key=lambda x: x.number)

    def to_part_offset(self, offset: int) -> Union[tuple[int, int], str]:
        if offset == 0:
            return 0, 0

        part_offset = offset
        for i, part in enumerate(self.parts):
            part_index = i
            if part_offset < part.size:
                return part_index, part_offset

            part_offset -= part.size

        return "part not found"  # Error

    def set_healing(self) -> None:
        self.metadata["x-rustfs-internal-healing"] = "true"

    def set_tier_free_version_id(self, version_id: str) -> None:
        self.metadata[f"x-rustfs-internal-{TIER_FV_ID}"] = version_id

    def tier_free_version_id(self) -> str:
        return self.metadata[f"x-rustfs-internal-{TIER_FV_ID}"]

    def set_tier_free_version(self) -> None:
        self.metadata[f"x-rustfs-internal-{TIER_FV_MARKER}"] = ""

    def set_skip_tier_free_version(self) -> None:
        self.metadata[f"x-rustfs-internal-{TIER_SKIP_FV_ID}"] = ""

    def skip_tier_free_version(self) -> bool:
        return f"x-rustfs-internal-{TIER_SKIP_FV_ID}" in self.metadata

    def tier_free_version(self) -> bool:
        return f"x-rustfs-internal-{TIER_FV_MARKER}" in self.metadata

    def set_inline_data(self) -> None:
        self.metadata["x-rustfs-internal-inline-data"] = "true"

    def set_data_moved(self) -> None:
        self.metadata["x-rustfs-internal-data-moved"] = "true"

    def inline_data(self) -> bool:
        return "x-rustfs-internal-inline-data" in self.metadata and not self.is_remote()

    def is_compressed(self) -> bool:
        return "x-rustfs-internal-compression" in self.metadata

    def is_remote(self) -> bool:
        return bool(self.transition_tier)

    def get_data_dir(self) -> str:
        if self.deleted:
            return "delete-marker"
        return str(self.data_dir) if self.data_dir else ""

    def read_quorum(self, dquorum: int) -> int:
        if self.deleted:
            return dquorum
        return self.erasure.data_blocks

    def shallow_copy(self) -> "FileInfo":
        return FileInfo(
            volume=self.volume,
            name=self.name,
            version_id=self.version_id,
            deleted=self.deleted,
            erasure=self.erasure,
        )

    def equals(self, other: "FileInfo") -> bool:
        if self.is_compressed() != other.is_compressed():
            return False

        if not self.transition_info_equals(other):
            return False

        if self.mod_time != other.mod_time:
            return False

        return self.erasure.equals(other.erasure)

    def transition_info_equals(self, other: "FileInfo") -> bool:
        return (
            self.transition_status == other.transition_status
            and self.transition_tier == other.transition_tier
            and self.transitioned_objname == other.transitioned_objname
            and self.transition_version_id == other.transition_version_id
        )

    def metadata_equals(self, other: "FileInfo") -> bool:
        if len(self.metadata) != len(other.metadata):
            return False
        for k, v in self.metadata.items():
            if other.metadata.get(k) != v:
                return False
        return True

    def replication_info_equals(self, other: "FileInfo") -> bool:
        return self.mark_deleted == other.mark_deleted


@dataclass
class FileInfoVersions:
    volume: str = ""
    name: str = ""
    latest_mod_time: Optional[float] = None  # time.OffsetDateTime
    versions: List[FileInfo] = field(default_factory=list)
    free_versions: List[FileInfo] = field(default_factory=list)

    def find_version_index(self, vid: uuid.UUID) -> Optional[int]:
        for i, v in enumerate(self.versions):
            if v.version_id == vid:
                return i
        return None

    def size(self) -> int:
        return sum(v.size for v in self.versions)


@dataclass
class RawFileInfo:
    buf: bytes = b""


@dataclass
class FilesInfo:
    files: List[FileInfo] = field(default_factory=list)
    is_truncated: bool = False