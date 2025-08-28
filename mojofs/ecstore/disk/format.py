import json
import uuid
from enum import Enum
from typing import List, Optional, Tuple, Any
from mojofs.ecstore.disk.error import DiskError,Error 


class FormatMetaVersion(str, Enum):
    V1 = "1"
    Unknown = "unknown"

    @staticmethod
    def from_str(s: str):
        if s == "1":
            return FormatMetaVersion.V1
        return FormatMetaVersion.Unknown


class FormatBackend(str, Enum):
    Erasure = "xl"
    ErasureSingle = "xl-single"
    Unknown = "unknown"

    @staticmethod
    def from_str(s: str):
        if s == "xl":
            return FormatBackend.Erasure
        if s == "xl-single":
            return FormatBackend.ErasureSingle
        return FormatBackend.Unknown


class FormatErasureVersion(str, Enum):
    V1 = "1"
    V2 = "2"
    V3 = "3"
    Unknown = "unknown"

    @staticmethod
    def from_str(s: str):
        if s == "1":
            return FormatErasureVersion.V1
        if s == "2":
            return FormatErasureVersion.V2
        if s == "3":
            return FormatErasureVersion.V3
        return FormatErasureVersion.Unknown


class DistributionAlgoVersion(str, Enum):
    V1 = "CRCMOD"
    V2 = "SIPMOD"
    V3 = "SIPMOD+PARITY"

    @staticmethod
    def from_str(s: str):
        if s == "CRCMOD":
            return DistributionAlgoVersion.V1
        if s == "SIPMOD":
            return DistributionAlgoVersion.V2
        if s == "SIPMOD+PARITY":
            return DistributionAlgoVersion.V3
        raise ValueError(f"Unknown DistributionAlgoVersion: {s}")

class DiskInfo:
    # Placeholder for DiskInfo, as in the Rust code it's only Option<DiskInfo>
    pass


class FormatErasureV3:
    def __init__(
        self,
        version: FormatErasureVersion,
        this: uuid.UUID,
        sets: List[List[uuid.UUID]],
        distribution_algo: DistributionAlgoVersion,
    ):
        self.version = version
        self.this = this
        self.sets = sets
        self.distribution_algo = distribution_algo

    @staticmethod
    def from_dict(d: dict):
        version = FormatErasureVersion.from_str(d.get("version", "unknown"))
        this_val = d.get("this")
        if this_val is None:
            this = uuid.UUID(int=0)
        else:
            this = uuid.UUID(this_val)
        sets = [
            [uuid.UUID(u) for u in set_list]
            for set_list in d.get("sets", [])
        ]
        distribution_algo = DistributionAlgoVersion.from_str(d.get("distributionAlgo", "CRCMOD"))
        return FormatErasureV3(version, this, sets, distribution_algo)

    def to_dict(self):
        return {
            "version": self.version.value,
            "this": str(self.this) if self.this.int != 0 else None,
            "sets": [[str(u) for u in set_list] for set_list in self.sets],
            "distributionAlgo": self.distribution_algo.value,
        }


class FormatV3:
    def __init__(
        self,
        version: FormatMetaVersion,
        format: FormatBackend,
        id: uuid.UUID,
        erasure: FormatErasureV3,
        disk_info: Optional[DiskInfo] = None,
    ):
        self.version = version
        self.format = format
        self.id = id
        self.erasure = erasure
        self.disk_info = disk_info

    @staticmethod
    def new(num_sets: int, set_len: int):
        format_type = FormatBackend.ErasureSingle if set_len == 1 else FormatBackend.Erasure
        erasure = FormatErasureV3(
            version=FormatErasureVersion.V3,
            this=uuid.UUID(int=0),
            sets=[
                [uuid.uuid4() for _ in range(set_len)]
                for _ in range(num_sets)
            ],
            distribution_algo=DistributionAlgoVersion.V3,
        )
        return FormatV3(
            version=FormatMetaVersion.V1,
            format=format_type,
            id=uuid.uuid4(),
            erasure=erasure,
            disk_info=None,
        )

    def drives(self) -> int:
        return sum(len(v) for v in self.erasure.sets)

    def to_json(self) -> str:
        d = {
            "version": self.version.value,
            "format": self.format.value,
            "id": str(self.id),
            "xl": self.erasure.to_dict(),
        }
        return json.dumps(d)

    @staticmethod
    def try_from(data: Any):
        try:
            if isinstance(data, bytes):
                d = json.loads(data.decode("utf-8"))
            elif isinstance(data, str):
                d = json.loads(data)
            else:
                raise ValueError("Unsupported input type for try_from")
            version = FormatMetaVersion.from_str(d.get("version", "unknown"))
            format_type = FormatBackend.from_str(d.get("format", "unknown"))
            id_val = d.get("id")
            id_uuid = uuid.UUID(id_val) if id_val else uuid.uuid4()
            erasure = FormatErasureV3.from_dict(d.get("xl", {}))
            return FormatV3(version, format_type, id_uuid, erasure)
        except Exception as e:
            raise Error(str(e))

    def find_disk_index_by_disk_id(self, disk_id: uuid.UUID) -> Tuple[int, int]:
        if disk_id.int == 0:
            raise DiskError(DiskError.DiskNotFound)
        if disk_id.int == (1 << 128) - 1:
            raise Error.other("disk offline")
        for i, set_ in enumerate(self.erasure.sets):
            for j, d in enumerate(set_):
                if disk_id == d:
                    return (i, j)
        raise Error.other(f"disk id not found {disk_id}")

    def check_other(self, other: "FormatV3"):
        import copy
        tmp = copy.deepcopy(other)
        this = tmp.erasure.this
        tmp.erasure.this = uuid.UUID(int=0)

        if len(self.erasure.sets) != len(other.erasure.sets):
            raise Error.other(
                f"Expected number of sets {len(self.erasure.sets)}, got {len(other.erasure.sets)}"
            )
        for i in range(len(self.erasure.sets)):
            if len(self.erasure.sets[i]) != len(other.erasure.sets[i]):
                raise Error.other(
                    f"Each set should be of same size, expected {len(self.erasure.sets[i])}, got {len(other.erasure.sets[i])}"
                )
            for j in range(len(self.erasure.sets[i])):
                if self.erasure.sets[i][j] != other.erasure.sets[i][j]:
                    raise Error.other(
                        f"UUID on positions {i}:{j} do not match with, expected {self.erasure.sets[i][j]} got {other.erasure.sets[i][j]}"
                    )
        for i in range(len(tmp.erasure.sets)):
            for j in range(len(tmp.erasure.sets[i])):
                if this == tmp.erasure.sets[i][j]:
                    return
        raise Error.other(
            f"DriveID {this} not found in any drive sets {other.erasure.sets}"
        )