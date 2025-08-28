import asyncio
import threading
from typing import List, Dict, Optional, Any, Callable
from dataclasses import dataclass
import uuid

# Constants
DISK_ASSUME_UNKNOWN_SIZE: int = 1 << 30
DISK_MIN_INODES: int = 1000
DISK_FILL_FRACTION: float = 0.99
DISK_RESERVE_FRACTION: float = 0.15


# Global Variables
GLOBAL_RUSTFS_PORT: int = None
GLOBAL_OBJECT_API: Any = None  # Replace Any with the correct type for ECStore
GLOBAL_LOCAL_DISK: List[Optional[Any]] = []  # Replace Any with the correct type for DiskStore
GLOBAL_IsErasure: bool = False
GLOBAL_IsDistErasure: bool = False
GLOBAL_IsErasureSD: bool = False
GLOBAL_LOCAL_DISK_MAP: Dict[str, Optional[Any]] = {}  # Replace Any with the correct type for DiskStore
GLOBAL_LOCAL_DISK_SET_DRIVES: List[List[List[Optional[Any]]]] = []  # Replace Any with the correct type for DiskStore
GLOBAL_Endpoints: Any = None  # Replace Any with the correct type for EndpointServerPools
GLOBAL_RootDiskThreshold: int = 0
GLOBAL_TierConfigMgr: Any = None  # Replace Any with the correct type for TierConfigMgr
GLOBAL_LifecycleSys: Any = None  # Replace Any with the correct type for LifecycleSys
GLOBAL_EventNotifier: Any = None  # Replace Any with the correct type for EventNotifier
globalDeploymentIDPtr: uuid.UUID = None
GLOBAL_BOOT_TIME: Any = None  # Replace Any with the correct type for SystemTime
GLOBAL_LocalNodeName: str = "127.0.0.1:9000"
GLOBAL_LocalNodeNameHex: str = "" # needs rustfs_utils equivalent
GLOBAL_NodeNamesHex: Dict[str, None] = {}
GLOBAL_REGION: str = None

# Background services cancellation token
GLOBAL_BACKGROUND_SERVICES_CANCEL_TOKEN: threading.Event = threading.Event()

# Credentials
@dataclass
class Credentials:
    access_key: str
    secret_key: str

GLOBAL_ACTIVE_CRED: Optional[Credentials] = None


def init_global_action_cred(ak: Optional[str], sk: Optional[str]) -> None:
    global GLOBAL_ACTIVE_CRED
    ak = ak if ak is not None else "default_access_key"  # Replace with rustfs_utils equivalent
    sk = sk if sk is not None else "default_secret_key"  # Replace with rustfs_utils equivalent

    GLOBAL_ACTIVE_CRED = Credentials(access_key=ak, secret_key=sk)


def get_global_action_cred() -> Optional[Credentials]:
    return GLOBAL_ACTIVE_CRED


def global_rustfs_port() -> int:
    global GLOBAL_RUSTFS_PORT
    if GLOBAL_RUSTFS_PORT is not None:
        return GLOBAL_RUSTFS_PORT
    else:
        return 9000  # Replace with rustfs_config.DEFAULT_PORT


def set_global_rustfs_port(value: int) -> None:
    global GLOBAL_RUSTFS_PORT
    GLOBAL_RUSTFS_PORT = value


def set_global_deployment_id(id: uuid.UUID) -> None:
    global globalDeploymentIDPtr
    globalDeploymentIDPtr = id


def get_global_deployment_id() -> Optional[str]:
    global globalDeploymentIDPtr
    if globalDeploymentIDPtr:
        return str(globalDeploymentIDPtr)
    else:
        return None


def set_global_endpoints(eps: List[Any]) -> None:  # Replace Any with the correct type for PoolEndpoints
    global GLOBAL_Endpoints
    GLOBAL_Endpoints = eps  # Replace EndpointServerPools.from(eps) with the correct conversion if needed


def get_global_endpoints() -> List[Any]:  # Replace Any with the correct type for PoolEndpoints
    global GLOBAL_Endpoints
    if GLOBAL_Endpoints:
        return GLOBAL_Endpoints
    else:
        return []  # Replace EndpointServerPools.default() with the correct default value


def new_object_layer_fn() -> Optional[Any]:  # Replace Any with the correct type for ECStore
    global GLOBAL_OBJECT_API
    return GLOBAL_OBJECT_API


async def set_object_layer(o: Any) -> None:  # Replace Any with the correct type for ECStore
    global GLOBAL_OBJECT_API
    GLOBAL_OBJECT_API = o


async def is_dist_erasure() -> bool:
    global GLOBAL_IsDistErasure
    return GLOBAL_IsDistErasure


async def is_erasure_sd() -> bool:
    global GLOBAL_IsErasureSD
    return GLOBAL_IsErasureSD


async def is_erasure() -> bool:
    global GLOBAL_IsErasure
    return GLOBAL_IsErasure


async def update_erasure_type(setup_type: str) -> None:  # Replace SetupType with str or Enum if needed
    global GLOBAL_IsErasure, GLOBAL_IsDistErasure, GLOBAL_IsErasureSD

    GLOBAL_IsErasure = setup_type == "Erasure"  # Replace SetupType.Erasure with the correct string or Enum value
    GLOBAL_IsDistErasure = setup_type == "DistErasure"  # Replace SetupType.DistErasure with the correct string or Enum value

    if GLOBAL_IsDistErasure:
        GLOBAL_IsErasure = True

    GLOBAL_IsErasureSD = setup_type == "ErasureSD"  # Replace SetupType.ErasureSD with the correct string or Enum value


# def is_legacy() -> bool:
#     if GLOBAL_Endpoints:
#         return len(GLOBAL_Endpoints) == 1 and GLOBAL_Endpoints[0].legacy
#     else:
#         return False


TypeLocalDiskSetDrives = List[List[List[Optional[Any]]]]  # Replace Any with the correct type for DiskStore


def set_global_region(region: str) -> None:
    global GLOBAL_REGION
    GLOBAL_REGION = region


def get_global_region() -> Optional[str]:
    global GLOBAL_REGION
    return GLOBAL_REGION


def init_background_services_cancel_token(cancel_token: threading.Event) -> None:
    global GLOBAL_BACKGROUND_SERVICES_CANCEL_TOKEN
    GLOBAL_BACKGROUND_SERVICES_CANCEL_TOKEN = cancel_token


def get_background_services_cancel_token() -> threading.Event:
    global GLOBAL_BACKGROUND_SERVICES_CANCEL_TOKEN
    return GLOBAL_BACKGROUND_SERVICES_CANCEL_TOKEN


def create_background_services_cancel_token() -> threading.Event:
    cancel_token = threading.Event()
    init_background_services_cancel_token(cancel_token)
    return cancel_token


def shutdown_background_services() -> None:
    global GLOBAL_BACKGROUND_SERVICES_CANCEL_TOKEN
    GLOBAL_BACKGROUND_SERVICES_CANCEL_TOKEN.set()