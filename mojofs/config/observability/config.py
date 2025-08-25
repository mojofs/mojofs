import os
from dataclasses import dataclass
from typing import Optional


# Environment variable keys
ENV_OBS_ENDPOINT = "RUSTFS_OBS_ENDPOINT"
ENV_OBS_USE_STDOUT = "RUSTFS_OBS_USE_STDOUT"
ENV_OBS_SAMPLE_RATIO = "RUSTFS_OBS_SAMPLE_RATIO"
ENV_OBS_METER_INTERVAL = "RUSTFS_OBS_METER_INTERVAL"
ENV_OBS_SERVICE_NAME = "RUSTFS_OBS_SERVICE_NAME"
ENV_OBS_SERVICE_VERSION = "RUSTFS_OBS_SERVICE_VERSION"
ENV_OBS_ENVIRONMENT = "RUSTFS_OBS_ENVIRONMENT"
ENV_OBS_LOGGER_LEVEL = "RUSTFS_OBS_LOGGER_LEVEL"
ENV_OBS_LOCAL_LOGGING_ENABLED = "RUSTFS_OBS_LOCAL_LOGGING_ENABLED"
ENV_OBS_LOG_DIRECTORY = "RUSTFS_OBS_LOG_DIRECTORY"
ENV_OBS_LOG_FILENAME = "RUSTFS_OBS_LOG_FILENAME"
ENV_OBS_LOG_ROTATION_SIZE_MB = "RUSTFS_OBS_LOG_ROTATION_SIZE_MB"
ENV_OBS_LOG_ROTATION_TIME = "RUSTFS_OBS_LOG_ROTATION_TIME"
ENV_OBS_LOG_KEEP_FILES = "RUSTFS_OBS_LOG_KEEP_FILES"

ENV_AUDIT_LOGGER_QUEUE_CAPACITY = "RUSTFS_AUDIT_LOGGER_QUEUE_CAPACITY"

# Default values
DEFAULT_AUDIT_LOGGER_QUEUE_CAPACITY = 10000


def _parse_bool(value: Optional[str], default: Optional[bool] = None) -> Optional[bool]:
    if value is None or value == "":
        return default
    lowered = value.strip().lower()
    if lowered in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _parse_int(value: Optional[str], default: Optional[int] = None) -> Optional[int]:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _parse_float(value: Optional[str], default: Optional[float] = None) -> Optional[float]:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except ValueError:
        return default