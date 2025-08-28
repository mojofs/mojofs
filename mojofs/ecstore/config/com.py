import os
import json
import logging
from threading import Lock

from mojofs.ecstore.config import Config, GLOBAL_STORAGE_CLASS
from mojofs.ecstore.config import storageclass
from mojofs.ecstore.config import DEFAULT_DELIMITER

CONFIG_PREFIX = "config"
CONFIG_FILE = "config.json"
STORAGE_CLASS_SUB_SYS = "storage_class"

RUSTFS_META_BUCKET = "meta"  # 假设
SLASH_SEPARATOR = "/"        # 假设

CONFIG_BUCKET = f"{RUSTFS_META_BUCKET}{SLASH_SEPARATOR}{CONFIG_PREFIX}"

# 动态子系统集合
SUB_SYSTEMS_DYNAMIC = set([STORAGE_CLASS_SUB_SYS])

logger = logging.getLogger("ecstore.config.com")

def get_config_file():
    return f"{CONFIG_PREFIX}{SLASH_SEPARATOR}{CONFIG_FILE}"

async def read_config(api, file):
    data, _ = await read_config_with_metadata(api, file, {})
    return data

async def read_config_with_metadata(api, file, opts):
    """
    读取配置文件及其元数据
    """
    try:
        rd = await api.get_object_reader(RUSTFS_META_BUCKET, file, None, {}, opts)
    except Exception as err:
        if getattr(err, "code", None) == "FileNotFound":
            raise FileNotFoundError("ConfigNotFound")
        logger.warning(f"read_config_with_metadata: err: {err}, file: {file}")
        raise
    data = await rd.read_all()
    if not data:
        raise FileNotFoundError("ConfigNotFound")
    return data, getattr(rd, "object_info", {})

async def save_config(api, file, data):
    opts = {"max_parity": True}
    return await save_config_with_opts(api, file, data, opts)

async def delete_config(api, file):
    try:
        await api.delete_object(
            RUSTFS_META_BUCKET,
            file,
            {
                "delete_prefix": True,
                "delete_prefix_object": True
            }
        )
        return True
    except Exception as err:
        if getattr(err, "code", None) == "FileNotFound":
            raise FileNotFoundError("ConfigNotFound")
        raise

async def save_config_with_opts(api, file, data, opts):
    try:
        await api.put_object(RUSTFS_META_BUCKET, file, data, opts)
    except Exception as err:
        logger.error(f"save_config_with_opts: err: {err}, file: {file}")
        raise
    return True

def new_server_config():
    return Config().new()

async def new_and_save_server_config(api):
    cfg = new_server_config()
    await lookup_configs(cfg, api)
    await save_server_config(api, cfg)
    return cfg

async def handle_missing_config(api, context):
    logger.warning(f"Configuration not found ({context}): Start initializing new configuration")
    cfg = await new_and_save_server_config(api)
    logger.warning(f"Configuration initialization complete ({context})")
    return cfg

def handle_config_read_error(err, file_path):
    logger.error(f"Read configuration failed (path: '{file_path}'): {err}")
    raise err

async def read_config_without_migrate(api):
    config_file = get_config_file()
    try:
        data = await read_config(api, config_file)
        return await read_server_config(api, data)
    except FileNotFoundError:
        return await handle_missing_config(api, "Read the main configuration")
    except Exception as err:
        return handle_config_read_error(err, config_file)

async def read_server_config(api, data):
    if not data:
        config_file = get_config_file()
        logger.warning(f"Received empty configuration data, try to reread from '{config_file}'")
        try:
            cfg_data = await read_config(api, config_file)
            cfg = Config().unmarshal(cfg_data)
            return cfg.merge()
        except FileNotFoundError:
            return await handle_missing_config(api, "Read alternate configuration")
        except Exception as err:
            return handle_config_read_error(err, config_file)
    cfg = Config().unmarshal(data)
    return cfg.merge()

async def save_server_config(api, cfg):
    data = cfg.marshal()
    config_file = get_config_file()
    await save_config(api, config_file, data)

async def lookup_configs(cfg, api):
    try:
        await apply_dynamic_config(cfg, api)
    except Exception as err:
        logger.error(f"apply_dynamic_config err {err}")

async def apply_dynamic_config(cfg, api):
    for key in SUB_SYSTEMS_DYNAMIC:
        await apply_dynamic_config_for_sub_sys(cfg, api, key)

async def apply_dynamic_config_for_sub_sys(cfg, api, subsys):
    set_drive_counts = getattr(api, "set_drive_counts", lambda: [])()
    if subsys == STORAGE_CLASS_SUB_SYS:
        kvs = cfg.get_value(STORAGE_CLASS_SUB_SYS, DEFAULT_DELIMITER) or {}
        for i, count in enumerate(set_drive_counts):
            try:
                res = storageclass.lookup_config(kvs, count)
                if i == 0 and getattr(GLOBAL_STORAGE_CLASS, "get", lambda: None)() is None:
                    try:
                        GLOBAL_STORAGE_CLASS.set(res)
                    except Exception as r:
                        logger.error(f"GLOBAL_STORAGE_CLASS.set failed {r}")
            except Exception as err:
                logger.error(f"init storage class err:{err}")
                break