import re
from datetime import timedelta

RUSTFS_BITROT_CYCLE_IN_MONTHS = 1

class Config:
    def __init__(self, bitrot="", sleep=timedelta(), io_count=0, drive_workers=0, cache=timedelta()):
        self.bitrot = bitrot
        self.sleep = sleep
        self.io_count = io_count
        self.drive_workers = drive_workers
        self.cache = cache

    def bitrot_scan_cycle(self):
        return self.cache

    def get_workers(self):
        return self.drive_workers

    def update(self, nopts):
        self.bitrot = nopts.bitrot
        self.io_count = nopts.io_count
        self.sleep = nopts.sleep
        self.drive_workers = nopts.drive_workers

def parse_bool(s):
    """
    尝试将字符串解析为布尔值。
    支持 'true', 'false', '1', '0', 'yes', 'no' 等。
    """
    s = s.strip().lower()
    if s in ("true", "1", "yes", "on"):
        return True
    if s in ("false", "0", "no", "off"):
        return False
    raise ValueError("无法解析为布尔值")

class ConfigError(Exception):
    pass

def parse_bitrot_config(s):
    """
    解析bitrot配置字符串，返回timedelta对象。
    支持布尔值或以'm'结尾的月份数。
    """
    try:
        enabled = parse_bool(s)
        if enabled:
            return timedelta(seconds=0)
        else:
            # 用-1秒表示禁用
            return timedelta(seconds=-1)
    except ValueError:
        if not s.endswith("m"):
            raise ConfigError("unknown format")
        try:
            months = int(s[:-1])
            if months < RUSTFS_BITROT_CYCLE_IN_MONTHS:
                raise ConfigError(f"minimum bitrot cycle is {RUSTFS_BITROT_CYCLE_IN_MONTHS} month(s)")
            # 1个月按30天计算
            return timedelta(minutes=months * 30 * 24 * 60)
        except Exception as err:
            raise ConfigError(str(err))