import json
import os

# Constants
ENV_ACCESS_KEY = "RUSTFS_ACCESS_KEY"
ENV_SECRET_KEY = "RUSTFS_SECRET_KEY"
ENV_ROOT_USER = "RUSTFS_ROOT_USER"
ENV_ROOT_PASSWORD = "RUSTFS_ROOT_PASSWORD"
RUSTFS_CONFIG_PREFIX = "config"
DEFAULT_DELIMITER = "."  # Assuming this is used as a default delimiter

class ConfigSys:
    def __init__(self):
        pass

    async def init(self, api):
        # Placeholder for async initialization logic
        # In Python, you'd typically use aiohttp or similar for async requests
        # and a database library like asyncpg or aiomysql for database interactions.
        # This is a simplified example.
        cfg = await self.read_config_without_migrate(api)
        self.lookup_configs(cfg, api)
        GLOBAL_SERVER_CONFIG.set(cfg)  # Assuming GLOBAL_SERVER_CONFIG is a global variable
        return None  # Representing Result::Ok(())

    async def read_config_without_migrate(self, api):
        # Placeholder for reading configuration without migration
        # This would involve fetching data from the 'api' (presumably a data store)
        # and converting it into a Config object.
        # Replace this with your actual implementation.
        return Config({})  # Return an empty Config object as a placeholder

    def lookup_configs(self, cfg, api):
        # Placeholder for looking up configurations
        # This would involve querying the 'api' for specific configurations
        # and updating the 'cfg' object accordingly.
        # Replace this with your actual implementation.
        pass


class KV:
    def __init__(self, key, value, hidden_if_empty=False):
        self.key = key
        self.value = value
        self.hidden_if_empty = hidden_if_empty

    def __repr__(self):
        return f"KV(key='{self.key}', value='{self.value}', hidden_if_empty={self.hidden_if_empty})"


class KVS:
    def __init__(self, kvs=None):
        self.kvs = kvs if kvs is not None else []

    def new(self):
        self.kvs = []
        return self

    def get(self, key):
        v = self.lookup(key)
        return v if v else ""

    def lookup(self, key):
        for kv in self.kvs:
            if kv.key == key:
                return kv.value
        return None

    def is_empty(self):
        return not bool(self.kvs)

    def keys(self):
        found_comment = False
        keys = []
        for kv in self.kvs:
            if kv.key == COMMENT_KEY:
                found_comment = True
            keys.append(kv.key)

        if not found_comment:
            keys.append(COMMENT_KEY)
        return keys

    def insert(self, key, value):
        for kv in self.kvs:
            if kv.key == key:
                kv.value = value
                return
        self.kvs.append(KV(key, value))

    def extend(self, other):
        for kv in other.kvs:
            self.insert(kv.key, kv.value)

    def __repr__(self):
        return f"KVS(kvs={self.kvs})"


class Config:
    def __init__(self, data=None):
        self.data = data if data is not None else {}

    def new(self):
        self.data = {}
        self.set_defaults()
        return self

    def get_value(self, sub_sys, key):
        if sub_sys in self.data and key in self.data[sub_sys]:
            return self.data[sub_sys][key]
        else:
            return None

    def set_defaults(self):
        if DEFAULT_KVS.get():
            defaults = DEFAULT_KVS.get()
            for k, v in defaults.items():
                if k not in self.data:
                    self.data[k] = {DEFAULT_DELIMITER: v}
                elif DEFAULT_DELIMITER not in self.data[k]:
                    self.data[k][DEFAULT_DELIMITER] = v

    def unmarshal(self, data):
        m = json.loads(data)
        self.data = m
        self.set_defaults()
        return self

    def marshal(self):
        return json.dumps(self.data).encode('utf-8')

    def merge(self):
        # TODO: merge default
        return Config(self.data.copy())

    def __repr__(self):
        return f"Config(data={self.data})"


# Global variables (simulating LazyLock and OnceLock)
GLOBAL_STORAGE_CLASS = None  # Placeholder
DEFAULT_KVS = type('DEFAULT_KVS', (object,), {'_value': None, 'set': lambda self, value: setattr(self, '_value', value), 'get': lambda self: getattr(self, '_value', None)})()
GLOBAL_SERVER_CONFIG = None  # Placeholder
GLOBAL_CONFIG_SYS = ConfigSys()

COMMENT_KEY = "comment" # Assuming this is defined elsewhere and needed here

def register_default_kvs(kvs):
    DEFAULT_KVS.set(kvs)


def init():
    kvs = {}
    # Load storageclass default configuration
    # Assuming storageclass and notify are modules with DEFAULT_KVS defined
    try:
        import storageclass
        kvs[STORAGE_CLASS_SUB_SYS] = storageclass.DEFAULT_KVS
    except ImportError:
        print("storageclass module not found")

    try:
        import notify
        kvs[NOTIFY_WEBHOOK_SUB_SYS] = notify.DEFAULT_WEBHOOK_KVS
        kvs[NOTIFY_MQTT_SUB_SYS] = notify.DEFAULT_MQTT_KVS
    except ImportError:
        print("notify module not found")

    register_default_kvs(kvs)

# Example usage (replace with actual values)
STORAGE_CLASS_SUB_SYS = "storageclass"
NOTIFY_WEBHOOK_SUB_SYS = "notify_webhook"
NOTIFY_MQTT_SUB_SYS = "notify_mqtt"