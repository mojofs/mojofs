from .arn import *
from .mqtt import *
from .store import *
from .webhook import *

# --- Configuration Constants ---
DEFAULT_TARGET = "1"

NOTIFY_PREFIX = "notify"

NOTIFY_ROUTE_PREFIX = NOTIFY_PREFIX+"_"

NOTIFY_MQTT_SUB_SYS = "notify_mqtt"
NOTIFY_WEBHOOK_SUB_SYS = "notify_webhook"

NOTIFY_SUB_SYSTEMS = [NOTIFY_MQTT_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS]

NOTIFY_KAFKA_SUB_SYS = "notify_kafka"
NOTIFY_MY_SQL_SUB_SYS = "notify_mysql"
NOTIFY_NATS_SUB_SYS = "notify_nats"
NOTIFY_NSQ_SUB_SYS = "notify_nsq"
NOTIFY_ES_SUB_SYS = "notify_elasticsearch"
NOTIFY_AMQP_SUB_SYS = "notify_amqp"
NOTIFY_POSTGRES_SUB_SYS = "notify_postgres"
NOTIFY_REDIS_SUB_SYS = "notify_redis"