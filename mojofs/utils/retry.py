import asyncio
import random
import time
from typing import Optional, List

MAX_RETRY = 10
MAX_JITTER = 1.0
NO_JITTER = 0.0

DEFAULT_RETRY_UNIT = 0.2  # 秒
DEFAULT_RETRY_CAP = 1.0   # 秒

RETRYABLE_S3CODES = [
    "RequestError",
    "RequestTimeout",
    "Throttling",
    "ThrottlingException",
    "RequestLimitExceeded",
    "RequestThrottled",
    "InternalError",
    "ExpiredToken",
    "ExpiredTokenException",
    "SlowDown",
]

RETRYABLE_HTTP_STATUSCODES = [
    408,  # REQUEST_TIMEOUT
    429,  # TOO_MANY_REQUESTS
    # 499, # 客户端关闭请求，部分云厂商自定义
    500,  # INTERNAL_SERVER_ERROR
    502,  # BAD_GATEWAY
    503,  # SERVICE_UNAVAILABLE
    504,  # GATEWAY_TIMEOUT
    # 520, # Web服务器返回未知错误，部分云厂商自定义
]

class RetryTimer:
    """
    一个异步重试计时器，支持指数退避和抖动
    """
    def __init__(self, max_retry: int = MAX_RETRY, base_sleep: float = DEFAULT_RETRY_UNIT, max_sleep: float = DEFAULT_RETRY_CAP, jitter: float = MAX_JITTER, random_seed: Optional[int] = None):
        self.base_sleep = base_sleep
        self.max_sleep = max_sleep
        self.jitter = max(NO_JITTER, min(jitter, MAX_JITTER))
        self.max_retry = max_retry
        self.rem = max_retry
        self.random = random.Random(random_seed)
        self._attempt = 0

    def _next_sleep(self):
        # 指数退避
        sleep = self.base_sleep * (2 ** self._attempt)
        if sleep > self.max_sleep:
            sleep = self.max_sleep
        if self.jitter > NO_JITTER:
            # 抖动: 在[ sleep*(1-jitter), sleep ]之间随机
            min_sleep = sleep * (1.0 - self.jitter)
            sleep = self.random.uniform(min_sleep, sleep)
        return sleep

    def __aiter__(self):
        # 注意：__aiter__ 应该是同步方法，返回 self
        return self

    async def __anext__(self):
        if self.rem <= 0:
            raise StopAsyncIteration
        sleep = self._next_sleep()
        await asyncio.sleep(sleep)
        self.rem -= 1
        self._attempt += 1
        return

def is_s3code_retryable(s3code: str) -> bool:
    """
    判断S3错误码是否可重试
    """
    return s3code in RETRYABLE_S3CODES

def is_http_status_retryable(http_statuscode: int) -> bool:
    """
    判断HTTP状态码是否可重试
    """
    return http_statuscode in RETRYABLE_HTTP_STATUSCODES

def is_request_error_retryable(err: Exception) -> bool:
    """
    判断请求异常是否可重试
    这里只做简单实现，实际可根据需要扩展
    """
    # 可以根据异常类型、内容等进一步判断
    # 例如 asyncio.TimeoutError, ConnectionError 等
    # 这里只是示例
    return isinstance(err, (asyncio.TimeoutError, ConnectionError))

# 测试代码
if __name__ == "__main__":
    import sys

    async def test_retry():
        req_retry = 10
        random_seed = random.randint(0, 100)
        retry_timer = RetryTimer(req_retry, DEFAULT_RETRY_UNIT, DEFAULT_RETRY_CAP, MAX_JITTER, random_seed)
        print(f"retry_timer: {retry_timer.__dict__}")
        async for _ in retry_timer:
            print(f"\ntime: {int(time.time() * 1000)}")

    asyncio.run(test_retry())