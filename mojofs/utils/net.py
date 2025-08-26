import socket
import ipaddress
import threading
from urllib.parse import urlparse, urlunparse
from typing import Set, Optional, Tuple, Union, List

# 缓存本地IP地址
_local_ips_lock = threading.Lock()
_local_ips: Optional[Set[str]] = None

def _get_local_ips() -> Set[str]:
    global _local_ips
    with _local_ips_lock:
        if _local_ips is not None:
            return _local_ips
        ips = set()
        try:
            # Add all IPs from all interfaces
            for iface in socket.if_nameindex():
                try:
                    for fam, _, _, _, sockaddr in socket.getaddrinfo(socket.gethostname(), None):
                        if fam == socket.AF_INET:
                            ips.add(sockaddr[0])
                        elif fam == socket.AF_INET6:
                            ips.add(sockaddr[0].split('%')[0])
                except Exception:
                    continue
            # Add loopback addresses
            ips.add('127.0.0.1')
            ips.add('::1')
        except Exception:
            pass
        _local_ips = ips
        return ips

def is_socket_addr(addr: str) -> bool:
    """
    判断字符串是否为合法的IP地址或Socket地址
    """
    try:
        # Try IPv4/IPv6 address
        ipaddress.ip_address(addr)
        return True
    except ValueError:
        pass
    # Try host:port or [ipv6]:port
    try:
        if addr.startswith('['):
            # [ipv6]:port
            host, port = addr.rsplit(']:', 1)
            host = host[1:]
        else:
            if ':' not in addr:
                return False
            host, port = addr.rsplit(':', 1)
        # Validate port
        if not port.isdigit() or not (0 <= int(port) <= 65535):
            return False
        # Validate host
        try:
            ipaddress.ip_address(host)
            return True
        except ValueError:
            return False
    except Exception:
        return False

def check_local_server_addr(server_addr: str) -> Tuple[str, int]:
    """
    检查server_addr是否为本地地址，返回(host, port)
    """
    # 解析host和port
    if server_addr.startswith('['):
        # [ipv6]:port
        try:
            host, port = server_addr.rsplit(']:', 1)
            host = host[1:]
        except Exception:
            raise ValueError("invalid socket address")
    else:
        if ':' not in server_addr:
            raise ValueError("invalid socket address")
        try:
            host, port = server_addr.rsplit(':', 1)
        except Exception:
            raise ValueError("invalid socket address")
    try:
        port = int(port)
    except Exception:
        raise ValueError("invalid port value")
    if not (0 <= port <= 65535):
        raise ValueError("invalid port value")
    # 0.0.0.0 或 :: 是通配本地
    if host in ('0.0.0.0', '::'):
        return (host, port)
    # 检查是否本地
    # Accept also "localhost" as local, and resolve it to a local IP
    try:
        # Try to resolve host to IPs
        resolved_ips = set()
        try:
            ipaddress.ip_address(host)
            resolved_ips.add(host)
        except ValueError:
            # Not an IP, try DNS
            infos = socket.getaddrinfo(host, None)
            for info in infos:
                resolved_ips.add(info[4][0].split('%')[0])
        # Check if any resolved IP is in local IPs
        local_ips = _get_local_ips()
        for ip in resolved_ips:
            if ip in local_ips:
                return (host, port)
    except Exception:
        pass
    raise ValueError("host in server address should be this server")

def is_local_host(host: str, port: int = 0, local_port: int = 0) -> bool:
    """
    检查host是否为本地IP
    """
    local_ips = _get_local_ips()
    try:
        # 域名解析
        try:
            ip = ipaddress.ip_address(host)
            is_local = str(ip) in local_ips
        except ValueError:
            # 不是IP，尝试解析域名
            try:
                infos = socket.getaddrinfo(host, None)
                is_local = any(info[4][0].split('%')[0] in local_ips for info in infos)
            except Exception:
                return False
        if port > 0:
            return is_local and port == local_port
        return is_local
    except Exception:
        return False

def get_host_ip(host: str) -> Set[str]:
    """
    返回host解析到的所有IP
    """
    try:
        # 如果是IP直接返回
        try:
            ipaddress.ip_address(host)
            return {host}
        except ValueError:
            pass
        # 域名解析
        infos = socket.getaddrinfo(host, None)
        return {info[4][0].split('%')[0] for info in infos}
    except Exception as e:
        raise ValueError(f"get_host_ip error: {e}")

def get_available_port() -> int:
    """
    获取一个可用端口
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('0.0.0.0', 0))
    port = s.getsockname()[1]
    s.close()
    return port

def must_get_local_ips() -> List[str]:
    """
    返回本地所有IP
    """
    return list(_get_local_ips())

def get_default_location(u, region_override: str) -> str:
    raise NotImplementedError()

def get_endpoint_url(endpoint: str, secure: bool) -> str:
    """
    返回endpoint的url字符串
    """
    scheme = "https" if secure else "http"
    url = f"{scheme}://{endpoint}"
    # 检查url合法性
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("url parse error.")
    return url

DEFAULT_DIAL_TIMEOUT = 5

ALLOWED_CUSTOM_QUERY_PREFIX = "x-"

def is_custom_query_value(qs_key: str) -> bool:
    return qs_key.startswith(ALLOWED_CUSTOM_QUERY_PREFIX)

class XHost:
    def __init__(self, name: str, port: int, is_port_set: bool):
        self.name = name
        self.port = port
        self.is_port_set = is_port_set

    def __str__(self):
        if not self.is_port_set:
            return self.name
        elif ':' in self.name:
            return f"[{self.name}]:{self.port}"
        else:
            return f"{self.name}:{self.port}"

    @classmethod
    def try_from(cls, value: str):
        try:
            if value.startswith('['):
                # [ipv6]:port
                host, port = value.rsplit(']:', 1)
                host = host[1:]
            else:
                if ':' not in value:
                    raise ValueError("invalid socket address")
                host, port = value.rsplit(':', 1)
            port = int(port)
            if not (0 <= port <= 65535):
                raise ValueError("invalid port value")
            # 解析host
            try:
                ipaddress.ip_address(host)
                name = host
            except ValueError:
                # 域名解析
                infos = socket.getaddrinfo(host, None)
                name = infos[0][4][0].split('%')[0]
            is_port_set = port > 0
            return cls(name, port, is_port_set)
        except Exception:
            raise ValueError("value invalid")

def parse_and_resolve_address(addr_str: str) -> Tuple[str, int]:
    """
    解析并返回(host, port)
    """
    try:
        if addr_str.startswith(':'):
            port_str = addr_str[1:]
            port = int(port_str)
            if port == 0:
                port = get_available_port()
            return ("::", port)
        else:
            host, port = check_local_server_addr(addr_str)
            if port == 0:
                port = get_available_port()
            return (host, port)
    except Exception as e:
        raise ValueError(str(e))

# bytes_stream 的Python实现通常依赖于异步生成器
import asyncio

async def bytes_stream(stream, content_length: int):
    """
    限制总字节数的异步字节流生成器
    :param stream: 异步可迭代对象，yield bytes
    :param content_length: 限制总字节数
    """
    remaining = content_length
    async for chunk in stream:
        if len(chunk) > remaining:
            chunk = chunk[:remaining]
        remaining -= len(chunk)
        yield chunk
        if remaining <= 0:
            break

# 测试代码
import unittest

class TestNetUtils(unittest.TestCase):
    def test_is_socket_addr(self):
        test_cases = [
            ("192.168.1.0", True),
            ("127.0.0.1", True),
            ("10.0.0.1", True),
            ("0.0.0.0", True),
            ("255.255.255.255", True),
            ("2001:db8::1", True),
            ("::1", True),
            ("::", True),
            ("fe80::1", True),
            ("192.168.1.0:8080", True),
            ("127.0.0.1:9000", True),
            ("[2001:db8::1]:9000", True),
            ("[::1]:8080", True),
            ("0.0.0.0:0", True),
            ("localhost", False),
            ("localhost:9000", False),
            ("example.com", False),
            ("example.com:8080", False),
            ("http://192.168.1.0", False),
            ("http://192.168.1.0:9000", False),
            ("256.256.256.256", False),
            ("192.168.1", False),
            ("192.168.1.0.1", False),
            ("", False),
            (":", False),
            (":::", False),
            ("invalid_ip", False),
        ]
        for addr, expected in test_cases:
            result = is_socket_addr(addr)
            self.assertEqual(expected, result, f"addr: '{addr}', expected: {expected}, got: {result}")

    def test_check_local_server_addr(self):
        valid_cases = ["localhost:54321", "127.0.0.1:9000", "0.0.0.0:9000", "[::1]:8080", "::1:8080"]
        for addr in valid_cases:
            try:
                host, port = check_local_server_addr(addr)
                self.assertTrue(True)
            except Exception as e:
                self.fail(f"Expected '{addr}' to be valid, but got error: {e}")

        invalid_cases = [
            ("localhost", "invalid socket address"),
            ("", "invalid socket address"),
            ("example.org:54321", "host in server address should be this server"),
            ("8.8.8.8:53", "host in server address should be this server"),
            (":-10", "invalid port value"),
            ("invalid:port", "invalid port value"),
        ]
        for addr, expected_error_pattern in invalid_cases:
            with self.assertRaises(Exception) as cm:
                check_local_server_addr(addr)
            error_msg = str(cm.exception)
            self.assertTrue(expected_error_pattern in error_msg or "invalid socket address" in error_msg,
                            f"Error message '{error_msg}' doesn't contain expected pattern '{expected_error_pattern}' for address '{addr}'")

    def test_is_local_host(self):
        self.assertTrue(is_local_host("localhost"))
        self.assertTrue(is_local_host("127.0.0.1"))
        self.assertTrue(is_local_host("::1"))
        # 端口匹配
        self.assertTrue(is_local_host("localhost", 8080, 8080))
        self.assertFalse(is_local_host("localhost", 8080, 9000))
        # 非本地
        self.assertFalse(is_local_host("8.8.8.8"))
        # 错误域名
        self.assertFalse(is_local_host("invalid.nonexistent.domain.example"))

    def test_get_host_ip(self):
        self.assertEqual(get_host_ip("192.168.1.1"), {"192.168.1.1"})
        self.assertEqual(get_host_ip("::1"), {"::1"})
        # localhost 至少包含127.0.0.1或::1
        ips = get_host_ip("localhost")
        self.assertTrue("127.0.0.1" in ips or "::1" in ips)
        # 错误域名
        with self.assertRaises(Exception):
            get_host_ip("invalid.nonexistent.domain.example")

    def test_get_available_port(self):
        port1 = get_available_port()
        port2 = get_available_port()
        self.assertTrue(port1 > 0)
        self.assertTrue(port2 > 0)
        self.assertNotEqual(port1, port2)

    def test_must_get_local_ips(self):
        local_ips = must_get_local_ips()
        self.assertTrue("127.0.0.1" in local_ips or "::1" in local_ips)
        self.assertTrue(len(local_ips) > 0)
        for ip in local_ips:
            ipaddress.ip_address(ip)  # 不抛异常即合法

    def test_xhost_display(self):
        host_no_port = XHost("example.com", 0, False)
        self.assertEqual(str(host_no_port), "example.com")
        host_with_port = XHost("192.168.1.1", 8080, True)
        self.assertEqual(str(host_with_port), "192.168.1.1:8080")
        host_ipv6_with_port = XHost("2001:db8::1", 9000, True)
        self.assertEqual(str(host_ipv6_with_port), "[2001:db8::1]:9000")
        host_domain_with_port = XHost("example.com", 443, True)
        self.assertEqual(str(host_domain_with_port), "example.com:443")

    def test_xhost_try_from(self):
        result = XHost.try_from("192.168.1.1:8080")
        self.assertEqual(result.name, "192.168.1.1")
        self.assertEqual(result.port, 8080)
        self.assertTrue(result.is_port_set)
        result = XHost.try_from("192.168.1.1:0")
        self.assertEqual(result.name, "192.168.1.1")
        self.assertEqual(result.port, 0)
        self.assertFalse(result.is_port_set)
        result = XHost.try_from("[2001:db8::1]:9000")
        self.assertEqual(result.name, "2001:db8::1")
        self.assertEqual(result.port, 9000)
        self.assertTrue(result.is_port_set)
        result = XHost.try_from("localhost:3000")
        self.assertIn(result.name, ["127.0.0.1", "::1"])
        self.assertEqual(result.port, 3000)
        self.assertTrue(result.is_port_set)
        with self.assertRaises(Exception):
            XHost.try_from("invalid_format")
        with self.assertRaises(Exception):
            XHost.try_from("")

    def test_parse_and_resolve_address(self):
        host, port = parse_and_resolve_address(":8080")
        self.assertEqual(host, "::")
        self.assertEqual(port, 8080)
        host, port = parse_and_resolve_address(":0")
        self.assertEqual(host, "::")
        self.assertTrue(port > 0)
        host, port = parse_and_resolve_address("localhost:9000")
        self.assertEqual(port, 9000)
        host, port = parse_and_resolve_address("localhost:0")
        self.assertTrue(port > 0)
        host, port = parse_and_resolve_address("0.0.0.0:7000")
        self.assertEqual(host, "0.0.0.0")
        self.assertEqual(port, 7000)
        with self.assertRaises(Exception):
            parse_and_resolve_address(":invalid_port")
        with self.assertRaises(Exception):
            parse_and_resolve_address("example.org:8080")

    def test_edge_cases(self):
        self.assertFalse(is_socket_addr(""))
        self.assertFalse(is_socket_addr(":"))
        self.assertFalse(is_socket_addr("[::]"))
        self.assertFalse(is_socket_addr("[::1"))
        long_string = "a" * 1000
        self.assertFalse(is_socket_addr(long_string))
        self.assertFalse(is_socket_addr("测试.example.com"))
        self.assertFalse(is_socket_addr("test@example.com:8080"))
        self.assertFalse(is_socket_addr("http://example.com:8080"))

    def test_boundary_values(self):
        self.assertTrue(is_socket_addr("127.0.0.1:0"))
        self.assertTrue(is_socket_addr("127.0.0.1:65535"))
        self.assertFalse(is_socket_addr("127.0.0.1:65536"))
        self.assertTrue(is_socket_addr("0.0.0.0"))
        self.assertTrue(is_socket_addr("255.255.255.255"))
        self.assertFalse(is_socket_addr("256.0.0.0"))
        self.assertFalse(is_socket_addr("0.0.0.256"))
        host_max_port = XHost("example.com", 65535, True)
        self.assertEqual(str(host_max_port), "example.com:65535")
        host_zero_port = XHost("example.com", 0, True)
        self.assertEqual(str(host_zero_port), "example.com:0")

if __name__ == "__main__":
    unittest.main()