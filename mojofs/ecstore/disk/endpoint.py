import os
import sys
import socket
import pathlib
from urllib.parse import urlparse, urlunparse, ParseResult
from enum import Enum, auto
from mojofs.ecstore.disk.error import Error
from mojofs.utils import is_socket_addr, is_local_host


class EndpointType(Enum):
    Path = auto()
    Url = auto()

def url_parse_from_file_path(value: str):
    # 检查是否为ip:port格式
    addr = value.split('/', 1)[0]
    if is_socket_addr(addr):
        raise Error(Error.Kind.Unexpected, "invalid URL endpoint format: missing scheme http or https")
    # 绝对路径
    try:
        file_path = str(pathlib.Path(value).absolute())
    except Exception as err:
        raise Error(Error.Kind.Unexpected, f"absolute path failed: {err}")
    # 构造file://url
    if sys.platform.startswith('win'):
        # Windows: file:///C:/path
        if not file_path.startswith('/'):
            file_path = '/' + file_path.replace('\\', '/')
    return ParseResult(scheme='file', netloc='', path=file_path, params='', query='', fragment='')

class Endpoint:
    def __init__(self, url: ParseResult, is_local: bool, pool_idx: int = -1, set_idx: int = -1, disk_idx: int = -1):
        self.url = url
        self.is_local = is_local
        self.pool_idx = pool_idx
        self.set_idx = set_idx
        self.disk_idx = disk_idx

    @classmethod
    def try_from(cls, value: str):
        if value in ("", "/", "\\"):
            raise Error(Error.Kind.Unexpected, "empty or root endpoint is not supported")
        is_local = False
        try:
            url = urlparse(value)
            # URL风格
            if url.scheme and url.netloc:
                # 只允许http/https, 不能有用户名、fragment、query
                if url.scheme not in ("http", "https") or url.username or url.fragment or url.query:
                    raise Error(Error.Kind.Unexpected, "invalid URL endpoint format")
                # 检查host是否为空
                if url.hostname is None or url.hostname == "":
                    raise Error(Error.Kind.Unexpected, "invalid URL endpoint format: empty host name")
                # 检查端口是否合法
                try:
                    port = url.port
                except ValueError as e:
                    # 这里专门捕获端口号超出范围的异常，并抛出自定义错误信息
                    if "out of range" in str(e):
                        raise Error(Error.Kind.Unexpected, "invalid URL endpoint format: port number must be between 1 to 65535")
                    else:
                        raise Error(Error.Kind.Unexpected, f"invalid URL endpoint format: {e}")
                if url.port is not None:
                    if not (1 <= url.port <= 65535):
                        raise Error(Error.Kind.Unexpected, "invalid URL endpoint format: port number must be between 1 to 65535")
                path = url.path
                # 绝对化路径
                if sys.platform.startswith('win'):
                    # windows: 去掉前导斜杠
                    if path.startswith('/'):
                        path = path[1:]
                    abs_path = str(pathlib.Path(path).absolute())
                else:
                    abs_path = str(pathlib.Path(path).absolute())
                # 路径不能为根或空
                if pathlib.Path(abs_path).parent == pathlib.Path(abs_path) or abs_path in ("", "/", "\\"):
                    raise Error(Error.Kind.Unexpected, "empty or root path is not supported in URL endpoint")
                # 构造新的url
                url = url._replace(path=abs_path)
            elif url.scheme == '' and url.netloc == '':
                # 文件路径
                is_local = True
                url = url_parse_from_file_path(value)
            else:
                # 其他情况
                is_local = True
                url = url_parse_from_file_path(value)
        except Error:
            raise
        except Exception as e:
            # 端口号错误
            if "port" in str(e) and "out of range" in str(e):
                raise Error(Error.Kind.Unexpected, "invalid URL endpoint format: port number must be between 1 to 65535")
            raise Error(Error.Kind.Unexpected, f"invalid URL endpoint format: {e}")
        return cls(url, is_local)

    def get_type(self):
        if self.url.scheme == "file":
            return EndpointType.Path
        else:
            return EndpointType.Url

    def set_pool_index(self, idx: int):
        self.pool_idx = int(idx)

    def set_set_index(self, idx: int):
        self.set_idx = int(idx)

    def set_disk_index(self, idx: int):
        self.disk_idx = int(idx)

    def update_is_local(self, local_port: int):
        if self.url.scheme != "file" and self.url.hostname:
            port = self.url.port or 0
            self.is_local = is_local_host(self.url.hostname, port, local_port)
        return True

    def grid_host(self):
        if self.url.hostname:
            if self.url.port:
                return f"{self.url.scheme}://{self.url.hostname}:{self.url.port}"
            else:
                return f"{self.url.scheme}://{self.url.hostname}"
        return ""

    def host_port(self):
        if self.url.hostname:
            if self.url.port:
                return f"{self.url.hostname}:{self.url.port}"
            else:
                return f"{self.url.hostname}"
        return ""

    def get_file_path(self):
        path = self.url.path
        if sys.platform.startswith('win') and self.url.scheme == "file":
            # windows: 去掉前导斜杠
            return path.lstrip('/')
        return path

    def __str__(self):
        if self.url.scheme == "file":
            return self.get_file_path()
        else:
            return urlunparse(self.url)

    def __eq__(self, other):
        if not isinstance(other, Endpoint):
            return False
        return (self.url == other.url and self.is_local == other.is_local and
                self.pool_idx == other.pool_idx and self.set_idx == other.set_idx and
                self.disk_idx == other.disk_idx)

    def __hash__(self):
        return hash((self.url, self.is_local, self.pool_idx, self.set_idx, self.disk_idx))

    def clone(self):
        return Endpoint(self.url, self.is_local, self.pool_idx, self.set_idx, self.disk_idx)

# 单元测试
import unittest

class TestEndpoint(unittest.TestCase):
    def test_new_endpoint(self):
        class TestCase:
            def __init__(self, arg, expected_endpoint, expected_type, expected_err):
                self.arg = arg
                self.expected_endpoint = expected_endpoint
                self.expected_type = expected_type
                self.expected_err = expected_err

        u2 = urlparse("https://example.org/path")
        u4 = urlparse("http://192.168.253.200/path")
        u6 = urlparse("http://server:/path")
        root_slash_foo = url_parse_from_file_path("/foo")

        test_cases = [
            TestCase(
                "/foo",
                Endpoint(root_slash_foo, True, -1, -1, -1),
                EndpointType.Path,
                None
            ),
            TestCase(
                "https://example.org/path",
                Endpoint(u2, False, -1, -1, -1),
                EndpointType.Url,
                None
            ),
            TestCase(
                "http://192.168.253.200/path",
                Endpoint(u4, False, -1, -1, -1),
                EndpointType.Url,
                None
            ),
            TestCase(
                "",
                None,
                None,
                "empty or root endpoint is not supported"
            ),
            TestCase(
                "/",
                None,
                None,
                "empty or root endpoint is not supported"
            ),
            TestCase(
                "\\",
                None,
                None,
                "empty or root endpoint is not supported"
            ),
            TestCase(
                "c://foo",
                None,
                None,
                "invalid URL endpoint format"
            ),
            TestCase(
                "ftp://foo",
                None,
                None,
                "invalid URL endpoint format"
            ),
            TestCase(
                "http://server/path?location",
                None,
                None,
                "invalid URL endpoint format"
            ),
            TestCase(
                "http://:/path",
                None,
                None,
                "invalid URL endpoint format: empty host name"
            ),
            TestCase(
                "http://:8080/path",
                None,
                None,
                "invalid URL endpoint format: empty host name"
            ),
            TestCase(
                "http://server:/path",
                Endpoint(u6, False, -1, -1, -1),
                EndpointType.Url,
                None
            ),
            TestCase(
                "https://93.184.216.34:808080/path",
                None,
                None,
                "invalid URL endpoint format: port number must be between 1 to 65535"
            ),
            TestCase(
                "http://server:8080//",
                None,
                None,
                "empty or root path is not supported in URL endpoint"
            ),
            TestCase(
                "http://server:8080/",
                None,
                None,
                "empty or root path is not supported in URL endpoint"
            ),
            TestCase(
                "192.168.1.210:9000",
                None,
                None,
                "invalid URL endpoint format: missing scheme http or https"
            ),
        ]

        for tc in test_cases:
            if tc.expected_err is None:
                ep = Endpoint.try_from(tc.arg)
                ep.update_is_local(9000)
                self.assertEqual(ep.get_type(), tc.expected_type, f"{tc.arg}: type")
                self.assertEqual(ep, tc.expected_endpoint, f"{tc.arg}: endpoint")
            else:
                try:
                    Endpoint.try_from(tc.arg)
                except Error as e:
                    self.assertIn(tc.expected_err, str(e), f"{tc.arg}: error")
                else:
                    self.fail(f"{tc.arg}: 未抛出 DiskError 异常")

    def test_endpoint_display(self):
        file_endpoint = Endpoint.try_from("/tmp/data")
        self.assertEqual(str(file_endpoint), "/tmp/data")
        url_endpoint = Endpoint.try_from("http://example.com:9000/path")
        self.assertEqual(str(url_endpoint), "http://example.com:9000/path")

    def test_endpoint_type(self):
        file_endpoint = Endpoint.try_from("/tmp/data")
        self.assertEqual(file_endpoint.get_type(), EndpointType.Path)
        url_endpoint = Endpoint.try_from("http://example.com:9000/path")
        self.assertEqual(url_endpoint.get_type(), EndpointType.Url)

    def test_endpoint_indexes(self):
        endpoint = Endpoint.try_from("/tmp/data")
        self.assertEqual(endpoint.pool_idx, -1)
        self.assertEqual(endpoint.set_idx, -1)
        self.assertEqual(endpoint.disk_idx, -1)
        endpoint.set_pool_index(2)
        endpoint.set_set_index(3)
        endpoint.set_disk_index(4)
        self.assertEqual(endpoint.pool_idx, 2)
        self.assertEqual(endpoint.set_idx, 3)
        self.assertEqual(endpoint.disk_idx, 4)

    def test_endpoint_grid_host(self):
        endpoint = Endpoint.try_from("http://example.com:9000/path")
        self.assertEqual(endpoint.grid_host(), "http://example.com:9000")
        endpoint_no_port = Endpoint.try_from("https://example.com/path")
        self.assertEqual(endpoint_no_port.grid_host(), "https://example.com")
        file_endpoint = Endpoint.try_from("/tmp/data")
        self.assertEqual(file_endpoint.grid_host(), "")

    def test_endpoint_host_port(self):
        endpoint = Endpoint.try_from("http://example.com:9000/path")
        self.assertEqual(endpoint.host_port(), "example.com:9000")
        endpoint_no_port = Endpoint.try_from("https://example.com/path")
        self.assertEqual(endpoint_no_port.host_port(), "example.com")
        file_endpoint = Endpoint.try_from("/tmp/data")
        self.assertEqual(file_endpoint.host_port(), "")

    def test_endpoint_get_file_path(self):
        file_endpoint = Endpoint.try_from("/tmp/data")
        self.assertEqual(file_endpoint.get_file_path(), "/tmp/data")
        url_endpoint = Endpoint.try_from("http://example.com:9000/path/to/data")
        self.assertEqual(url_endpoint.get_file_path(), "/path/to/data")

    def test_endpoint_clone_and_equality(self):
        endpoint1 = Endpoint.try_from("/tmp/data")
        endpoint2 = endpoint1.clone()
        self.assertEqual(endpoint1, endpoint2)
        self.assertEqual(endpoint1.url, endpoint2.url)
        self.assertEqual(endpoint1.is_local, endpoint2.is_local)
        self.assertEqual(endpoint1.pool_idx, endpoint2.pool_idx)
        self.assertEqual(endpoint1.set_idx, endpoint2.set_idx)
        self.assertEqual(endpoint1.disk_idx, endpoint2.disk_idx)

    def test_endpoint_with_special_paths(self):
        complex_path = "/var/lib/rustfs/data/bucket1"
        endpoint = Endpoint.try_from(complex_path)
        self.assertEqual(endpoint.get_file_path(), complex_path)
        self.assertTrue(endpoint.is_local)
        self.assertEqual(endpoint.get_type(), EndpointType.Path)

    def test_endpoint_update_is_local(self):
        endpoint = Endpoint.try_from("http://localhost:9000/path")
        self.assertTrue(endpoint.update_is_local(9000))
        file_endpoint = Endpoint.try_from("/tmp/data")
        self.assertTrue(file_endpoint.update_is_local(9000))

    def test_url_parse_from_file_path(self):
        url = url_parse_from_file_path("/tmp/test")
        self.assertEqual(url.scheme, "file")

    def test_endpoint_hash(self):
        endpoint1 = Endpoint.try_from("/tmp/data1")
        endpoint2 = Endpoint.try_from("/tmp/data2")
        endpoint3 = endpoint1.clone()
        s = set()
        s.add(endpoint1)
        s.add(endpoint2)
        s.add(endpoint3)
        self.assertEqual(len(s), 2)

if __name__ == "__main__":
    unittest.main()