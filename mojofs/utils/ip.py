import socket
import ipaddress
import unittest

def get_local_ip():
    """
    获取本机的IP地址，优先返回IPv4，如果失败则尝试IPv6。
    如果都无法获取，返回None。
    :return: str 或 None
    """
    # 优先尝试获取IPv4地址
    try:
        # 通过UDP连接一个外部地址（不需要真的连通），获取本地IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
            if ip and not ip.startswith("127."):
                return ip
        finally:
            s.close()
    except Exception:
        pass

    # 尝试获取IPv6地址
    try:
        s = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        try:
            s.connect(('2001:4860:4860::8888', 80))
            ip = s.getsockname()[0]
            if ip and not ip == '::1':
                return ip
        finally:
            s.close()
    except Exception:
        pass

    # 最后尝试通过主机名获取
    try:
        hostname = socket.gethostname()
        for family in (socket.AF_INET, socket.AF_INET6):
            try:
                for info in socket.getaddrinfo(hostname, None, family, socket.SOCK_DGRAM):
                    addr = info[4][0]
                    if family == socket.AF_INET and not addr.startswith("127."):
                        return addr
                    if family == socket.AF_INET6 and addr != '::1':
                        return addr
            except Exception:
                continue
    except Exception:
        pass

    return None

def get_local_ip_with_default():
    """
    获取本机IP地址字符串，获取失败时返回"127.0.0.1"。
    :return: str
    """
    ip = get_local_ip()
    return ip if ip else "127.0.0.1"

class TestLocalIP(unittest.TestCase):
    def test_get_local_ip_returns_some_ip(self):
        ip = get_local_ip()
        self.assertTrue(ip is None or isinstance(ip, str))
        if ip:
            print(f"Local IP address: {ip}")
            try:
                ip_obj = ipaddress.ip_address(ip)
                self.assertFalse(ip_obj.is_unspecified)
                print(f"Got IP address: {ip_obj}")
            except Exception:
                self.fail("Returned IP is not a valid IP address")

    def test_get_local_ip_with_default_never_empty(self):
        ip_string = get_local_ip_with_default()
        self.assertTrue(isinstance(ip_string, str))
        self.assertNotEqual(ip_string, "")
        try:
            ip_obj = ipaddress.ip_address(ip_string)
        except Exception:
            self.fail(f"Returned string should be a valid IP address: {ip_string}")
        print(f"Local IP with default: {ip_string}")

    def test_get_local_ip_with_default_fallback(self):
        ip_string = get_local_ip_with_default()
        if get_local_ip() is None:
            self.assertEqual(ip_string, "127.0.0.1")
        try:
            ip_obj = ipaddress.ip_address(ip_string)
        except Exception:
            self.fail("Should always return a valid IP string")

    def test_ip_address_types(self):
        ip = get_local_ip()
        if ip:
            ip_obj = ipaddress.ip_address(ip)
            print(f"IP address: {ip_obj}")
            if isinstance(ip_obj, ipaddress.IPv4Address):
                self.assertFalse(ip_obj.is_multicast)
                self.assertFalse(ip_obj.is_unspecified)
                print(f"IPv4 is private: {ip_obj.is_private}, is loopback: {ip_obj.is_loopback}")
            elif isinstance(ip_obj, ipaddress.IPv6Address):
                self.assertFalse(ip_obj.is_multicast)
                print(f"IPv6 is loopback: {ip_obj.is_loopback}")

    def test_ip_string_format(self):
        ip_string = get_local_ip_with_default()
        self.assertNotIn(' ', ip_string)
        self.assertNotEqual(ip_string, "")
        ip_obj = ipaddress.ip_address(ip_string)
        back_to_string = str(ip_obj)
        print(f"Original: {ip_string}, Parsed back: {back_to_string}")

    def test_default_fallback_value(self):
        default_ip = "127.0.0.1"
        self.assertEqual(default_ip, "127.0.0.1")
        ip_obj = ipaddress.ip_address(default_ip)
        self.assertTrue(ip_obj.is_loopback)
        self.assertFalse(ip_obj.is_unspecified)
        self.assertFalse(ip_obj.is_multicast)

    def test_consistency_between_functions(self):
        ip_option = get_local_ip()
        ip_string = get_local_ip_with_default()
        if ip_option:
            self.assertEqual(ip_option, ip_string)
        else:
            self.assertEqual(ip_string, "127.0.0.1")

    def test_multiple_calls_consistency(self):
        ip1 = get_local_ip()
        ip2 = get_local_ip()
        ip_str1 = get_local_ip_with_default()
        ip_str2 = get_local_ip_with_default()
        self.assertEqual(ip1, ip2)
        self.assertEqual(ip_str1, ip_str2)

if __name__ == "__main__":
    unittest.main()