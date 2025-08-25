import platform
import sys
from typing import Optional
from mojofs.config import VERSION

class ServiceType:
    BASIS = "basis"
    CORE = "core"
    EVENT = "event"
    LOGGER = "logger"
    CUSTOM = "custom"

    @classmethod
    def as_str(cls, service_type):
        if service_type == cls.BASIS:
            return "basis"
        elif service_type == cls.CORE:
            return "core"
        elif service_type == cls.EVENT:
            return "event"
        elif service_type == cls.LOGGER:
            return "logger"
        elif isinstance(service_type, str):
            return service_type
        else:
            raise ValueError("Invalid service type")


class UserAgent:
    def __init__(self, service_type):
        self.os_platform = self._get_os_platform()
        self.arch = platform.machine()
        self.version = VERSION
        self.service_type = service_type

    def _get_os_platform(self):
        if sys.platform == "win32":
            return self._get_windows_platform()
        elif sys.platform == "darwin":
            return self._get_macos_platform()
        elif sys.platform.startswith("linux"):
            return self._get_linux_platform()
        else:
            return "Unknown"

    def _get_windows_platform(self):
        return f"Windows NT {platform.version()}"

    def _get_macos_platform(self):
        version = platform.mac_ver()[0].split('.')
        major = version[0] if len(version) > 0 else "10"
        minor = version[1] if len(version) > 1 else "0"
        patch = version[2] if len(version) > 2 else "0"

        cpu_info = "Apple" if platform.machine() == "arm64" else "Intel"
        return f"Macintosh; {cpu_info} Mac OS X {major}_{minor}_{patch}"

    def _get_linux_platform(self):
        try:
            # Attempt to get a more descriptive Linux distribution name
            import distro
            return f"X11; {distro.name()} {distro.version()}"
        except ImportError:
            # Fallback to a generic Linux identifier if distro is not available
            return "X11; Linux Unknown"

    def __str__(self):
        if self.service_type == ServiceType.BASIS:
            return f"Mozilla/5.0 ({self.os_platform}; {self.arch}) RustFS/{self.version}"
        else:
            return f"Mozilla/5.0 ({self.os_platform}; {self.arch}) RustFS/{self.version} ({ServiceType.as_str(self.service_type)})"


def get_user_agent(service_type):
    return str(UserAgent(service_type))


if __name__ == '__main__':
    # Example Usage
    print(get_user_agent(ServiceType.BASIS))
    print(get_user_agent(ServiceType.CORE))
    print(get_user_agent(ServiceType.EVENT))
    print(get_user_agent(ServiceType.LOGGER))
    print(get_user_agent("monitor"))  # Custom service type

    import unittest

    class TestUserAgent(unittest.TestCase):
        def test_user_agent_format_basis(self):
            ua = get_user_agent(ServiceType.BASIS)
            self.assertTrue(ua.startswith("Mozilla/5.0"))
            self.assertTrue(f"RustFS/{VERSION}" in ua)

        def test_user_agent_format_core(self):
            ua = get_user_agent(ServiceType.CORE)
            self.assertTrue(ua.startswith("Mozilla/5.0"))
            self.assertTrue(f"RustFS/{VERSION} (core)" in ua)

        def test_user_agent_format_event(self):
            ua = get_user_agent(ServiceType.EVENT)
            self.assertTrue(ua.startswith("Mozilla/5.0"))
            self.assertTrue(f"RustFS/{VERSION} (event)" in ua)

        def test_user_agent_format_logger(self):
            ua = get_user_agent(ServiceType.LOGGER)
            self.assertTrue(ua.startswith("Mozilla/5.0"))
            self.assertTrue(f"RustFS/{VERSION} (logger)" in ua)

        def test_user_agent_format_custom(self):
            ua = get_user_agent("monitor")
            self.assertTrue(ua.startswith("Mozilla/5.0"))
            self.assertTrue(f"RustFS/{VERSION} (monitor)" in ua)

        def test_all_service_type(self):
            ua_core = get_user_agent(ServiceType.CORE)
            ua_event = get_user_agent(ServiceType.EVENT)
            ua_logger = get_user_agent(ServiceType.LOGGER)
            ua_custom = get_user_agent("monitor")

            print(f"Core User-Agent: {ua_core}")
            print(f"Event User-Agent: {ua_event}")
            print(f"Logger User-Agent: {ua_logger}")
            print(f"Custom User-Agent: {ua_custom}")

    unittest.main(argv=['first-arg-is-ignored'], exit=False)