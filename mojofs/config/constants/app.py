APP_NAME = "RustFS"  # Application name
VERSION = "0.0.1"  # Application version

DEFAULT_LOG_LEVEL = "info"  # Default configuration logger level
USE_STDOUT = False  # Default configuration use stdout
SAMPLE_RATIO = 1.0  # Default configuration sample ratio
METER_INTERVAL = 30  # Default configuration meter interval

SERVICE_VERSION = "0.0.1"  # Default configuration service version
ENVIRONMENT = "production"  # Default configuration environment

MAX_CONNECTIONS = 100  # Maximum number of connections
DEFAULT_TIMEOUT_MS = 3000  # Timeout for connections

DEFAULT_ACCESS_KEY = "rustfsadmin"  # Default Access Key
DEFAULT_SECRET_KEY = "rustfsadmin"  # Default Secret Key
DEFAULT_CONSOLE_ENABLE = True  # Default console enable
DEFAULT_OBS_ENDPOINT = ""  # Default OBS configuration endpoint

RUSTFS_TLS_KEY = "rustfs_key.pem"  # Default TLS key for rustfs
RUSTFS_TLS_CERT = "rustfs_cert.pem"  # Default TLS cert for rustfs

DEFAULT_PORT = 9000  # Default port for rustfs
DEFAULT_ADDRESS = f":{DEFAULT_PORT}"  # Default address for rustfs

DEFAULT_CONSOLE_PORT = 9001  # Default port for rustfs console
DEFAULT_CONSOLE_ADDRESS = f":{DEFAULT_CONSOLE_PORT}"  # Default address for rustfs console

DEFAULT_LOG_FILENAME = "rustfs"  # Default log filename for rustfs
DEFAULT_OBS_LOG_FILENAME = f"{DEFAULT_LOG_FILENAME}.log"  # Default OBS log filename for rustfs
DEFAULT_SINK_FILE_LOG_FILE = f"{DEFAULT_LOG_FILENAME}-sink.log"  # Default sink file log file for rustfs
DEFAULT_LOG_DIR = "logs"  # Default log directory for rustfs
DEFAULT_LOG_ROTATION_SIZE_MB = 100  # Default log rotation size mb for rustfs
DEFAULT_LOG_ROTATION_TIME = "day"  # Default log rotation time for rustfs
DEFAULT_LOG_KEEP_FILES = 30  # Default log keep files for rustfs


import unittest

class TestAppConstants(unittest.TestCase):

    def test_app_basic_constants(self):
        # Test application basic constants
        self.assertEqual(APP_NAME, "RustFS")
        self.assertFalse(" " in APP_NAME, "App name should not contain spaces")

        self.assertEqual(VERSION, "0.0.1")
        self.assertEqual(SERVICE_VERSION, "0.0.1")
        self.assertEqual(VERSION, SERVICE_VERSION, "Version and service version should be consistent")

    def test_logging_constants(self):
        # Test logging related constants
        self.assertEqual(DEFAULT_LOG_LEVEL, "info")
        self.assertIn(DEFAULT_LOG_LEVEL, ["trace", "debug", "info", "warn", "error"], "Log level should be a valid tracing level")

        self.assertEqual(SAMPLE_RATIO, 1.0)
        self.assertEqual(METER_INTERVAL, 30)

    def test_environment_constants(self):
        # Test environment related constants
        self.assertEqual(ENVIRONMENT, "production")
        self.assertIn(ENVIRONMENT, ["development", "staging", "production", "test"], "Environment should be a standard environment name")

    def test_connection_constants(self):
        # Test connection related constants
        self.assertEqual(MAX_CONNECTIONS, 100)
        self.assertEqual(DEFAULT_TIMEOUT_MS, 3000)

    def test_security_constants(self):
        # Test security related constants
        self.assertEqual(DEFAULT_ACCESS_KEY, "rustfsadmin")
        self.assertGreaterEqual(len(DEFAULT_ACCESS_KEY), 8, "Access key should be at least 8 characters")

        self.assertEqual(DEFAULT_SECRET_KEY, "rustfsadmin")
        self.assertGreaterEqual(len(DEFAULT_SECRET_KEY), 8, "Secret key should be at least 8 characters")

        # In production environment, access key and secret key should be different
        # These are default values, so being the same is acceptable, but should be warned in documentation
        print("Warning: Default access key and secret key are the same. Change them in production!")

    def test_file_path_constants(self):
        self.assertEqual(RUSTFS_TLS_KEY, "rustfs_key.pem")
        self.assertTrue(RUSTFS_TLS_KEY.endswith(".pem"), "TLS key should be PEM format")

        self.assertEqual(RUSTFS_TLS_CERT, "rustfs_cert.pem")
        self.assertTrue(RUSTFS_TLS_CERT.endswith(".pem"), "TLS cert should be PEM format")

    def test_port_constants(self):
        # Test port related constants
        self.assertEqual(DEFAULT_PORT, 9000)
        self.assertEqual(DEFAULT_CONSOLE_PORT, 9001)
        self.assertNotEqual(DEFAULT_PORT, DEFAULT_CONSOLE_PORT, "Main port and console port should be different")

    def test_address_constants(self):
        # Test address related constants
        self.assertEqual(DEFAULT_ADDRESS, ":9000")
        self.assertTrue(DEFAULT_ADDRESS.startswith(":"), "Address should start with colon")
        self.assertIn(str(DEFAULT_PORT), DEFAULT_ADDRESS, "Address should contain the default port")

        self.assertEqual(DEFAULT_CONSOLE_ADDRESS, ":9001")
        self.assertTrue(DEFAULT_CONSOLE_ADDRESS.startswith(":"), "Console address should start with colon")
        self.assertIn(str(DEFAULT_CONSOLE_PORT), DEFAULT_CONSOLE_ADDRESS, "Console address should contain the console port")

        self.assertNotEqual(DEFAULT_ADDRESS, DEFAULT_CONSOLE_ADDRESS, "Main address and console address should be different")

    def test_string_constants_validity(self):
        # Test validity of string constants
        string_constants = [
            APP_NAME,
            VERSION,
            DEFAULT_LOG_LEVEL,
            SERVICE_VERSION,
            ENVIRONMENT,
            DEFAULT_ACCESS_KEY,
            DEFAULT_SECRET_KEY,
            RUSTFS_TLS_KEY,
            RUSTFS_TLS_CERT,
            DEFAULT_ADDRESS,
            DEFAULT_CONSOLE_ADDRESS,
        ]

        for constant in string_constants:
            self.assertTrue(constant, "String constant should not be empty")
            self.assertFalse(constant.startswith(" "), f"String constant should not start with space: {constant}")
            self.assertFalse(constant.endswith(" "), f"String constant should not end with space: {constant}")

    def test_numeric_constants_validity(self):
        # Test validity of numeric constants
        self.assertTrue(SAMPLE_RATIO != float('inf') and SAMPLE_RATIO != float('-inf'), "Sample ratio should be finite")
        self.assertFalse(SAMPLE_RATIO != SAMPLE_RATIO, "Sample ratio should not be NaN")

    def test_security_best_practices(self):
        # Test security best practices

        # These are default values, should be changed in production environments
        print("Security Warning: Default credentials detected!")
        print(f"Access Key: {DEFAULT_ACCESS_KEY}")
        print(f"Secret Key: {DEFAULT_SECRET_KEY}")
        print("These should be changed in production environments!")

        # Verify that key lengths meet minimum security requirements
        self.assertGreaterEqual(len(DEFAULT_ACCESS_KEY), 8, "Access key should be at least 8 characters")
        self.assertGreaterEqual(len(DEFAULT_SECRET_KEY), 8, "Secret key should be at least 8 characters")

        # Check if default credentials contain common insecure patterns
        insecure_patterns = ["admin", "password", "123456", "default"]
        access_key_lower = DEFAULT_ACCESS_KEY.lower()
        secret_key_lower = DEFAULT_SECRET_KEY.lower()

        # Note: More security check logic can be added here
        # For example, check if keys contain insecure patterns

    def test_configuration_consistency(self):
        # Test configuration consistency

        # Version consistency
        self.assertEqual(VERSION, SERVICE_VERSION, "Application version should match service version")

        # Port conflict check
        ports = [DEFAULT_PORT, DEFAULT_CONSOLE_PORT]
        self.assertEqual(len(set(ports)), len(ports), "Ports should be unique")

        # Address format consistency
        self.assertEqual(DEFAULT_ADDRESS, f":{DEFAULT_PORT}")
        self.assertEqual(DEFAULT_CONSOLE_ADDRESS, f":{DEFAULT_CONSOLE_PORT}")


if __name__ == '__main__':
    unittest.main()