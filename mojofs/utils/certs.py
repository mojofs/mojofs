import os
import ssl
from typing import Dict, Tuple, List
import logging
from mojofs.config import RUSTFS_TLS_CERT,RUSTFS_TLS_KEY

logger = logging.getLogger(__name__)

class CertsError(Exception):
    """Custom exception for certificate related errors."""
    pass


def load_certs(filename: str) -> List[str]:
    """Load public certificate from file."""
    try:
        with open(filename, 'r') as cert_file:
            content = cert_file.read()
            if not content.strip():
                raise CertsError(f"No valid certificate was found in the certificate file {filename}")
            
            # Basic validation: check if content looks like a PEM certificate
            if not content.strip().startswith("-----BEGIN CERTIFICATE-----"):
                raise CertsError(f"No valid certificate was found in the certificate file {filename}")
            
            certs = [cert.strip() for cert in content.split("-----BEGIN CERTIFICATE-----") if cert.strip()]
            if not certs:
                raise CertsError(f"No valid certificate was found in the certificate file {filename}")
            return certs
    except FileNotFoundError as e:
        raise CertsError(f"failed to open {filename}: {e}") from e
    except Exception as e:
        raise CertsError(f"certificate file {filename} format error: {e}") from e


def load_private_key(filename: str) -> str:
    """Load private key from file."""
    try:
        with open(filename, 'r') as key_file:
            key = key_file.read().strip()
            if not key:
                raise CertsError(f"no private key found in {filename}")
            
            # Basic validation: check if content looks like a PEM private key
            if not (key.startswith("-----BEGIN PRIVATE KEY-----") or 
                   key.startswith("-----BEGIN RSA PRIVATE KEY-----") or
                   key.startswith("-----BEGIN EC PRIVATE KEY-----")):
                raise CertsError(f"no private key found in {filename}")
            
            return key
    except FileNotFoundError as e:
        raise CertsError(f"failed to open {filename}: {e}") from e
    except Exception as e:
        raise CertsError(f"no private key found in {filename}: {e}") from e


def load_all_certs_from_directory(dir_path: str) -> Dict[str, Tuple[List[str], str]]:
    """
    Load all certificates and private keys in the directory.

    This function loads all certificate and private key pairs from the specified directory.
    It looks for files named `rustfs_cert.pem` and `rustfs_key.pem` in each subdirectory.
    The root directory can also contain a default certificate/private key pair.
    """
    cert_key_pairs: Dict[str, Tuple[List[str], str]] = {}
    
    # Handle edge cases for path validation
    if not dir_path or dir_path.strip() == "":
        raise CertsError(f"Invalid directory path: empty or None path provided")
    
    dir_path_obj = os.path.abspath(dir_path)
    dir = os.path.normpath(dir_path_obj)

    if not os.path.exists(dir) or not os.path.isdir(dir):
        raise CertsError(f"The certificate directory does not exist or is not a directory: {dir_path}")

    # 1. First check whether there is a certificate/private key pair in the root directory
    root_cert_path = os.path.join(dir, RUSTFS_TLS_CERT)
    root_key_path = os.path.join(dir, RUSTFS_TLS_KEY)

    if os.path.exists(root_cert_path) and os.path.exists(root_key_path):
        logger.debug(f"find the root directory certificate: {root_cert_path}")
        try:
            certs, key = load_cert_key_pair(root_cert_path, root_key_path)
            # The root directory certificate is used as the default certificate and is stored using special keys.
            cert_key_pairs["default"] = (certs, key)
        except CertsError as e:
            logger.warning(f"unable to load root directory certificate: {e}")

    # 2. Iterate through all folders in the directory
    try:
        for entry in os.scandir(dir):
            if entry.is_dir():
                domain_name = entry.name

                # find certificate and private key files
                cert_path = os.path.join(dir, domain_name, RUSTFS_TLS_CERT)  # e.g., rustfs_cert.pem
                key_path = os.path.join(dir, domain_name, RUSTFS_TLS_KEY)  # e.g., rustfs_key.pem

                if os.path.exists(cert_path) and os.path.exists(key_path):
                    logger.debug(f"find the domain name certificate: {domain_name} in {cert_path}")
                    try:
                        certs, key = load_cert_key_pair(cert_path, key_path)
                        cert_key_pairs[domain_name] = (certs, key)
                    except CertsError as e:
                        logger.warning(f"unable to load the certificate for {domain_name} domain name: {e}")
    except OSError as e:
        raise CertsError(f"Error reading directory {dir}: {e}") from e

    if not cert_key_pairs:
        raise CertsError(f"No valid certificate/private key pair found in directory {dir_path}")

    return cert_key_pairs


def load_cert_key_pair(cert_path: str, key_path: str) -> Tuple[List[str], str]:
    """
    Loading a single certificate private key pair.

    This function loads a certificate and private key from the specified paths.
    It returns a tuple containing the certificate and private key.
    """
    certs = load_certs(cert_path)
    key = load_private_key(key_path)
    return certs, key


def create_multi_cert_resolver(cert_key_pairs: Dict[str, Tuple[List[str], str]]) -> ssl.SSLContext:
    """
    Create a multi-cert resolver.

    This function loads all certificates and private keys from the specified directory.
    It uses the first certificate/private key pair found in the root directory as the default certificate.
    The rest of the certificates/private keys are used for SNI resolution.
    """
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
    context.set_ecdh_curve('prime256v1')

    default_cert = None

    for domain, (certs, key) in cert_key_pairs.items():
        try:
            if domain == "default":
                # Load default certificate
                cert_pem = '\n'.join(certs)
                context.load_cert_chain(certfile=None, keyfile=None, cert_pem=cert_pem, key_pem=key)
                default_cert = True
            else:
                # Load SNI certificates
                cert_pem = '\n'.join(certs)
                context.load_cert_chain(certfile=None, keyfile=None, cert_pem=cert_pem, key_pem=key)
        except Exception as e:
            raise CertsError(f"Failed to load certificate for {domain}: {e}") from e

    if default_cert is None and cert_key_pairs:
        first_domain = next(iter(cert_key_pairs))
        certs, key = cert_key_pairs[first_domain]
        cert_pem = '\n'.join(certs)
        try:
            context.load_cert_chain(certfile=None, keyfile=None, cert_pem=cert_pem, key_pem=key)
        except Exception as e:
            raise CertsError(f"Failed to load certificate for {first_domain}: {e}") from e

    if not cert_key_pairs:
        try:
            context.load_cert_chain(certfile=None, keyfile=None)
        except Exception as e:
            raise CertsError(f"Failed to load default certificate: {e}") from e

    def sni_callback(conn: ssl.SSLSocket, server_name: str) -> None:
        """SNI callback to select the appropriate certificate."""
        if server_name in cert_key_pairs:
            certs, key = cert_key_pairs[server_name]
            cert_pem = '\n'.join(certs)
            try:
                conn.context().load_cert_chain(certfile=None, keyfile=None, cert_pem=cert_pem, key_pem=key)
            except Exception as e:
                logger.error(f"Failed to load certificate for {server_name}: {e}")

    context.sni_callback = sni_callback
    return context


def tls_key_log() -> bool:
    """Checks if TLS key logging is enabled."""
    keylog = os.environ.get("RUSTFS_TLS_KEYLOG", "").strip().lower()
    return keylog in ("1", "on", "true", "yes")


import unittest
import tempfile

class TestCerts(unittest.TestCase):

    def test_certs_error_function(self):
        error_msg = "Test error message"
        with self.assertRaises(CertsError) as context:
            raise CertsError(error_msg)
        self.assertEqual(str(context.exception), error_msg)

    def test_load_certs_file_not_found(self):
        with self.assertRaises(CertsError) as context:
            load_certs("non_existent_file.pem")
        self.assertTrue("failed to open" in str(context.exception))

    def test_load_private_key_file_not_found(self):
        with self.assertRaises(CertsError) as context:
            load_private_key("non_existent_key.pem")
        self.assertTrue("failed to open" in str(context.exception))

    def test_load_certs_empty_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            cert_path = os.path.join(temp_dir, "empty.pem")
            with open(cert_path, "w") as f:
                f.write("")

            with self.assertRaises(CertsError) as context:
                load_certs(cert_path)
            self.assertTrue("No valid certificate was found" in str(context.exception))

    def test_load_certs_invalid_format(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            cert_path = os.path.join(temp_dir, "invalid.pem")
            with open(cert_path, "w") as f:
                f.write("invalid certificate content")

            with self.assertRaises(CertsError) as context:
                load_certs(cert_path)
            self.assertTrue("No valid certificate was found" in str(context.exception))

    def test_load_private_key_empty_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            key_path = os.path.join(temp_dir, "empty_key.pem")
            with open(key_path, "w") as f:
                f.write("")

            with self.assertRaises(CertsError) as context:
                load_private_key(key_path)
            self.assertTrue("no private key found" in str(context.exception))

    def test_load_private_key_invalid_format(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            key_path = os.path.join(temp_dir, "invalid_key.pem")
            with open(key_path, "w") as f:
                f.write("invalid private key content")

            with self.assertRaises(CertsError) as context:
                load_private_key(key_path)
            self.assertTrue("no private key found" in str(context.exception))

    def test_load_all_certs_from_directory_not_exists(self):
        with self.assertRaises(CertsError) as context:
            load_all_certs_from_directory("/non/existent/directory")
        self.assertTrue("does not exist or is not a directory" in str(context.exception))

    def test_load_all_certs_from_directory_empty(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                load_all_certs_from_directory(temp_dir)
                self.fail("CertsError not raised")
            except CertsError as context:
                self.assertTrue("No valid certificate/private key pair found" in str(context))

    def test_load_all_certs_from_directory_file_instead_of_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "not_a_directory.txt")
            with open(file_path, "w") as f:
                f.write("content")

            with self.assertRaises(CertsError) as context:
                load_all_certs_from_directory(file_path)
        self.assertTrue("does not exist or is not a directory" in str(context.exception))

    def test_load_cert_key_pair_missing_cert(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            key_path = os.path.join(temp_dir, "test_key.pem")
            with open(key_path, "w") as f:
                f.write("-----BEGIN PRIVATE KEY-----\ndummy key content\n-----END PRIVATE KEY-----")

            with self.assertRaises(CertsError) as context:
                load_cert_key_pair("non_existent_cert.pem", key_path)
            self.assertTrue("failed to open" in str(context.exception))

    def test_load_cert_key_pair_missing_key(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            cert_path = os.path.join(temp_dir, "test_cert.pem")
            with open(cert_path, "w") as f:
                f.write("-----BEGIN CERTIFICATE-----\ndummy cert content\n-----END CERTIFICATE-----")

            with self.assertRaises(CertsError) as context:
                load_cert_key_pair(cert_path, "non_existent_key.pem")
            self.assertTrue("failed to open" in str(context.exception))

    def test_create_multi_cert_resolver_empty_map(self):
        with self.assertRaises(CertsError) as context:
            create_multi_cert_resolver({})
        self.assertTrue("Failed to load default certificate" in str(context.exception))

    def test_error_message_formatting(self):
        test_cases = [
            ("file not found", "failed to open test.pem: file not found"),
            ("permission denied", "failed to open key.pem: permission denied"),
            ("invalid format", "certificate file cert.pem format error: invalid format"),
        ]

        for input, _expected_pattern in test_cases:
            with self.assertRaises(CertsError) as context:
                raise CertsError(f"failed to open test.pem: {input}")
            self.assertTrue(input in str(context.exception))

            with self.assertRaises(CertsError) as context:
                raise CertsError(f"failed to open key.pem: {input}")
            self.assertTrue(input in str(context.exception))

    def test_path_handling_edge_cases(self):
        path_cases = [
            "",               # Empty path
            "/non/existent/path",  # Non-existent absolute path
            "relative/non/existent/path",  # Non-existent relative path
        ]

        for path in path_cases:
            try:
                load_all_certs_from_directory(path)
                self.fail("CertsError not raised")
            except CertsError:
                pass

    def test_filename_constants_consistency(self):
        self.assertEqual(os.path.basename(RUSTFS_TLS_CERT), "rustfs_cert.pem")
        self.assertEqual(os.path.basename(RUSTFS_TLS_KEY), "rustfs_key.pem")

        self.assertTrue(RUSTFS_TLS_CERT)
        self.assertTrue(RUSTFS_TLS_KEY)

        self.assertTrue(RUSTFS_TLS_CERT.endswith(".pem"))
        self.assertTrue(RUSTFS_TLS_KEY.endswith(".pem"))

    def test_directory_structure_validation(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            sub_dir = os.path.join(temp_dir, "example.com")
            os.makedirs(sub_dir)

            try:
                load_all_certs_from_directory(temp_dir)
                self.fail("CertsError not raised")
            except CertsError as context:
                self.assertTrue("No valid certificate/private key pair found" in str(context))

    def test_unicode_path_handling(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            unicode_dir = os.path.join(temp_dir, "测试目录")
            os.makedirs(unicode_dir)

            try:
                load_all_certs_from_directory(unicode_dir)
                self.fail("CertsError not raised")
            except CertsError as context:
                self.assertTrue("No valid certificate/private key pair found" in str(context))

    def test_concurrent_access_safety(self):
        import threading

        with tempfile.TemporaryDirectory() as temp_dir:
            dir_path = temp_dir

            def load_certs_from_dir():
                try:
                    load_all_certs_from_directory(dir_path)
                    # If no exception is raised, explicitly fail the test
                    assert False, "CertsError was not raised"
                except CertsError:
                    pass  # Expected exception

            threads = []
            for _ in range(5):
                thread = threading.Thread(target=load_certs_from_dir)
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

    def test_memory_efficiency(self):
        import sys

        error = CertsError("test")
        error_size = sys.getsizeof(error)

        self.assertTrue(error_size < 1024, f"Error size should be reasonable, got {error_size} bytes")

if __name__ == '__main__':
    unittest.main()