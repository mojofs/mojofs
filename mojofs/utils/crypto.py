import base64
import binascii
import hmac
import hashlib
from typing import Callable, List, Any, Union

def base64_encode(input_bytes: bytes) -> str:
    """URL安全无填充的base64编码"""
    return base64.urlsafe_b64encode(input_bytes).rstrip(b'=').decode('ascii')

def base64_decode(input_bytes: bytes) -> bytes:
    """URL安全无填充的base64解码"""
    s = input_bytes.decode('ascii') if isinstance(input_bytes, bytes) else input_bytes
    # 补齐padding
    padding = '=' * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + padding)

def hex_encode(data: Union[bytes, bytearray]) -> str:
    """小写十六进制编码"""
    return binascii.hexlify(data).decode('ascii')

def is_sha256_checksum(s: str) -> bool:
    """校验字符串是否为sha256校验和（64位小写十六进制）"""
    return len(s) == 64 and all(c in '0123456789abcdef' for c in s)

def hmac_sha1(key: Union[bytes, bytearray], data: Union[bytes, bytearray]) -> bytes:
    """hmac_sha1(key, data) 返回20字节结果"""
    return hmac.new(key, data, hashlib.sha1).digest()

def hmac_sha256(key: Union[bytes, bytearray], data: Union[bytes, bytearray]) -> bytes:
    """hmac_sha256(key, data) 返回32字节结果"""
    return hmac.new(key, data, hashlib.sha256).digest()

def hex_bytes32(src: Union[bytes, bytearray], f: Callable[[str], Any]) -> Any:
    """将32字节数据转为小写十六进制字符串后传递给回调f"""
    hex_str = hex_encode(src)
    return f(hex_str)

def sha256(data: Union[bytes, bytearray]) -> bytes:
    """sha256哈希，返回32字节结果"""
    return hashlib.sha256(data).digest()

def sha256_chunk(chunk: List[bytes]) -> bytes:
    """对chunk中的每个bytes顺序update，返回32字节sha256结果"""
    h = hashlib.sha256()
    for data in chunk:
        h.update(data)
    return h.digest()

def hex_sha256(data: Union[bytes, bytearray], f: Callable[[str], Any]) -> Any:
    """f(hex(sha256(data)))"""
    return hex_bytes32(sha256(data), f)

def hex_sha256_chunk(chunk: List[bytes], f: Callable[[str], Any]) -> Any:
    """f(hex(sha256(chunk)))"""
    return hex_bytes32(sha256_chunk(chunk), f)

def test_base64_encoding_decoding():
    original_uuid_timestamp = "c0194290-d911-45cb-8e12-79ec563f46a8x1735460504394878000"
    encoded_string = base64_encode(original_uuid_timestamp.encode('utf-8'))
    print("Encoded:", encoded_string)
    decoded_bytes = base64_decode(encoded_string.encode('ascii'))
    decoded_string = decoded_bytes.decode('utf-8')
    assert decoded_string == original_uuid_timestamp

if __name__ == "__main__":
    test_base64_encoding_decoding()