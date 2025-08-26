import os
import sys

GLOBAL_DIR_SUFFIX = "__XLDIR__"
SLASH_SEPARATOR = "/"
GLOBAL_DIR_SUFFIX_WITH_SLASH = "__XLDIR__/"

def has_suffix(s: str, suffix: str) -> bool:
    if sys.platform.startswith("win"):
        return s.lower().endswith(suffix.lower())
    else:
        return s.endswith(suffix)

def encode_dir_object(obj: str) -> str:
    if has_suffix(obj, SLASH_SEPARATOR):
        return obj.rstrip(SLASH_SEPARATOR) + GLOBAL_DIR_SUFFIX
    else:
        return obj

def is_dir_object(obj: str) -> bool:
    return encode_dir_object(obj).endswith(GLOBAL_DIR_SUFFIX)

def decode_dir_object(obj: str) -> str:
    if has_suffix(obj, GLOBAL_DIR_SUFFIX):
        return obj.rstrip(GLOBAL_DIR_SUFFIX) + SLASH_SEPARATOR
    else:
        return obj

def retain_slash(s: str) -> str:
    if not s:
        return s
    if s.endswith(SLASH_SEPARATOR):
        return s
    else:
        return s + SLASH_SEPARATOR

def strings_has_prefix_fold(s: str, prefix: str) -> bool:
    return len(s) >= len(prefix) and (s[:len(prefix)] == prefix or s[:len(prefix)].lower() == prefix.lower())

def has_prefix(s: str, prefix: str) -> bool:
    if sys.platform.startswith("win"):
        return strings_has_prefix_fold(s, prefix)
    return s.startswith(prefix)

def path_join(elem):
    # elem: List[os.PathLike]
    joined_path = os.path.join(*[str(e) for e in elem])
    return joined_path

def path_join_buf(elements):
    # elements: List[str]
    trailing_slash = bool(elements) and elements[-1].endswith(SLASH_SEPARATOR)
    dst = ""
    added = 0
    for e in elements:
        if added > 0 or e:
            if added > 0:
                dst += SLASH_SEPARATOR
            dst += e
            added += len(e)
    # 清理路径
    clean_path = os.path.normpath(dst)
    if trailing_slash and not clean_path.endswith(SLASH_SEPARATOR):
        clean_path += SLASH_SEPARATOR
    return clean_path

def path_to_bucket_object_with_base_path(base_path: str, path: str):
    path = path[len(base_path):] if path.startswith(base_path) else path
    path = path.lstrip(SLASH_SEPARATOR)
    idx = path.find(SLASH_SEPARATOR)
    if idx != -1:
        return path[:idx], path[idx + len(SLASH_SEPARATOR):]
    return path, ""

def path_to_bucket_object(s: str):
    return path_to_bucket_object_with_base_path("", s)

def base_dir_from_prefix(prefix: str) -> str:
    base_dir = dir(prefix)
    if base_dir in (".", "./", "/"):
        base_dir = ""
    if "/" not in prefix:
        base_dir = ""
    if base_dir and not base_dir.endswith(SLASH_SEPARATOR):
        base_dir += SLASH_SEPARATOR
    return base_dir

class LazyBuf:
    def __init__(self, s: str):
        self.s = s
        self.buf = None
        self.w = 0

    def index(self, i: int) -> int:
        if self.buf is not None:
            return self.buf[i]
        else:
            return ord(self.s[i])

    def append(self, c: int):
        if self.buf is None:
            if self.w < len(self.s) and ord(self.s[self.w]) == c:
                self.w += 1
                return
            new_buf = [ord(ch) for ch in self.s]
            self.buf = new_buf
        if self.buf is not None:
            if self.w < len(self.buf):
                self.buf[self.w] = c
            else:
                self.buf.append(c)
            self.w += 1

    def string(self) -> str:
        if self.buf is not None:
            return ''.join(chr(b) for b in self.buf[:self.w])
        else:
            return self.s[:self.w]

def clean(path: str) -> str:
    if not path:
        return "."
    rooted = path.startswith('/')
    n = len(path)
    out = LazyBuf(path)
    r = 0
    dotdot = 0
    if rooted:
        out.append(ord('/'))
        r = 1
        dotdot = 1
    while r < n:
        c = path[r]
        if c == '/':
            r += 1
        elif c == '.' and (r + 1 == n or path[r + 1] == '/'):
            r += 1
        elif (c == '.' and r + 1 < n and path[r + 1] == '.' and (r + 2 == n or path[r + 2] == '/')):
            r += 2
            if out.w > dotdot:
                out.w -= 1
                while out.w > dotdot and out.index(out.w) != ord('/'):
                    out.w -= 1
            elif not rooted:
                if out.w > 0:
                    out.append(ord('/'))
                out.append(ord('.'))
                out.append(ord('.'))
                dotdot = out.w
        else:
            if (rooted and out.w != 1) or (not rooted and out.w != 0):
                out.append(ord('/'))
            while r < n and path[r] != '/':
                out.append(ord(path[r]))
                r += 1
    if out.w == 0:
        return "."
    return out.string()

def split(path: str):
    i = path.rfind('/')
    if i != -1:
        return path[:i+1], path[i+1:]
    return path, ""

def dir(path: str) -> str:
    a, _ = split(path)
    return clean(a)

def trim_etag(etag: str) -> str:
    return etag.strip('"')

# 测试代码
if __name__ == "__main__":
    def test_base_dir_from_prefix():
        a = "da/"
        result = base_dir_from_prefix(a)
        assert result != ""

    def test_clean():
        assert clean("") == "."
        assert clean("abc") == "abc"
        assert clean("abc/def") == "abc/def"
        assert clean("a/b/c") == "a/b/c"
        assert clean(".") == "."
        assert clean("..") == ".."
        assert clean("../..") == "../.."
        assert clean("../../abc") == "../../abc"
        assert clean("/abc") == "/abc"
        assert clean("/") == "/"
        assert clean("abc/") == "abc"
        assert clean("abc/def/") == "abc/def"
        assert clean("a/b/c/") == "a/b/c"
        assert clean("./") == "."
        assert clean("../") == ".."
        assert clean("../../") == "../.."
        assert clean("/abc/") == "/abc"
        assert clean("abc//def//ghi") == "abc/def/ghi"
        assert clean("//abc") == "/abc"
        assert clean("///abc") == "/abc"
        assert clean("//abc//") == "/abc"
        assert clean("abc//") == "abc"
        assert clean("abc/./def") == "abc/def"
        assert clean("/./abc/def") == "/abc/def"
        assert clean("abc/.") == "abc"
        assert clean("abc/./../def") == "def"
        assert clean("abc//./../def") == "def"
        assert clean("abc/../../././../def") == "../../def"
        assert clean("abc/def/ghi/../jkl") == "abc/def/jkl"
        assert clean("abc/def/../ghi/../jkl") == "abc/jkl"
        assert clean("abc/def/..") == "abc"
        assert clean("abc/def/../..") == "."
        assert clean("/abc/def/../..") == "/"
        assert clean("abc/def/../../..") == ".."
        assert clean("/abc/def/../../..") == "/"
        assert clean("abc/def/../../../ghi/jkl/../../../mno") == "../../mno"

    test_base_dir_from_prefix()
    test_clean()