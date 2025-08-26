import re
import random
import string
import base64
from typing import List, Any, Optional, Union

class PatternError(Exception):
    pass

def parse_bool(s: str) -> bool:
    true_set = {"1", "t", "T", "true", "TRUE", "True", "on", "ON", "On", "enabled"}
    false_set = {"0", "f", "F", "false", "FALSE", "False", "off", "OFF", "Off", "disabled"}
    if s in true_set:
        return True
    elif s in false_set:
        return False
    else:
        raise PatternError(f"ParseBool: parsing {s}")

def deep_match_rune(s: bytes, pattern: bytes, simple: bool) -> bool:
    while pattern:
        c = chr(pattern[0])
        if c == '*':
            if len(pattern) == 1:
                return True
            else:
                return (deep_match_rune(s, pattern[1:], simple) or
                        (s and deep_match_rune(s[1:], pattern, simple)))
        elif c == '?':
            if not s:
                return simple
        else:
            if not s or s[0] != pattern[0]:
                return False
        s = s[1:]
        pattern = pattern[1:]
    return not s and not pattern

def match_simple(pattern: str, name: str) -> bool:
    if pattern == "":
        return name == pattern
    if pattern == "*":
        return True
    return deep_match_rune(name.encode(), pattern.encode(), True)

def match_pattern(pattern: str, name: str) -> bool:
    if pattern == "":
        return name == pattern
    if pattern == "*":
        return True
    return deep_match_rune(name.encode(), pattern.encode(), False)

def has_pattern(patterns: List[str], match_str: str) -> bool:
    for pattern in patterns:
        if match_simple(pattern, match_str):
            return True
    return False

def has_string_suffix_in_slice(s: str, lst: List[str]) -> bool:
    s = s.lower()
    for v in lst:
        if v == "*":
            return True
        if s.endswith(v.lower()):
            return True
    return False

def match_as_pattern_prefix(pattern: str, text: str) -> bool:
    i = 0
    while i < len(text) and i < len(pattern):
        c = pattern[i]
        if c == '*':
            return True
        elif c == '?':
            i += 1
        else:
            if pattern[i] != text[i]:
                return False
        i += 1
    return len(text) <= len(pattern)

ELLIPSES_RE = re.compile(r"(.*)(\{[0-9a-z]*\.\.\.[0-9a-z]*\})(.*)")

OPEN_BRACES = "{"
CLOSE_BRACES = "}"
ELLIPSES = "..."

class Pattern:
    def __init__(self, prefix: str = "", suffix: str = "", seq: Optional[List[str]] = None):
        self.prefix = prefix
        self.suffix = suffix
        self.seq = seq or []

    def expand(self) -> List[str]:
        ret = []
        for v in self.seq:
            if self.prefix and not self.suffix:
                ret.append(f"{self.prefix}{v}")
            elif not self.prefix and self.suffix:
                ret.append(f"{v}{self.suffix}")
            elif not self.prefix and not self.suffix:
                ret.append(str(v))
            else:
                ret.append(f"{self.prefix}{v}{self.suffix}")
        return ret

    def __len__(self):
        return len(self.seq)

    def is_empty(self):
        return not self.seq

    def __eq__(self, other):
        if not isinstance(other, Pattern):
            return False
        return self.prefix == other.prefix and self.suffix == other.suffix and self.seq == other.seq

    def __repr__(self):
        return f"Pattern(prefix={self.prefix!r}, suffix={self.suffix!r}, seq={self.seq!r})"

class ArgPattern:
    def __init__(self, inner: List[Pattern]):
        self.inner = inner

    def expand(self) -> List[List[str]]:
        ret = [v.expand() for v in self.inner]
        return self.arg_expander(ret)

    @staticmethod
    def arg_expander(lbs: List[List[str]]) -> List[List[str]]:
        if len(lbs) == 1:
            return [[v] for v in lbs[0]]
        ret = []
        first, *others = lbs
        for bs in first:
            ots = ArgPattern.arg_expander(others)
            for obs in ots:
                ret.append(obs + [bs])
        return ret

    def total_sizes(self) -> int:
        acc = 1
        for v in self.inner:
            acc *= len(v.seq)
        return acc

    def __eq__(self, other):
        if not isinstance(other, ArgPattern):
            return False
        return self.inner == other.inner

    def __repr__(self):
        return f"ArgPattern(inner={self.inner!r})"

def find_ellipses_patterns(arg: str) -> ArgPattern:
    parts = ELLIPSES_RE.match(arg)
    if not parts:
        raise PatternError(
            f"Invalid ellipsis format in ({arg}), Ellipsis range must be provided in format {{N...M}} where N and M are positive integers, M must be greater than N,  with an allowed minimum range of 4"
        )
    patterns = []
    while True:
        prefix = parts.group(1)
        seq = parse_ellipses_range(parts.group(2))
        next_parts = ELLIPSES_RE.match(prefix)
        if next_parts:
            patterns.append(Pattern("", parts.group(3), seq))
            parts = next_parts
        else:
            patterns.append(Pattern(prefix, parts.group(3), seq))
            break
    for p in patterns:
        if (OPEN_BRACES in p.prefix or CLOSE_BRACES in p.prefix or
            OPEN_BRACES in p.suffix or CLOSE_BRACES in p.suffix):
            raise PatternError(
                f"Invalid ellipsis format in ({arg}), Ellipsis range must be provided in format {{N...M}} where N and M are positive integers, M must be greater than N,  with an allowed minimum range of 4"
            )
    return ArgPattern(patterns)

def has_ellipses(s: List[str]) -> bool:
    pattern = [ELLIPSES, OPEN_BRACES, CLOSE_BRACES]
    return any(any(p in v for p in pattern) for v in s)

def parse_ellipses_range(pattern: str) -> List[str]:
    if OPEN_BRACES not in pattern:
        raise PatternError("Invalid argument")
    if CLOSE_BRACES not in pattern:
        raise PatternError("Invalid argument")
    content = pattern.lstrip(OPEN_BRACES).rstrip(CLOSE_BRACES)
    ellipses_range = content.split(ELLIPSES)
    if len(ellipses_range) != 2:
        raise PatternError("Invalid argument")
    start_str, end_str = ellipses_range
    try:
        start = int(start_str)
        end = int(end_str)
    except Exception:
        raise PatternError("Invalid argument")
    if start > end:
        raise PatternError("Invalid argument:range start cannot be bigger than end")
    ret = []
    width = max(len(start_str), len(end_str))
    for i in range(start, end + 1):
        if start_str.startswith('0') and len(start_str) > 1:
            ret.append(f"{i:0{width}d}")
        else:
            ret.append(str(i))
    return ret

def gen_access_key(length: int) -> str:
    ALPHA_NUMERIC_TABLE = string.digits + string.ascii_uppercase
    if length < 3:
        raise PatternError("access key length is too short")
    return ''.join(random.choice(ALPHA_NUMERIC_TABLE) for _ in range(length))

def gen_secret_key(length: int) -> str:
    if length < 8:
        raise PatternError("secret key length is too short")
    # Generate enough random bytes to get at least `length` base64 characters
    # Each 3 bytes = 4 base64 chars, so n bytes = ceil(length * 3 / 4)
    nbytes = (length * 3 + 3) // 4
    key = base64.urlsafe_b64encode(random.randbytes(nbytes)).decode('utf-8').replace('=', '')
    key_str = key.replace("/", "+")
    return key_str[:length]

# Unit tests
import unittest

class TestStringUtils(unittest.TestCase):
    def test_has_ellipses(self):
        test_cases = [
            (1, ["64"], False),
            (2, ["{1..64}"], True),
            (3, ["{1..2..}"], True),
            (4, ["1...64"], True),
            (5, ["{1...2O}"], True),
            (6, ["..."], True),
            (7, ["{-1...1}"], True),
            (8, ["{0...-1}"], True),
            (9, ["{1....4}"], True),
            (10, ["{1...64}"], True),
            (11, ["{...}"], True),
            (12, ["{1...64}", "{65...128}"], True),
            (13, ["http://rustfs{2...3}/export/set{1...64}"], True),
            (14, [
                "http://rustfs{2...3}/export/set{1...64}",
                "http://rustfs{2...3}/export/set{65...128}",
            ], True),
            (15, ["mydisk-{a...z}{1...20}"], True),
            (16, ["mydisk-{1...4}{1..2.}"], True),
        ]
        for i, args, expected in test_cases:
            ret = has_ellipses(args)
            self.assertEqual(ret, expected, f"Test{i}: Expected {expected}, got {ret}")

    def test_find_ellipses_patterns(self):
        class TestCase:
            def __init__(self, num, pattern, success=False, want=None):
                self.num = num
                self.pattern = pattern
                self.success = success
                self.want = want or []

        test_cases = [
            TestCase(1, "{1..64}"),
            TestCase(2, "1...64"),
            TestCase(2, "..."),
            TestCase(3, "{1..."),
            TestCase(4, "...64}"),
            TestCase(5, "{...}"),
            TestCase(6, "{-1...1}"),
            TestCase(7, "{0...-1}"),
            TestCase(8, "{1...2O}"),
            TestCase(9, "{64...1}"),
            TestCase(10, "{1....4}"),
            TestCase(11, "mydisk-{a...z}{1...20}"),
            TestCase(12, "mydisk-{1...4}{1..2.}"),
            TestCase(13, "{1..2.}-mydisk-{1...4}"),
            TestCase(14, "{{1...4}}"),
            TestCase(16, "{4...02}"),
            TestCase(17, "{f...z}"),
            TestCase(18, "{1...64}", True, [[str(i)] for i in range(1, 65)]),
            TestCase(19, "{1...5} {65...70}", True, [
                [f"{i} ", f"{j}"] for j in range(65, 71) for i in range(1, 6)
            ]),
            TestCase(20, "{01...036}", True, [[f"{i:03d}"] for i in range(1, 37)]),
            TestCase(21, "{001...036}", True, [[f"{i:03d}"] for i in range(1, 37)]),
        ]
        for test_case in test_cases:
            try:
                v = find_ellipses_patterns(test_case.pattern)
                if not test_case.success:
                    self.fail(f"Test{test_case.num}: Expected failure but passed instead")
                got = v.expand()
                if len(got) != len(test_case.want):
                    self.fail(f"Test{test_case.num}: Expected {len(test_case.want)}, got {len(got)}")
                self.assertEqual(got, test_case.want, f"Test{test_case.num}: Expected {test_case.want}, got {got}")
            except Exception as e:
                if test_case.success:
                    self.fail(f"Test{test_case.num}: Expected success but failed instead {e}")

if __name__ == "__main__":
    unittest.main()