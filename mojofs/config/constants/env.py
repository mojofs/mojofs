DEFAULT_DELIMITER = "_"
ENV_PREFIX = "RUSTFS_"
ENV_WORD_DELIMITER = "_"

DEFAULT_DIR = "/opt/rustfs/events"  # Default directory for event store
DEFAULT_LIMIT = 100000  # Default store limit

# Standard config keys and values.
ENABLE_KEY = "enable"
COMMENT_KEY = "comment"

# Medium-drawn lines separator
# This is used to separate words in environment variable names.
ENV_WORD_DELIMITER_DASH = "-"


from enum import Enum

class EnableState(Enum):
    TRUE = "true"
    FALSE = "false"
    EMPTY = ""
    YES = "yes"
    NO = "no"
    ON = "on"
    OFF = "off"
    ENABLED = "enabled"
    DISABLED = "disabled"
    OK = "ok"
    NOT_OK = "not_ok"
    SUCCESS = "success"
    FAILURE = "failure"
    ACTIVE = "active"
    INACTIVE = "inactive"
    ONE = "1"
    ZERO = "0"

    @classmethod
    def from_str(cls, s):
        s = s.strip().lower()
        if s == "true":
            return cls.TRUE
        elif s == "false":
            return cls.FALSE
        elif s == "":
            return cls.EMPTY
        elif s == "yes":
            return cls.YES
        elif s == "no":
            return cls.NO
        elif s == "on":
            return cls.ON
        elif s == "off":
            return cls.OFF
        elif s == "enabled":
            return cls.ENABLED
        elif s == "disabled":
            return cls.DISABLED
        elif s == "ok":
            return cls.OK
        elif s == "not_ok":
            return cls.NOT_OK
        elif s == "success":
            return cls.SUCCESS
        elif s == "failure":
            return cls.FAILURE
        elif s == "active":
            return cls.ACTIVE
        elif s == "inactive":
            return cls.INACTIVE
        elif s == "1":
            return cls.ONE
        elif s == "0":
            return cls.ZERO
        else:
            raise ValueError(f"Invalid EnableState string: {s}")

    def __str__(self):
        return self.value

    def is_enabled(self):
        return self in (
            EnableState.TRUE,
            EnableState.YES,
            EnableState.ON,
            EnableState.ENABLED,
            EnableState.OK,
            EnableState.SUCCESS,
            EnableState.ACTIVE,
            EnableState.ONE,
        )

    def is_disabled(self):
        return self in (
            EnableState.FALSE,
            EnableState.NO,
            EnableState.OFF,
            EnableState.DISABLED,
            EnableState.NOT_OK,
            EnableState.FAILURE,
            EnableState.INACTIVE,
            EnableState.ZERO,
            EnableState.EMPTY,
        )

    @classmethod
    def get_default(cls):
        return cls.EMPTY


import unittest

class TestEnableState(unittest.TestCase):

    def test_enable_state_display_and_fromstr(self):
        cases = [
            (EnableState.TRUE, "true"),
            (EnableState.FALSE, "false"),
            (EnableState.EMPTY, ""),
            (EnableState.YES, "yes"),
            (EnableState.NO, "no"),
            (EnableState.ON, "on"),
            (EnableState.OFF, "off"),
            (EnableState.ENABLED, "enabled"),
            (EnableState.DISABLED, "disabled"),
            (EnableState.OK, "ok"),
            (EnableState.NOT_OK, "not_ok"),
            (EnableState.SUCCESS, "success"),
            (EnableState.FAILURE, "failure"),
            (EnableState.ACTIVE, "active"),
            (EnableState.INACTIVE, "inactive"),
            (EnableState.ONE, "1"),
            (EnableState.ZERO, "0"),
        ]
        for variant, string in cases:
            self.assertEqual(str(variant), string)
            self.assertEqual(EnableState.from_str(string), variant)

        # Test invalid string
        with self.assertRaises(ValueError):
            EnableState.from_str("invalid")

    def test_enable_state_enum(self):
        cases = [
            (EnableState.TRUE, "true"),
            (EnableState.FALSE, "false"),
            (EnableState.EMPTY, ""),
            (EnableState.YES, "yes"),
            (EnableState.NO, "no"),
            (EnableState.ON, "on"),
            (EnableState.OFF, "off"),
            (EnableState.ENABLED, "enabled"),
            (EnableState.DISABLED, "disabled"),
            (EnableState.OK, "ok"),
            (EnableState.NOT_OK, "not_ok"),
            (EnableState.SUCCESS, "success"),
            (EnableState.FAILURE, "failure"),
            (EnableState.ACTIVE, "active"),
            (EnableState.INACTIVE, "inactive"),
            (EnableState.ONE, "1"),
            (EnableState.ZERO, "0"),
        ]
        for variant, string in cases:
            self.assertEqual(str(variant), string)

    def test_enable_state_enum_from_str(self):
         cases = [
            ("true", EnableState.TRUE),
            ("false", EnableState.FALSE),
            ("", EnableState.EMPTY),
            ("yes", EnableState.YES),
            ("no", EnableState.NO),
            ("on", EnableState.ON),
            ("off", EnableState.OFF),
            ("enabled", EnableState.ENABLED),
            ("disabled", EnableState.DISABLED),
            ("ok", EnableState.OK),
            ("not_ok", EnableState.NOT_OK),
            ("success", EnableState.SUCCESS),
            ("failure", EnableState.FAILURE),
            ("active", EnableState.ACTIVE),
            ("inactive", EnableState.INACTIVE),
            ("1", EnableState.ONE),
            ("0", EnableState.ZERO),
        ]
         for string, variant in cases:
            self.assertEqual(EnableState.from_str(string), variant)

    def test_enable_state_default(self):
        default_state = EnableState.get_default()
        self.assertEqual(default_state, EnableState.EMPTY)
        self.assertEqual(str(default_state), "")

    def test_enable_state_as_str(self):
        cases = [
            (EnableState.TRUE, "true"),
            (EnableState.FALSE, "false"),
            (EnableState.EMPTY, ""),
            (EnableState.YES, "yes"),
            (EnableState.NO, "no"),
            (EnableState.ON, "on"),
            (EnableState.OFF, "off"),
            (EnableState.ENABLED, "enabled"),
            (EnableState.DISABLED, "disabled"),
            (EnableState.OK, "ok"),
            (EnableState.NOT_OK, "not_ok"),
            (EnableState.SUCCESS, "success"),
            (EnableState.FAILURE, "failure"),
            (EnableState.ACTIVE, "active"),
            (EnableState.INACTIVE, "inactive"),
            (EnableState.ONE, "1"),
            (EnableState.ZERO, "0"),
        ]
        for variant, string in cases:
            self.assertEqual(variant.value, string)

    def test_enable_state_is_enabled(self):
        enabled_states = [
            EnableState.TRUE,
            EnableState.YES,
            EnableState.ON,
            EnableState.ENABLED,
            EnableState.OK,
            EnableState.SUCCESS,
            EnableState.ACTIVE,
            EnableState.ONE,
        ]
        for state in enabled_states:
            self.assertTrue(state.is_enabled())

        disabled_states = [
            EnableState.FALSE,
            EnableState.NO,
            EnableState.OFF,
            EnableState.DISABLED,
            EnableState.NOT_OK,
            EnableState.FAILURE,
            EnableState.INACTIVE,
            EnableState.ZERO,
            EnableState.EMPTY,
        ]
        for state in disabled_states:
            self.assertTrue(state.is_disabled())