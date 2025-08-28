class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class FileNotFound(Error):
    """Raised when a file is not found."""
    def __init__(self, message="File not found"):
        self.message = message
        super().__init__(self.message)


class FileVersionNotFound(Error):
    """Raised when a specific file version is not found."""
    def __init__(self, message="File version not found"):
        self.message = message
        super().__init__(self.message)


class VolumeNotFound(Error):
    """Raised when a volume is not found."""
    def __init__(self, message="Volume not found"):
        self.message = message
        super().__init__(self.message)


class FileCorrupt(Error):
    """Raised when a file is corrupt."""
    def __init__(self, message="File corrupt"):
        self.message = message
        super().__init__(self.message)


class DoneForNow(Error):
    """Raised when an operation is done for now."""
    def __init__(self, message="Done for now"):
        self.message = message
        super().__init__(self.message)


class MethodNotAllowed(Error):
    """Raised when a method is not allowed."""
    def __init__(self, message="Method not allowed"):
        self.message = message
        super().__init__(self.message)


class Unexpected(Error):
    """Raised when an unexpected error occurs."""
    def __init__(self, message="Unexpected error"):
        self.message = message
        super().__init__(self.message)


class Io(Error):
    """Raised for I/O errors."""
    def __init__(self, io_error, message=None):
        self.io_error = io_error
        self.message = message if message else f"I/O error: {io_error}"
        super().__init__(self.message)


class RmpSerdeDecode(Error):
    """Raised for rmp serde decode errors."""
    def __init__(self, message):
        self.message = f"rmp serde decode error: {message}"
        super().__init__(self.message)


class RmpSerdeEncode(Error):
    """Raised for rmp serde encode errors."""
    def __init__(self, message):
        self.message = f"rmp serde encode error: {message}"
        super().__init__(self.message)


class FromUtf8(Error):
    """Raised for invalid UTF-8 errors."""
    def __init__(self, message):
        self.message = f"Invalid UTF-8: {message}"
        super().__init__(self.message)


class RmpDecodeValueRead(Error):
    """Raised for rmp decode value read errors."""
    def __init__(self, message):
        self.message = f"rmp decode value read error: {message}"
        super().__init__(self.message)


class RmpEncodeValueWrite(Error):
    """Raised for rmp encode value write errors."""
    def __init__(self, message):
        self.message = f"rmp encode value write error: {message}"
        super().__init__(self.message)


class RmpDecodeNumValueRead(Error):
    """Raised for rmp decode num value read errors."""
    def __init__(self, message):
        self.message = f"rmp decode num value read error: {message}"
        super().__init__(self.message)


class RmpDecodeMarkerRead(Error):
    """Raised for rmp decode marker read errors."""
    def __init__(self, message):
        self.message = f"rmp decode marker read error: {message}"
        super().__init__(self.message)


class TimeComponentRange(Error):
    """Raised for time component range errors."""
    def __init__(self, message):
        self.message = f"time component range error: {message}"
        super().__init__(self.message)


class UuidParse(Error):
    """Raised for UUID parse errors."""
    def __init__(self, message):
        self.message = f"uuid parse error: {message}"
        super().__init__(self.message)


def other_error(error):
    """Creates an I/O error from another error."""
    import errno
    return Io(OSError(errno.EIO, str(error)))


def is_io_eof(e):
    """Checks if an error is an I/O EOF error."""
    if isinstance(e, Io) and isinstance(e.io_error, OSError):
        import errno
        return e.io_error.errno == errno.EIO and "Unexpected EOF" in str(e.io_error) #TODO: Check if this is the correct way to check for EOF
    return False


# Example Usage (You'll likely need more robust tests)
if __name__ == '__main__':
    try:
        raise FileNotFound()
    except Error as e:
        print(e)

    try:
        raise Io(OSError(13, "Permission denied"))
    except Error as e:
        print(e)

    try:
        raise other_error("Custom error message")
    except Error as e:
        print(e)