import os
import enum
import gzip
import bz2
import lzma
import zlib
import zstandard
import tarfile
import zipfile
import io

class CompressionFormat(enum.Enum):
    Gzip = 1  # .gz
    Bzip2 = 2 # .bz2
    Zip = 3   # .zip
    Xz = 4    # .xz
    Zlib = 5  # .z
    Zstd = 6  # .zst
    Tar = 7   # .tar (uncompressed)
    Unknown = 8

class CompressionLevel(enum.Enum):
    Fastest = 1
    Best = 2
    Default = 3
    Level = 4

    def __new__(cls, value=None):
        obj = object.__new__(cls)
        obj._value = value
        return obj

    @property
    def value(self):
        return self._value

class CompressionFormatHelper:
    @staticmethod
    def from_extension(ext: str) -> CompressionFormat:
        ext = ext.lower()
        if ext in ("gz", "gzip"):
            return CompressionFormat.Gzip
        elif ext in ("bz2", "bzip2"):
            return CompressionFormat.Bzip2
        elif ext == "zip":
            return CompressionFormat.Zip
        elif ext == "xz":
            return CompressionFormat.Xz
        elif ext == "zlib":
            return CompressionFormat.Zlib
        elif ext in ("zst", "zstd"):
            return CompressionFormat.Zstd
        elif ext == "tar":
            return CompressionFormat.Tar
        else:
            return CompressionFormat.Unknown

    @staticmethod
    def from_path(path: str) -> CompressionFormat:
        ext = os.path.splitext(path)[1][1:]  # Extract extension without the dot
        return CompressionFormatHelper.from_extension(ext)

    @staticmethod
    def extension(format: CompressionFormat) -> str:
        if format == CompressionFormat.Gzip:
            return "gz"
        elif format == CompressionFormat.Bzip2:
            return "bz2"
        elif format == CompressionFormat.Zip:
            return "zip"
        elif format == CompressionFormat.Xz:
            return "xz"
        elif format == CompressionFormat.Zlib:
            return "zlib"
        elif format == CompressionFormat.Zstd:
            return "zst"
        elif format == CompressionFormat.Tar:
            return "tar"
        else:
            return ""

    @staticmethod
    def is_supported(format: CompressionFormat) -> bool:
        return format != CompressionFormat.Unknown

    @staticmethod
    def get_decoder(format: CompressionFormat, input_data: io.BytesIO):
        if format == CompressionFormat.Gzip:
            return gzip.GzipFile(fileobj=input_data, mode='rb')
        elif format == CompressionFormat.Bzip2:
            return bz2.BZ2File(input_data, 'rb')
        elif format == CompressionFormat.Xz:
            return lzma.open(input_data, 'rb')
        elif format == CompressionFormat.Zlib:
            return zlib.decompressobj()  # Returns a decompressor object
        elif format == CompressionFormat.Zstd:
            return zstandard.ZstdDecompressor().stream_reader(input_data)
        elif format == CompressionFormat.Tar:
            return input_data # Tarfile expects a file-like object
        elif format == CompressionFormat.Zip:
            raise ValueError("Zip format requires special handling, use extract_zip function instead")
        else:
            raise ValueError("Unsupported file format")

    @staticmethod
    def get_encoder(format: CompressionFormat, output_data: io.BytesIO, level: CompressionLevel):
        if format == CompressionFormat.Gzip:
            compresslevel = 9 # Default
            if level == CompressionLevel.Fastest:
                compresslevel = 1
            elif level == CompressionLevel.Best:
                compresslevel = 9
            elif level == CompressionLevel.Level and level.value is not None:
                compresslevel = level.value

            return gzip.GzipFile(fileobj=output_data, mode='wb', compresslevel=compresslevel)

        elif format == CompressionFormat.Bzip2:
            compresslevel = 9
            if level == CompressionLevel.Fastest:
                compresslevel = 1
            elif level == CompressionLevel.Best:
                compresslevel = 9
            elif level == CompressionLevel.Level and level.value is not None:
                compresslevel = level.value
            return bz2.BZ2File(output_data, 'wb', compresslevel=compresslevel)

        elif format == CompressionFormat.Xz:
            preset = 6
            if level == CompressionLevel.Fastest:
                preset = 0
            elif level == CompressionLevel.Best:
                preset = 9
            elif level == CompressionLevel.Level and level.value is not None:
                preset = level.value
            return lzma.open(output_data, 'wb', preset=preset)

        elif format == CompressionFormat.Zlib:
            compresslevel = 6
            if level == CompressionLevel.Fastest:
                compresslevel = 1
            elif level == CompressionLevel.Best:
                compresslevel = 9
            elif level == CompressionLevel.Level and level.value is not None:
                compresslevel = level.value
            return zlib.compressobj(level=compresslevel) # Returns a compressor object

        elif format == CompressionFormat.Zstd:
            compresslevel = 3 # Default
            if level == CompressionLevel.Fastest:
                compresslevel = 1
            elif level == CompressionLevel.Best:
                compresslevel = 9 # Or higher, check zstd docs
            elif level == CompressionLevel.Level and level.value is not None:
                compresslevel = level.value
            return zstandard.ZstdCompressor(level=compresslevel).stream_writer(output_data)

        elif format == CompressionFormat.Tar:
            return output_data

        elif format == CompressionFormat.Zip:
            raise ValueError("Zip format requires special handling, use create_zip function instead")
        else:
            raise ValueError("Unsupported file format")


def decompress_tar(input_data: io.BytesIO, format: CompressionFormat, callback):
    """Decompresses tar format compressed files."""
    try:
        decoder = CompressionFormatHelper.get_decoder(format, input_data)
        if format == CompressionFormat.Tar:
            tar = tarfile.open(fileobj=decoder, mode="r:")
        else:
            tar = tarfile.open(fileobj=decoder, mode="r:*")

        for member in tar.getmembers():
            callback(member, tar.extractfile(member))  # Pass both member and file object
        tar.close()
    except Exception as e:
        raise IOError(f"Decompression failed: {e}")


class ZipEntry:
    def __init__(self, name: str, size: int, compressed_size: int, is_dir: bool, compression_method: str):
        self.name = name
        self.size = size
        self.compressed_size = compressed_size
        self.is_dir = is_dir
        self.compression_method = compression_method


def extract_zip_simple(zip_path: str, extract_to: str) -> list[ZipEntry]:
    """Simplified ZIP file processing."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
            entries = []
            for zip_info in zip_ref.infolist():
                entries.append(ZipEntry(
                    name=zip_info.filename,
                    size=zip_info.file_size,
                    compressed_size=zip_info.compress_size,
                    is_dir=zip_info.is_dir(),
                    compression_method=zip_info.compress_type
                ))
            return entries
    except Exception as e:
        raise IOError(f"ZIP extraction failed: {e}")


def create_zip_simple(zip_path: str, files: list[tuple[str, bytes]], compression_level: CompressionLevel):
    """Simplified ZIP file creation."""
    try:
        compress_type = zipfile.ZIP_DEFLATED
        compresslevel = 9
        if compression_level == CompressionLevel.Fastest:
            compresslevel = 1
        elif compression_level == CompressionLevel.Best:
            compresslevel = 9
        elif compression_level == CompressionLevel.Level and compression_level.value is not None:
            compresslevel = compression_level.value

        if compresslevel == 0:
            compress_type = zipfile.ZIP_STORED

        with zipfile.ZipFile(zip_path, 'w', compression=compress_type, compresslevel=compresslevel) as zip_file:
            for filename, file_content in files:
                zip_file.writestr(filename, file_content)
    except Exception as e:
        raise IOError(f"ZIP creation failed: {e}")


class Compressor:
    def __init__(self, format: CompressionFormat, level: CompressionLevel = CompressionLevel.Default):
        self.format = format
        self.level = level

    def compress(self, input_data: bytes) -> bytes:
        """Compress data."""
        output = io.BytesIO()
        try:
            encoder = CompressionFormatHelper.get_encoder(self.format, output, self.level)

            if self.format == CompressionFormat.Zlib:
                compressed_data = encoder.compress(input_data)
                compressed_data += encoder.flush()
                output.write(compressed_data)
            else:
                if hasattr(encoder, 'write'):
                    encoder.write(input_data)
                else:
                    raise ValueError(f"Encoder for {self.format} does not have a write method.")

            if hasattr(encoder, 'close'):
                encoder.close()

            return output.getvalue()
        except Exception as e:
            raise IOError(f"Compression failed: {e}")

    def decompress(self, input_data: bytes) -> bytes:
        """Decompress data."""
        input_buffer = io.BytesIO(input_data)
        output = io.BytesIO()
        try:
            decoder = CompressionFormatHelper.get_decoder(self.format, input_buffer)

            if self.format == CompressionFormat.Zlib:
                decompressor = decoder
                chunk_size = 16 * 1024
                while True:
                    chunk = decompressor.decompress(input_buffer.read(chunk_size))
                    if not chunk:
                        break
                    output.write(chunk)
            else:
                while True:
                    chunk = decoder.read(16 * 1024)
                    if not chunk:
                        break
                    output.write(chunk)

            if hasattr(decoder, 'close'):
                decoder.close()

            return output.getvalue()
        except Exception as e:
            raise IOError(f"Decompression failed: {e}")


class Decompressor:
    def __init__(self, format: CompressionFormat):
        self.format = format

    @staticmethod
    def auto_detect(path: str) -> 'Decompressor':
        format = CompressionFormatHelper.from_path(path)
        return Decompressor(format)

    def decompress_file(self, input_path: str, output_path: str):
        """Decompress file."""
        try:
            with open(input_path, 'rb') as infile:
                input_data = io.BytesIO(infile.read())
            decoder = CompressionFormatHelper.get_decoder(self.format, input_data)

            with open(output_path, 'wb') as outfile:
                if isinstance(decoder, zlib.decompressobj):
                    chunk_size = 16 * 1024
                    while True:
                        chunk = decoder.decompress(input_data.read(chunk_size))
                        if not chunk:
                            break
                        outfile.write(chunk)
                else:
                    while True:
                        chunk = decoder.read(16 * 1024)
                        if not chunk:
                            break
                        outfile.write(chunk)

            if hasattr(decoder, 'close'):
                decoder.close()

        except Exception as e:
            raise IOError(f"File decompression failed: {e}")


if __name__ == '__main__':
    # Example Usage (replace with your actual tests)
    import os

    # Create data directory if it doesn't exist
    data_dir = "./data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    # Compression format detection
    print(CompressionFormatHelper.from_extension("gz"))
    print(CompressionFormatHelper.from_path(os.path.join(data_dir, "test.bz2")))

    # Compression/Decompression
    text = b"This is a test string for compression."
    compressor = Compressor(CompressionFormat.Gzip)
    compressed_data = compressor.compress(text)
    decompressed_data = compressor.decompress(compressed_data)
    print(f"Original: {text}")
    print(f"Decompressed: {decompressed_data}")

    #Zip file handling (example - replace with actual files)
    create_zip_simple(os.path.join(data_dir, "test.zip"), [("test.txt", b"Test content")], CompressionLevel.Default)
    extract_zip_simple(os.path.join(data_dir, "test.zip"), os.path.join(data_dir, "extracted"))

    #Tar file handling (example - replace with actual tar file)
    def print_tar_entry(member, file_obj):
        print(f"Extracted: {member.name}")
        if file_obj:
            print(f"Content: {file_obj.read().decode()}")

    # Create a dummy test.tar.gz for demonstration
    import tarfile
    import gzip
    tar_gz_path = os.path.join(data_dir, "test.tar.gz")
    with gzip.open(tar_gz_path, "wb") as f:
        with tarfile.open(fileobj=f, mode="w:gz") as tar:
            # Add a dummy file to the tar archive
            info = tarfile.TarInfo("test.txt")
            info.size = len(b"Test content in tar")
            tar.addfile(info, io.BytesIO(b"Test content in tar"))


    with open(tar_gz_path, "rb") as f:
        try:
            decompress_tar(io.BytesIO(f.read()), CompressionFormat.Gzip, print_tar_entry)
        except Exception as e:
            print(f"Error during tar decompression: {e}")
    pass