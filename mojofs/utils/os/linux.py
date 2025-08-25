import os
import stat
import shutil

class DiskInfo:
    def __init__(self, total=0, free=0, used=0, files=0, ffree=0, fstype="", major=0, minor=0):
        self.total = total
        self.free = free
        self.used = used
        self.files = files
        self.ffree = ffree
        self.fstype = fstype
        self.major = major
        self.minor = minor

class IOStats:
    def __init__(self, read_ios=0, read_merges=0, read_sectors=0, read_ticks=0,
                 write_ios=0, write_merges=0, write_sectors=0, write_ticks=0,
                 current_ios=0, total_ticks=0, req_ticks=0,
                 discard_ios=0, discard_merges=0, discard_sectors=0, discard_ticks=0):
        self.read_ios = read_ios
        self.read_merges = read_merges
        self.read_sectors = read_sectors
        self.read_ticks = read_ticks
        self.write_ios = write_ios
        self.write_merges = write_merges
        self.write_sectors = write_sectors
        self.write_ticks = write_ticks
        self.current_ios = current_ios
        self.total_ticks = total_ticks
        self.req_ticks = req_ticks
        self.discard_ios = discard_ios
        self.discard_merges = discard_merges
        self.discard_sectors = discard_sectors
        self.discard_ticks = discard_ticks


def get_info(path):
    """Returns total and free bytes available in a directory, e.g. `/`."""
    try:
        total, used, free = shutil.disk_usage(path)
        stat_info = os.statvfs(path)
        files = stat_info.f_files
        ffree = stat_info.f_ffree
        
        # Get filesystem type (requires platform-specific handling, simplified here)
        fstype = "UNKNOWN"  # Replace with a more robust method if needed

        st = os.stat(path)
        major = os.major(st.st_dev)
        minor = os.minor(st.st_dev)

        return DiskInfo(total, free, used, files, ffree, fstype, major, minor)
    except Exception as e:
        raise IOError(f"Error getting disk info for {path}: {e}")


def get_fs_type(fs_magic):
    """Returns the filesystem type of the underlying mounted filesystem."""
    # This is a simplified mapping; a more complete solution would require a more extensive mapping
    fs_types = {
        0x01021994: "TMPFS",
        0x4d44: "MSDOS",
        0xef53: "EXT4",  # Most common EXT type
        # Add more filesystem types as needed
    }
    return fs_types.get(fs_magic, "UNKNOWN")


def same_disk(disk1, disk2):
    """Checks if two paths reside on the same disk."""
    try:
        stat1 = os.stat(disk1)
        stat2 = os.stat(disk2)
        return stat1.st_dev == stat2.st_dev
    except Exception as e:
        raise IOError(f"Error comparing disks: {e}")


def get_drive_stats(major, minor):
    """Retrieves drive statistics from /sys/dev/block/{major}:{minor}/stat."""
    stats_file = f"/sys/dev/block/{major}:{minor}/stat"
    return read_drive_stats(stats_file)


def read_drive_stats(stats_file):
    """Reads and parses drive statistics from the given file."""
    try:
        with open(stats_file, 'r') as f:
            stats = [int(x) for x in f.read().split()]

        if len(stats) < 11:
            raise ValueError(f"Invalid format in {stats_file}")

        io_stats = IOStats(
            read_ios=stats[0],
            read_merges=stats[1],
            read_sectors=stats[2],
            read_ticks=stats[3],
            write_ios=stats[4],
            write_merges=stats[5],
            write_sectors=stats[6],
            write_ticks=stats[7],
            current_ios=stats[8],
            total_ticks=stats[9],
            req_ticks=stats[10]
        )

        if len(stats) > 14:
            io_stats.discard_ios = stats[11]
            io_stats.discard_merges = stats[12]
            io_stats.discard_sectors = stats[13]
            io_stats.discard_ticks = stats[14]

        return io_stats
    except FileNotFoundError:
        raise IOError(f"Stats file not found: {stats_file}")
    except ValueError as e:
        raise IOError(f"Error parsing stats from {stats_file}: {e}")
    except Exception as e:
        raise IOError(f"Error reading stats from {stats_file}: {e}")


def read_stat(file_name):
    """Reads a stat file and returns a list of integers."""
    try:
        with open(file_name, 'r') as f:
            return [int(x) for x in f.read().split()]
    except FileNotFoundError:
        raise IOError(f"File not found: {file_name}")
    except ValueError as e:
        raise IOError(f"Error parsing integers from {file_name}: {e}")
    except Exception as e:
        raise IOError(f"Error reading file {file_name}: {e}")


if __name__ == '__main__':
    # Example Usage (replace with your actual tests)
    import os
    import shutil

    # Create a dummy directory for testing
    test_dir = "test_disk_info"
    if not os.path.exists(test_dir):
        os.makedirs(test_dir)

    # Get disk info
    try:
        disk_info = get_info(test_dir)
        print(f"Disk Info for {test_dir}:")
        print(f"  Total: {disk_info.total} bytes")
        print(f"  Free: {disk_info.free} bytes")
        print(f"  Used: {disk_info.used} bytes")
        print(f"  Files: {disk_info.files}")
        print(f"  FFree: {disk_info.ffree}")
        print(f"  FSType: {disk_info.fstype}")
        print(f"  Major: {disk_info.major}")
        print(f"  Minor: {disk_info.minor}")
    except IOError as e:
        print(f"Error: {e}")

    # Get drive stats (example - replace with actual major/minor numbers)
    try:
        major = 8  # Example major number
        minor = 0  # Example minor number
        drive_stats = get_drive_stats(major, minor)
        print(f"\nDrive Stats for major: {major}, minor: {minor}:")
        print(f"  Read IOs: {drive_stats.read_ios}")
        print(f"  Write IOs: {drive_stats.write_ios}")
    except IOError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    # Clean up the dummy directory
    shutil.rmtree(test_dir)