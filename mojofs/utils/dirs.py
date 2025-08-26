import os
import tempfile
from pathlib import Path
from mojofs.config import DEFAULT_LOG_DIR, DEFAULT_LOG_FILENAME

def get_project_root() -> Path:
    """
    获取当前项目的绝对路径。
    优先级如下：
    1. 通过当前可执行文件路径推断项目根目录。
    2. 通过当前工作目录推断项目根目录。
    如果都失败，抛出异常。
    """
    # 1. 当前可执行文件路径
    try:
        current_exe = Path(os.path.abspath(os.sys.executable))
        project_root = current_exe.parent.parent  # 假设在target/debug或target/release
        print(f"通过可执行文件路径推断项目根目录: {project_root}")
        return project_root
    except Exception:
        pass

    # 2. 当前工作目录
    try:
        current_dir = Path.cwd()
        project_root = current_dir.parent
        print(f"通过当前工作目录推断项目根目录: {project_root}")
        return project_root
    except Exception:
        pass

    raise RuntimeError("无法获取项目根目录，请检查运行环境和项目结构。")

def ensure_directory_writable(path: Path) -> bool:
    """
    确保目录可写。若目录不存在则尝试创建，并测试写入权限。
    """
    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception:
        return False

    test_file = path / ".write_test"
    try:
        with open(test_file, "w") as f:
            f.write("test")
        test_file.unlink()
        return True
    except Exception:
        return False

def get_log_directory(key: str) -> Path:
    """
    获取日志目录，优先级如下：
    1. 指定的环境变量
    2. 系统临时目录
    3. 用户主目录
    4. 当前工作目录
    5. 相对路径
    """
    # 1. 环境变量
    log_dir = os.environ.get(key)
    if log_dir:
        path = Path(log_dir)
        if ensure_directory_writable(path):
            return path

    # 2. 系统临时目录
    temp_dir = Path(tempfile.gettempdir()) / DEFAULT_LOG_FILENAME / DEFAULT_LOG_DIR
    if ensure_directory_writable(temp_dir):
        return temp_dir

    # 3. 用户主目录
    home_dir = os.environ.get("HOME") or os.environ.get("USERPROFILE")
    if home_dir:
        path = Path(home_dir) / f".{DEFAULT_LOG_FILENAME}" / DEFAULT_LOG_DIR
        if ensure_directory_writable(path):
            return path

    # 4. 当前工作目录
    cwd_dir = Path.cwd() / DEFAULT_LOG_DIR
    if ensure_directory_writable(cwd_dir):
        return cwd_dir

    # 5. 相对路径
    return Path(DEFAULT_LOG_DIR)

def get_log_directory_to_string(key: str) -> str:
    """
    获取日志目录的字符串形式
    """
    return str(get_log_directory(key))

# 单元测试
if __name__ == "__main__":
    # 测试 get_project_root
    try:
        root = get_project_root()
        assert root.exists(), f"项目根目录不存在: {root}"
        print(f"测试通过，项目根目录: {root}")
    except Exception as e:
        print(f"获取项目根目录失败: {e}")

    # 测试 get_log_directory
    log_dir = get_log_directory("MOJOFS_LOG_DIR")
    print(f"日志目录: {log_dir}")
    assert ensure_directory_writable(log_dir), "日志目录不可写"