import os
import site

def add_module_to_path():
    """将当前模块添加到Python搜索路径"""
    # 获取当前文件所在目录
    project_root = os.path.dirname(os.path.abspath(__file__))
    # 获取site-packages目录路径
    site_packages_path = None
    for path in site.getsitepackages():
        print(path)
        if "site-packages" in path:
            site_packages_path = path
            break
    # 将项目根路径写入askkb.pth文件
    pth_file_path = os.path.join(site_packages_path, "python.pth")
    with open(pth_file_path, "w", encoding="utf-8") as f:
        f.write(project_root)