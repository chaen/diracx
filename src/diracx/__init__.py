from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("diracx")
except PackageNotFoundError:
    # package is not installed
    pass
