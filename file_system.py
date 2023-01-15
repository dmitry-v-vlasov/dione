from typing import overload
from pathlib import Path

# from multimethod import multimethod
# from multimethod import multidispatch


# @multidispatch
# def new_path(path_string: str) -> Path:
#     return Path(path_string)


# @multidispatch
# def new_path(*path_parts) -> Path:
#     return Path(path_parts)


# @multidispatch
def make_directory(path_string: str, parents: bool=True) -> Path:
    path = Path(path_string)
    path.mkdir(parents=parents, exist_ok=True)
    return path


# @multidispatch
# def make_directory(*path_parts, parents: bool=True) -> Path:
#     path = Path(path_parts)
#     path.mkdir(parents=parents, exist_ok=True)
#     return path
    

# @multidispatch
# def exists_directory(path_string: str) -> bool:
#     path = Path(path_string)
#     return path.exists() and path.is_dir()


# @multidispatch
# def exists_directory(path: Path) -> bool:
#     return path.exists() and path.is_dir()


# @multimethod
# def exists_file(path_string: str) -> bool:
#     path = Path(path_string)
#     return path.exists() and path.is_file()


# @multidispatch
# def exists_file(path: Path) -> bool:
#     return path.exists() and path.is_file()

