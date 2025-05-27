import os
from typing import Optional
import shutil

def findIndex(list: list, key: str, value: str):
    return next((i for i, p in enumerate(list) if p[key].lower() == value.lower()), -1)

def copyAndAppendFile(srcFile: str, destFile: str):
    with open(srcFile, 'r') as src:
        content = src.read()

    with open(destFile, 'a') as desc:
        desc.write(content)

    return True

def copyFile(srcFile: str, destFolder: str, renameFile: Optional[str] = None) -> bool:
    os.makedirs(destFolder, exist_ok=True)

    # Determine destination filename
    destFileName = renameFile if renameFile else os.path.basename(srcFile)

    # Full destination path
    destPath = os.path.join(destFolder, destFileName)

    # Copy the file
    shutil.copy2(srcFile, destPath)

    return True
