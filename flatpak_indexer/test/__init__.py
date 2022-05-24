from pathlib import Path
from typing import Optional


rootpath: Optional[Path] = None


def get_test_data_path():
    assert rootpath is not None, "rootpath must be set in conftest.py"
    return rootpath / "test-data"
