import pathlib
from typing import Optional


class Entity:
    def __init__(self):
        self._last_modified_time: Optional[int] = None

    def _is_up_to_date(self, filepath: pathlib.Path):
        return self._last_modified_time == filepath.stat().st_mtime_ns
