# Copyright 2023 Avaiga Private Limited
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

import pathlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

from taipy.config.common.scope import Scope

from .._version._version_manager_factory import _VersionManagerFactory
from ..common.alias import DataNodeId, Edit
from .data_node import DataNode


class _AbstractFileDataNode(object):
    """Abstract base class for data node implementations (CSVDataNode, ParquetDataNode, ExcelDataNode,
    PickleDataNode and JSONDataNode) that are file based."""

    __EXTENSION_MAP = {"csv": "csv", "excel": "xlsx", "parquet": "parquet", "pickle": "p", "json": "json"}

    def _build_path(self, storage_type):
        from taipy.config.config import Config

        folder = f"{storage_type}s"
        dir_path = pathlib.Path(Config.global_config.storage_folder) / folder
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)
        return dir_path / f"{self.id}.{self.__EXTENSION_MAP.get(storage_type)}"
