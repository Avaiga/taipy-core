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

import os
import pathlib
import re
import urllib.parse
from abc import abstractmethod
from datetime import datetime, timedelta
from os.path import isfile
from typing import Dict, List, Optional, Set

import modin.pandas as modin_pd
import pandas as pd
from sqlalchemy import create_engine, text

from taipy.config.common.scope import Scope

from .._version._version_manager_factory import _VersionManagerFactory
from ..common.alias import DataNodeId, Edit
from ..exceptions.exceptions import InvalidExposedType, MissingRequiredProperty, UnknownDatabaseEngine
from .data_node import DataNode


class _AbstractFileDataNode(DataNode):
    """Abstract base class for data node implementations (CSVDataNode, ParquetDataNode, ExcelDataNode,
    PickleDataNode and JSONDataNode) that are file based."""

    __STORAGE_TYPE = "NOT_IMPLEMENTED"

    __EXPOSED_TYPE_PROPERTY = "exposed_type"
    __EXPOSED_TYPE_NUMPY = "numpy"
    __EXPOSED_TYPE_PANDAS = "pandas"
    __EXPOSED_TYPE_MODIN = "modin"
    __VALID_STRING_EXPOSED_TYPES = [__EXPOSED_TYPE_PANDAS, __EXPOSED_TYPE_NUMPY, __EXPOSED_TYPE_MODIN]

    __PATH_KEY = "path"
    __DEFAULT_PATH_KEY = "default_path"
    __HAS_HEADER_PROPERTY = "has_header"

    _REQUIRED_PROPERTIES: List[str] = []
    __EXTENSION_MAP = {"csv": "csv", "excel": "xlsx", "parquet": "parquet", "pickle": "p", "json": "json"}

    def __init__(
        self,
        config_id: str,
        scope: Scope,
        id: Optional[DataNodeId] = None,
        name: Optional[str] = None,
        owner_id: Optional[str] = None,
        parent_ids: Optional[Set[str]] = None,
        last_edit_date: Optional[datetime] = None,
        edits: List[Edit] = None,
        version: str = None,
        validity_period: Optional[timedelta] = None,
        edit_in_progress: bool = False,
        properties: Dict = None,
    ):
        if properties is None:
            properties = {}

        super().__init__(
            config_id,
            scope,
            id,
            name,
            owner_id,
            parent_ids,
            last_edit_date,
            edits,
            version or _VersionManagerFactory._build_manager()._get_latest_version(),
            validity_period,
            edit_in_progress,
            **properties,
        )

    def _build_path(self, storage_type):
        from taipy.config.config import Config

        folder = "pickles" if storage_type == "pickle" else "data_nodes"
        dir_path = pathlib.Path(Config.global_config.storage_folder) / folder
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)
        return dir_path / f"{self.id}.{self.__EXTENSION_MAP.get(storage_type)}"
