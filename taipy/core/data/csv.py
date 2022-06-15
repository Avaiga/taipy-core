# Copyright 2022 Avaiga Private Limited
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

import csv
from datetime import datetime, timedelta
from os.path import isfile
from typing import Any, Dict, List, Optional

import pandas as pd
from taipy.core.common._reload import _self_reload
from taipy.core.common._warnings import _warn_deprecated
from taipy.core.common.alias import DataNodeId, JobId
from taipy.core.common.scope import Scope
from taipy.core.data.data_node import DataNode

from taipy.core.exceptions.exceptions import MissingRequiredProperty


class CSVDataNode(DataNode):
    """Data Node stored as a CSV file.

    Attributes:
        config_id (str): Identifier of the data node configuration. This string must be a valid
            Python identifier.
        scope (Scope^): The scope of this data node.
        id (str): The unique identifier of this data node.
        name (str): A user-readable name of this data node.
        parent_id (str): The identifier of the parent (pipeline_id, scenario_id, cycle_id) or `None`.
        last_edit_date (datetime): The date and time of the last modification.
        job_ids (List[str]): The ordered list of jobs that have written this data node.
        validity_period (Optional[timedelta]): The validity period of a cacheable data node.
            Implemented as a timedelta. If _validity_period_ is set to None, the data_node is
            always up-to-date.
        edit_in_progress (bool): True if a task computing the data node has been submitted
            and not completed yet. False otherwise.
        properties (dict[str, Any]): A dictionary of additional properties. Note that the
            _properties_ parameter must at least contain a _"path"_ entry representing the path
            of the CSV file.
    """

    __STORAGE_TYPE = "csv"
    __EXPOSED_TYPE_PROPERTY = "exposed_type"
    __EXPOSED_TYPE_NUMPY = "numpy"
    __PATH_KEY = "path"
    __DEFAULT_PATH_KEY = "default_path"
    __HAS_HEADER_PROPERTY = "has_header"
    _REQUIRED_PROPERTIES: List[str] = []

    def __init__(
        self,
        config_id: str,
        scope: Scope,
        id: Optional[DataNodeId] = None,
        name: Optional[str] = None,
        parent_id: Optional[str] = None,
        last_edit_date: Optional[datetime] = None,
        job_ids: List[JobId] = None,
        validity_period: Optional[timedelta] = None,
        edit_in_progress: bool = False,
        properties: Dict = None,
    ):
        if properties is None:
            properties = {}
        if missing := set(self._REQUIRED_PROPERTIES) - set(properties.keys()):
            raise MissingRequiredProperty(
                f"The following properties " f"{', '.join(x for x in missing)} were not informed and are required"
            )
        if self.__HAS_HEADER_PROPERTY not in properties.keys():
            properties[self.__HAS_HEADER_PROPERTY] = True
        super().__init__(
            config_id,
            scope,
            id,
            name,
            parent_id,
            last_edit_date,
            job_ids,
            validity_period,
            edit_in_progress,
            **properties,
        )
        self._path = self.__build_path()
        if not self._last_edit_date and isfile(self._path):
            self.unlock_edit()

    @classmethod
    def storage_type(cls) -> str:
        return cls.__STORAGE_TYPE

    @property  # type: ignore
    @_self_reload(DataNode._MANAGER_NAME)
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        self.properties[self.__DEFAULT_PATH_KEY] = value

    def __build_path(self):
        if path := self._properties.get(self.__DEFAULT_PATH_KEY):
            return path
        if path := self._properties.get(self.__PATH_KEY):
            _warn_deprecated("path", suggest="default_path")
            return path
        raise MissingRequiredProperty("default_path is required")

    def _read(self):
        if self.__EXPOSED_TYPE_PROPERTY in self.properties:
            if self.properties[self.__EXPOSED_TYPE_PROPERTY] == self.__EXPOSED_TYPE_NUMPY:
                return self._read_as_numpy()
            return self._read_as(self.properties[self.__EXPOSED_TYPE_PROPERTY])
        return self._read_as_pandas_dataframe()

    def _read_as(self, custom_class):
        with open(self._path) as csvFile:
            res = list()
            if self.properties[self.__HAS_HEADER_PROPERTY]:
                reader = csv.DictReader(csvFile)
                for line in reader:
                    res.append(custom_class(**line))
            else:
                reader = csv.reader(
                    csvFile,
                )
                for line in reader:
                    res.append(custom_class(*line))
            return res

    def _read_as_numpy(self):
        return self._read_as_pandas_dataframe().to_numpy()

    def _read_as_pandas_dataframe(self, usecols: Optional[List[int]] = None, column_names: Optional[List[str]] = None):
        try:
            if self.properties[self.__HAS_HEADER_PROPERTY]:
                if column_names:
                    return pd.read_csv(self._path)[column_names]
                return pd.read_csv(self._path)
            else:
                if usecols:
                    return pd.read_csv(self._path, header=None, usecols=usecols)
                return pd.read_csv(self._path, header=None)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()

    def _write(self, data: Any):
        pd.DataFrame(data).to_csv(self._path, index=False)

    def write_with_column_names(self, data: Any, columns: List[str] = None, job_id: Optional[JobId] = None):
        """Write a selection of columns.

        Parameters:
            data (Any): The data to write.
            columns (List[str]): The list of column names to write.
            job_id (JobId^): An optional identifier of the writer.
        """
        if not columns:
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame(data, columns=columns)
        df.to_csv(self._path, index=False)
        self._last_edit_date = datetime.now()
        if job_id:
            self.job_ids.append(job_id)
