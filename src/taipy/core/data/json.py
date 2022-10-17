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

import dataclasses
import json
from datetime import date, datetime, timedelta
from enum import Enum
from os.path import isfile
from typing import Any, Dict, List, Optional

from taipy.config.common.scope import Scope

from ..common._reload import _self_reload
from ..common.alias import DataNodeId, JobId
from ..exceptions.exceptions import MissingRequiredProperty
from .data_node import DataNode


class JSONDataNode(DataNode):
    """Data Node stored as a JSON file.

    Attributes:
        config_id (str): Identifier of the data node configuration. This string must be a valid
            Python identifier.
        scope (Scope^): The scope of this data node.
        id (str): The unique identifier of this data node.
        name (str): A user-readable name of this data node.
        parent_id (str): The identifier of the parent (pipeline_id, scenario_id, cycle_id) or `None`.
        last_edit_date (datetime): The date and time of the last modification.
        job_ids (List[str]): The ordered list of jobs that have written this data node.
        cacheable (bool): True if this data node is cacheable. False otherwise.
        validity_period (Optional[timedelta]): The validity period of a cacheable data node.
            Implemented as a timedelta. If _validity_period_ is set to None, the data_node is
            always up-to-date.
        edit_in_progress (bool): True if a task computing the data node has been submitted
            and not completed yet. False otherwise.
        path (str): The path to the JSON file.
        encoder (json.JSONEncoder): The JSON encoder that is used to write into the JSON file.
        decoder (json.JSONDecoder): The JSON decoder that is used to read from the JSON file.
        properties (dict[str, Any]): A dictionary of additional properties. The _properties_
            must have a _"default_path"_ or _"path"_ entry with the path of the JSON file.
    """

    __STORAGE_TYPE = "json"
    __DEFAULT_PATH_KEY = "default_path"
    __PATH_KEY = "path"
    _ENCODER_KEY = "encoder"
    _DECODER_KEY = "decoder"
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
        cacheable: bool = False,
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

        self._path = properties.get(self.__PATH_KEY, properties.get(self.__DEFAULT_PATH_KEY))
        if self._path is None:
            raise MissingRequiredProperty("default_path is required in a JSON data node config")
        else:
            properties[self.__PATH_KEY] = self._path

        super().__init__(
            config_id,
            scope,
            id,
            name,
            parent_id,
            last_edit_date,
            job_ids,
            cacheable,
            validity_period,
            edit_in_progress,
            **properties,
        )
        self._decoder = self._properties.get(self._DECODER_KEY, _DefaultJSONDecoder)
        self._encoder = self._properties.get(self._ENCODER_KEY, _DefaultJSONEncoder)

        if not self._last_edit_date and isfile(self._path):  # type: ignore
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
        self._path = value
        self.properties[self.__PATH_KEY] = value

    @property  # type: ignore
    @_self_reload(DataNode._MANAGER_NAME)
    def encoder(self):
        return self._encoder

    @encoder.setter
    def encoder(self, encoder: json.JSONEncoder):
        self.properties[self._ENCODER_KEY] = encoder

    @property  # type: ignore
    @_self_reload(DataNode._MANAGER_NAME)
    def decoder(self):
        return self._decoder

    @decoder.setter
    def decoder(self, decoder: json.JSONDecoder):
        self.properties[self._DECODER_KEY] = decoder

    def _read(self):
        with open(self._path, "r") as f:
            return json.load(f, cls=self._decoder)

    def _write(self, data: Any):
        with open(self._path, "w") as f:  # type: ignore
            json.dump(data, f, indent=4, cls=self._encoder)


class _DefaultJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Enum):
            return o.value
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


class _DefaultJSONDecoder(json.JSONDecoder):
    pass
