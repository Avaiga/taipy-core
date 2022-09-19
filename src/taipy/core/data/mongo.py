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

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import pymongo

from taipy.config.common.scope import Scope

from ..common._reload import _self_reload
from ..common.alias import DataNodeId, JobId
from ..exceptions.exceptions import InvalidExposedType, MissingRequiredProperty
from .data_node import DataNode
from .json import DefaultJSONDecoder, DefaultJSONEncoder


class MongoDataNode(DataNode):
    """Data Node stored in a Mongo database.

    Attributes:
        config_id (str): Identifier of the data node configuration. It must be a valid Python
            identifier.
        scope (Scope^): The scope of this data node.
        id (str): The unique identifier of this data node.
        name (str): A user-readable name of this data node.
        parent_id (str): The identifier of the parent (pipeline_id, scenario_id, cycle_id) or
            None.
        last_edit_date (datetime): The date and time of the last modification.
        job_ids (List[str]): The ordered list of jobs that have written this data node.
        validity_period (Optional[timedelta]): The validity period of a cacheable data node.
            Implemented as a timedelta. If _validity_period_ is set to None, the data_node is
            always up-to-date.
        edit_in_progress (bool): True if a task computing the data node has been submitted
            and not completed yet. False otherwise.
        properties (dict[str, Any]): A dictionary of additional properties. Note that the
            _properties_ parameter must at least contain an entry for _"db_username"_,
            _"db_password"_, _"db_name"_, _"collection_name"_, and _"read_query"_.
    """

    __STORAGE_TYPE = "mongo"
    __EXPOSED_TYPE_NUMPY = "numpy"
    __EXPOSED_TYPE_PANDAS = "pandas"
    __VALID_STRING_EXPOSED_TYPES = [__EXPOSED_TYPE_PANDAS, __EXPOSED_TYPE_NUMPY]
    __EXPOSED_TYPE_PROPERTY = "exposed_type"
    _ENCODER_KEY = "encoder"
    _DECODER_KEY = "decoder"
    _REQUIRED_PROPERTIES: List[str] = [
        "db_username",
        "db_password",
        "db_name",
        "collection_name",
        "read_query",
    ]

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
        required = self._REQUIRED_PROPERTIES
        if missing := set(required) - set(properties.keys()):
            raise MissingRequiredProperty(
                f"The following properties " f"{', '.join(x for x in missing)} were not informed and are required"
            )

        if self.__EXPOSED_TYPE_PROPERTY not in properties.keys():
            properties[self.__EXPOSED_TYPE_PROPERTY] = self.__EXPOSED_TYPE_PANDAS
        self._check_exposed_type(properties[self.__EXPOSED_TYPE_PROPERTY])

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
        self._decoder = self._properties.get(self._DECODER_KEY, DefaultJSONDecoder)
        self._encoder = self._properties.get(self._ENCODER_KEY, DefaultJSONEncoder)

        mongo_client = _connect_mongodb(
            db_host=properties.get("db_host", "localhost"),
            db_port=properties.get("db_port", 27017),
            db_username=properties.get("db_username"),
            db_password=properties.get("db_password"),
        )
        self.collection = mongo_client[properties.get("db_name")][properties.get("collection_name")]

        if not self._last_edit_date:
            self.unlock_edit()

    def _check_exposed_type(self, exposed_type):
        if isinstance(exposed_type, str) and exposed_type not in self.__VALID_STRING_EXPOSED_TYPES:
            raise InvalidExposedType(
                f"Invalid string exposed type {exposed_type}. Supported values are {', '.join(self.__VALID_STRING_EXPOSED_TYPES)}"
            )

    @classmethod
    def storage_type(cls) -> str:
        return cls.__STORAGE_TYPE

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
        if self.properties[self.__EXPOSED_TYPE_PROPERTY] == self.__EXPOSED_TYPE_PANDAS:
            return self._read_as_pandas_dataframe()
        if self.properties[self.__EXPOSED_TYPE_PROPERTY] == self.__EXPOSED_TYPE_NUMPY:
            return self._read_as_numpy()
        return self._read_as()

    def _read_as(self):
        custom_class = self.properties[self.__EXPOSED_TYPE_PROPERTY]
        cursor = self._read_by_query()

        return list(map(lambda row: custom_class(**row), cursor))

    def _read_as_numpy(self):
        return self._read_as_pandas_dataframe().to_numpy()

    def _read_as_pandas_dataframe(self, columns: Optional[List[str]] = None):
        cursor = self._read_by_query()
        if columns:
            return pd.DataFrame(list(cursor))[columns]
        return pd.DataFrame(list(cursor))

    def _read_by_query(self):
        """Query from MongoDB, exclude the _id field"""

        query_result = list(self.collection.find(self.read_query, {"_id": 0}))

        encoded_json = json.dumps(query_result)
        return json.loads(encoded_json, cls=self._decoder)

    def _write(self, data) -> None:
        """Check data against a collection of types to handle insertion on the database."""

        if isinstance(data, pd.DataFrame):
            self._insert_dataframe(data)
            return

        if not isinstance(data, list):
            data = [data]

        if len(data) == 0:
            self.collection.delete_many({})
            return

        self._insert_dicts(self._encode_json(data))

    def _insert_dicts(self, data: List[Dict]) -> None:
        """
        :param data: a list of dictionaries

        This method will overwrite the data contained in a list of dictionaries into a collection.
        """
        self.collection.delete_many({})
        self.collection.insert_many(data)

    def _insert_dataframe(self, df: pd.DataFrame) -> None:
        """
        :param df: a pandas dataframe
        """
        self._insert_dicts(df.to_dict(orient="records"))

    def _encode_json(self, data: Any) -> Dict:
        """
        :param data: data of any type to encode into JSON
        """
        encoded_json = json.dumps(data, cls=self._encoder)
        return json.loads(encoded_json)


def _connect_mongodb(db_host, db_port, db_username, db_password):
    if db_username and db_password:
        return pymongo.MongoClient(host=db_host, port=db_port, username=db_username, password=db_password)

    return pymongo.MongoClient(
        host=db_host,
        port=db_port,
    )
