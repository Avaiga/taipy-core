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

from datetime import datetime, timedelta
from inspect import isclass
from typing import Any, Dict, List, Optional

import pymongo

from taipy.config.common.scope import Scope

from ..common.alias import DataNodeId, JobId
from ..exceptions.exceptions import InvalidCustomDocument, MissingRequiredProperty
from .data_node import DataNode


class MongoCollectionDataNode(DataNode):
    """Data Node stored in a Mongo collection.

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
        cacheable (bool): True if this data node is cacheable. False otherwise.
        validity_period (Optional[timedelta]): The validity period of a cacheable data node.
            Implemented as a timedelta. If _validity_period_ is set to None, the data_node is
            always up-to-date.
        edit_in_progress (bool): True if a task computing the data node has been submitted
            and not completed yet. False otherwise.
        properties (dict[str, Any]): A dictionary of additional properties. Note that the
            _properties_ parameter must at least contain an entry for _"db_name"_, _"collection_name"_, and _"custom_document"_.
    """

    __STORAGE_TYPE = "mongo_collection"
    __CUSTOM_DOCUMENT_PROPERTY = "custom_document"
    __DB_EXTRA_ARGS_KEY = "db_extra_args"
    _REQUIRED_PROPERTIES: List[str] = [
        "db_name",
        "collection_name",
        __CUSTOM_DOCUMENT_PROPERTY,
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
        cacheable: bool = False,
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

        self._check_custom_document(properties[self.__CUSTOM_DOCUMENT_PROPERTY])

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

        mongo_client = _connect_mongodb(
            db_host=properties.get("db_host", "localhost"),
            db_port=properties.get("db_port", 27017),
            db_username=properties.get("db_username", ""),
            db_password=properties.get("db_password", ""),
            db_extra_args=properties.get(self.__DB_EXTRA_ARGS_KEY, {}),
        )
        self.collection = mongo_client[properties.get("db_name")][properties.get("collection_name")]

        self.custom_document = properties[self.__CUSTOM_DOCUMENT_PROPERTY]

        self._decoder = self._default_decoder
        custom_decoder = getattr(self.custom_document, "decode", None)
        if callable(custom_decoder):
            self._decoder = custom_decoder

        self._encoder = self._default_encoder
        custom_encoder = getattr(self.custom_document, "encode", None)
        if callable(custom_encoder):
            self._encoder = custom_encoder

        if not self._last_edit_date:
            self.unlock_edit()

    def _check_custom_document(self, custom_document):
        if not isclass(custom_document):
            raise InvalidCustomDocument(
                f"Invalid exposed type of {custom_document}. Only custom object class are supported."
            )

    @classmethod
    def storage_type(cls) -> str:
        return cls.__STORAGE_TYPE

    def _read(self):
        cursor = self._read_by_query()

        return list(map(lambda row: self._decoder(row), cursor))

    def _read_by_query(self):
        """Query from a Mongo collection, exclude the _id field"""

        return self.collection.find()

    def _write(self, data) -> None:
        """Check data against a collection of types to handle insertion on the database."""

        if not isinstance(data, list):
            data = [data]

        if len(data) == 0:
            self.collection.drop()
            return

        if isinstance(data[0], dict):
            self._insert_dicts(data)
        else:
            self._insert_dicts([self._encoder(row) for row in data])

    def _insert_dicts(self, data: List[Dict]) -> None:
        """
        :param data: a list of dictionaries

        This method will overwrite the data contained in a list of dictionaries into a collection.
        """
        self.collection.drop()
        self.collection.insert_many(data)

    def _default_decoder(self, document: Dict) -> Any:
        """Decode a Mongo dictionary to a custom document object for reading.

        Args:
            document (Dict): the document dictionary return by Mongo query.

        Returns:
            Any: A custom document object.
        """
        return self.custom_document(**document)

    def _default_encoder(self, document_object: Any) -> Dict:
        """Encode a custom document object to a dictionary for writing to MongoDB.

        Args:
            document_object: the custom document class.

        Returns:
            Dict: the document dictionary.
        """
        return document_object.__dict__


def _connect_mongodb(
    db_host: str, db_port: int, db_username: str, db_password: str, db_extra_args: Dict[str, str]
) -> pymongo.MongoClient:
    """Create a connection to a Mongo database.

    Args:
        db_host (str): the database host.
        db_port (int): the database port.
        db_username (str): the database username.
        db_password (str): the database password.
        db_extra_args (Dict[str, Any]): A dictionary of additional arguments to be passed into database connection string.

    Returns:
        pymongo.MongoClient
    """
    auth_str = ""
    if db_username and db_password:
        auth_str = f"{db_username}:{db_password}@"

    extra_args_str = "&".join(f"{k}={str(v)}" for k, v in db_extra_args.items())
    if extra_args_str:
        extra_args_str = "/?" + extra_args_str

    connection_string = f"mongodb://{auth_str}{db_host}:{db_port}{extra_args_str}"

    return pymongo.MongoClient(connection_string)
