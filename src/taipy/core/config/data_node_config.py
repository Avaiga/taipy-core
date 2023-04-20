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

import json
from copy import copy
from typing import Any, Callable, Dict, List, Optional, Union

from taipy.config._config import _Config
from taipy.config.common._config_blocker import _ConfigBlocker
from taipy.config.common._template_handler import _TemplateHandler as _tpl
from taipy.config.common.scope import Scope
from taipy.config.config import Config
from taipy.config.section import Section

from ..common._warnings import _warn_deprecated
from ..common.default_custom_document import DefaultCustomDocument


class DataNodeConfig(Section):
    """
    Configuration fields needed to instantiate a `DataNode^`.

    A Data Node config is made to be used as a generator for actual data nodes. It holds configuration information
    needed to create an actual data node.

    Attributes:
        id (str): Unique identifier of the data node config. It must be a valid Python variable name.
        storage_type (str): Storage type of the data nodes created from the data node config. The possible values
            are : "csv", "excel", "pickle", "sql_table", "sql", "mongo_collection", "generic", "json", "parquet" and
            "in_memory".
            The default value is "pickle".
            Note that the "in_memory" value can only be used when `JobConfig^`.mode is "standalone".
        scope (Optional[Scope^]): The optional `Scope^` of the data nodes instantiated from the data node config.
            The default value is SCENARIO.
        **properties (dict[str, any]): A dictionary of additional properties.
    """

    name = "DATA_NODE"

    _STORAGE_TYPE_KEY = "storage_type"
    _STORAGE_TYPE_VALUE_PICKLE = "pickle"
    _STORAGE_TYPE_VALUE_SQL_TABLE = "sql_table"
    _STORAGE_TYPE_VALUE_SQL = "sql"
    _STORAGE_TYPE_VALUE_MONGO_COLLECTION = "mongo_collection"
    _STORAGE_TYPE_VALUE_CSV = "csv"
    _STORAGE_TYPE_VALUE_EXCEL = "excel"
    _STORAGE_TYPE_VALUE_IN_MEMORY = "in_memory"
    _STORAGE_TYPE_VALUE_GENERIC = "generic"
    _STORAGE_TYPE_VALUE_JSON = "json"
    _STORAGE_TYPE_VALUE_PARQUET = "parquet"
    _DEFAULT_STORAGE_TYPE = _STORAGE_TYPE_VALUE_PICKLE
    _ALL_STORAGE_TYPES = [
        _STORAGE_TYPE_VALUE_PICKLE,
        _STORAGE_TYPE_VALUE_SQL_TABLE,
        _STORAGE_TYPE_VALUE_SQL,
        _STORAGE_TYPE_VALUE_MONGO_COLLECTION,
        _STORAGE_TYPE_VALUE_CSV,
        _STORAGE_TYPE_VALUE_EXCEL,
        _STORAGE_TYPE_VALUE_IN_MEMORY,
        _STORAGE_TYPE_VALUE_GENERIC,
        _STORAGE_TYPE_VALUE_JSON,
        _STORAGE_TYPE_VALUE_PARQUET,
    ]

    _EXPOSED_TYPE_KEY = "exposed_type"
    _EXPOSED_TYPE_PANDAS = "pandas"
    _EXPOSED_TYPE_MODIN = "modin"
    _EXPOSED_TYPE_NUMPY = "numpy"
    _DEFAULT_EXPOSED_TYPE = _EXPOSED_TYPE_PANDAS

    _ALL_EXPOSED_TYPES = [
        _EXPOSED_TYPE_PANDAS,
        _EXPOSED_TYPE_MODIN,
        _EXPOSED_TYPE_NUMPY,
    ]
    # Generic
    _OPTIONAL_READ_FUNCTION_GENERIC_PROPERTY = "read_fct"
    _OPTIONAL_READ_FUNCTION_ARGS_GENERIC_PROPERTY = "read_fct_args"
    _OPTIONAL_WRITE_FUNCTION_GENERIC_PROPERTY = "write_fct"
    _OPTIONAL_WRITE_FUNCTION_ARGS_GENERIC_PROPERTY = "write_fct_args"
    # CSV
    _OPTIONAL_EXPOSED_TYPE_CSV_PROPERTY = "exposed_type"
    _OPTIONAL_DEFAULT_PATH_CSV_PROPERTY = "default_path"
    _OPTIONAL_HAS_HEADER_CSV_PROPERTY = "has_header"
    # Excel
    _OPTIONAL_EXPOSED_TYPE_EXCEL_PROPERTY = "exposed_type"
    _OPTIONAL_DEFAULT_PATH_EXCEL_PROPERTY = "default_path"
    _OPTIONAL_HAS_HEADER_EXCEL_PROPERTY = "has_header"
    _OPTIONAL_SHEET_NAME_EXCEL_PROPERTY = "sheet_name"
    # In memory
    _OPTIONAL_DEFAULT_DATA_IN_MEMORY_PROPERTY = "default_data"
    # SQL
    _REQUIRED_DB_NAME_SQL_PROPERTY = "db_name"
    _REQUIRED_DB_ENGINE_SQL_PROPERTY = "db_engine"
    _DB_ENGINE_SQLITE = "sqlite"
    _OPTIONAL_FOLDER_PATH_SQLITE_PROPERTY = "sqlite_folder_path"
    _OPTIONAL_FILE_EXTENSION_SQLITE_PROPERTY = "sqlite_file_extension"
    _OPTIONAL_DB_PASSWORD_SQL_PROPERTY = "db_password"
    _OPTIONAL_DB_USERNAME_SQL_PROPERTY = "db_username"
    _OPTIONAL_PORT_SQL_PROPERTY = "db_port"
    _OPTIONAL_HOST_SQL_PROPERTY = "db_host"
    _OPTIONAL_DRIVER_SQL_PROPERTY = "db_driver"
    _OPTIONAL_DB_EXTRA_ARGS_SQL_PROPERTY = "db_extra_args"
    _OPTIONAL_EXPOSED_TYPE_SQL_PROPERTY = "exposed_type"
    # SQL_TABLE
    _REQUIRED_TABLE_NAME_SQL_TABLE_PROPERTY = "table_name"
    # SQL
    _REQUIRED_READ_QUERY_SQL_PROPERTY = "read_query"
    _REQUIRED_WRITE_QUERY_BUILDER_SQL_PROPERTY = "write_query_builder"
    # MONGO
    _REQUIRED_DB_NAME_MONGO_PROPERTY = "db_name"
    _REQUIRED_COLLECTION_NAME_MONGO_PROPERTY = "collection_name"
    _OPTIONAL_CUSTOM_DOCUMENT_MONGO_PROPERTY = "custom_document"
    _OPTIONAL_USERNAME_MONGO_PROPERTY = "db_username"
    _OPTIONAL_PASSWORD_MONGO_PROPERTY = "db_password"
    _OPTIONAL_HOST_MONGO_PROPERTY = "db_host"
    _OPTIONAL_PORT_MONGO_PROPERTY = "db_port"
    _OPTIONAL_DB_EXTRA_ARGS_MONGO_PROPERTY = "db_extra_args"
    # Pickle
    _OPTIONAL_DEFAULT_PATH_PICKLE_PROPERTY = "default_path"
    _OPTIONAL_DEFAULT_DATA_PICKLE_PROPERTY = "default_data"
    # JSON
    _OPTIONAL_ENCODER_JSON_PROPERTY = "encoder"
    _OPTIONAL_DECODER_JSON_PROPERTY = "decoder"
    _OPTIONAL_DEFAULT_PATH_JSON_PROPERTY = "default_path"
    # Parquet
    _OPTIONAL_EXPOSED_TYPE_PARQUET_PROPERTY = "exposed_type"
    _OPTIONAL_DEFAULT_PATH_PARQUET_PROPERTY = "default_path"
    _OPTIONAL_ENGINE_PARQUET_PROPERTY = "engine"
    _OPTIONAL_COMPRESSION_PARQUET_PROPERTY = "compression"
    _OPTIONAL_READ_KWARGS_PARQUET_PROPERTY = "read_kwargs"
    _OPTIONAL_WRITE_KWARGS_PARQUET_PROPERTY = "write_kwargs"

    _REQUIRED_PROPERTIES: Dict[str, List] = {
        _STORAGE_TYPE_VALUE_PICKLE: [],
        _STORAGE_TYPE_VALUE_SQL_TABLE: [
            _REQUIRED_DB_NAME_SQL_PROPERTY,
            _REQUIRED_DB_ENGINE_SQL_PROPERTY,
            _REQUIRED_TABLE_NAME_SQL_TABLE_PROPERTY,
        ],
        _STORAGE_TYPE_VALUE_SQL: [
            _REQUIRED_DB_NAME_SQL_PROPERTY,
            _REQUIRED_DB_ENGINE_SQL_PROPERTY,
            _REQUIRED_READ_QUERY_SQL_PROPERTY,
            _REQUIRED_WRITE_QUERY_BUILDER_SQL_PROPERTY,
        ],
        _STORAGE_TYPE_VALUE_MONGO_COLLECTION: [
            _REQUIRED_DB_NAME_MONGO_PROPERTY,
            _REQUIRED_COLLECTION_NAME_MONGO_PROPERTY,
        ],
        _STORAGE_TYPE_VALUE_CSV: [],
        _STORAGE_TYPE_VALUE_EXCEL: [],
        _STORAGE_TYPE_VALUE_IN_MEMORY: [],
        _STORAGE_TYPE_VALUE_GENERIC: [],
        _STORAGE_TYPE_VALUE_JSON: [],
        _STORAGE_TYPE_VALUE_PARQUET: [],
    }

    _OPTIONAL_PROPERTIES = {
        _STORAGE_TYPE_VALUE_GENERIC: {
            _OPTIONAL_READ_FUNCTION_GENERIC_PROPERTY: None,
            _OPTIONAL_WRITE_FUNCTION_GENERIC_PROPERTY: None,
            _OPTIONAL_READ_FUNCTION_ARGS_GENERIC_PROPERTY: None,
            _OPTIONAL_WRITE_FUNCTION_ARGS_GENERIC_PROPERTY: None,
        },
        _STORAGE_TYPE_VALUE_CSV: {
            _OPTIONAL_DEFAULT_PATH_CSV_PROPERTY: None,
            _OPTIONAL_HAS_HEADER_CSV_PROPERTY: True,
            _OPTIONAL_EXPOSED_TYPE_CSV_PROPERTY: _DEFAULT_EXPOSED_TYPE,
        },
        _STORAGE_TYPE_VALUE_EXCEL: {
            _OPTIONAL_DEFAULT_PATH_EXCEL_PROPERTY: None,
            _OPTIONAL_HAS_HEADER_EXCEL_PROPERTY: True,
            _OPTIONAL_SHEET_NAME_EXCEL_PROPERTY: None,
            _OPTIONAL_EXPOSED_TYPE_EXCEL_PROPERTY: _DEFAULT_EXPOSED_TYPE,
        },
        _STORAGE_TYPE_VALUE_IN_MEMORY: {_OPTIONAL_DEFAULT_DATA_IN_MEMORY_PROPERTY: None},
        _STORAGE_TYPE_VALUE_SQL_TABLE: {
            _OPTIONAL_DB_USERNAME_SQL_PROPERTY: None,
            _OPTIONAL_DB_PASSWORD_SQL_PROPERTY: None,
            _OPTIONAL_HOST_SQL_PROPERTY: "localhost",
            _OPTIONAL_PORT_SQL_PROPERTY: 1433,
            _OPTIONAL_DRIVER_SQL_PROPERTY: "",
            _OPTIONAL_FOLDER_PATH_SQLITE_PROPERTY: None,
            _OPTIONAL_FILE_EXTENSION_SQLITE_PROPERTY: ".db",
            _OPTIONAL_DB_EXTRA_ARGS_SQL_PROPERTY: None,
            _OPTIONAL_EXPOSED_TYPE_SQL_PROPERTY: _DEFAULT_EXPOSED_TYPE,
        },
        _STORAGE_TYPE_VALUE_SQL: {
            _OPTIONAL_DB_USERNAME_SQL_PROPERTY: None,
            _OPTIONAL_DB_PASSWORD_SQL_PROPERTY: None,
            _OPTIONAL_HOST_SQL_PROPERTY: "localhost",
            _OPTIONAL_PORT_SQL_PROPERTY: 1433,
            _OPTIONAL_DRIVER_SQL_PROPERTY: "",
            _OPTIONAL_FOLDER_PATH_SQLITE_PROPERTY: None,
            _OPTIONAL_FILE_EXTENSION_SQLITE_PROPERTY: ".db",
            _OPTIONAL_DB_EXTRA_ARGS_SQL_PROPERTY: None,
            _OPTIONAL_EXPOSED_TYPE_SQL_PROPERTY: _DEFAULT_EXPOSED_TYPE,
        },
        _STORAGE_TYPE_VALUE_MONGO_COLLECTION: {
            _OPTIONAL_CUSTOM_DOCUMENT_MONGO_PROPERTY: DefaultCustomDocument,
            _OPTIONAL_USERNAME_MONGO_PROPERTY: "",
            _OPTIONAL_PASSWORD_MONGO_PROPERTY: "",
            _OPTIONAL_HOST_MONGO_PROPERTY: "localhost",
            _OPTIONAL_PORT_MONGO_PROPERTY: 27017,
            _OPTIONAL_DB_EXTRA_ARGS_MONGO_PROPERTY: None,
        },
        _STORAGE_TYPE_VALUE_PICKLE: {
            _OPTIONAL_DEFAULT_PATH_PICKLE_PROPERTY: None,
            _OPTIONAL_DEFAULT_DATA_PICKLE_PROPERTY: None,
        },
        _STORAGE_TYPE_VALUE_JSON: {
            _OPTIONAL_DEFAULT_PATH_PICKLE_PROPERTY: None,
            _OPTIONAL_ENCODER_JSON_PROPERTY: None,
            _OPTIONAL_DECODER_JSON_PROPERTY: None,
        },
        _STORAGE_TYPE_VALUE_PARQUET: {
            _OPTIONAL_DEFAULT_PATH_PARQUET_PROPERTY: None,
            _OPTIONAL_ENGINE_PARQUET_PROPERTY: "pyarrow",
            _OPTIONAL_COMPRESSION_PARQUET_PROPERTY: "snappy",
            _OPTIONAL_READ_KWARGS_PARQUET_PROPERTY: None,
            _OPTIONAL_WRITE_KWARGS_PARQUET_PROPERTY: None,
            _OPTIONAL_EXPOSED_TYPE_PARQUET_PROPERTY: _DEFAULT_EXPOSED_TYPE,
        },
    }

    _SCOPE_KEY = "scope"
    _DEFAULT_SCOPE = Scope.SCENARIO

    def __init__(self, id: str, storage_type: Optional[str] = None, scope: Scope = None, **properties):
        self._storage_type = storage_type
        self._scope = scope
        super().__init__(id, **properties)

    def __copy__(self):
        return DataNodeConfig(self.id, self._storage_type, self._scope, **copy(self._properties))

    def __getattr__(self, item: str) -> Optional[Any]:
        return _tpl._replace_templates(self._properties.get(item))

    @property
    def storage_type(self):
        return _tpl._replace_templates(self._storage_type)

    @storage_type.setter  # type: ignore
    @_ConfigBlocker._check()
    def storage_type(self, val):
        self._storage_type = val

    @property
    def scope(self):
        return _tpl._replace_templates(self._scope)

    @scope.setter  # type: ignore
    @_ConfigBlocker._check()
    def scope(self, val):
        self._scope = val

    @property
    def cacheable(self):
        _warn_deprecated("cacheable", suggest="the skippable feature")
        cacheable = self._properties.get("cacheable")
        if cacheable is not None:
            return _tpl._replace_templates(cacheable)
        else:
            return False

    @cacheable.setter  # type: ignore
    @_ConfigBlocker._check()
    def cacheable(self, val):
        _warn_deprecated("cacheable", suggest="the skippable feature")
        self._properties["cacheable"] = val

    @classmethod
    def default_config(cls):
        return DataNodeConfig(cls._DEFAULT_KEY, cls._DEFAULT_STORAGE_TYPE, cls._DEFAULT_SCOPE)

    def _to_dict(self):
        as_dict = {}
        if self._storage_type is not None:
            as_dict[self._STORAGE_TYPE_KEY] = self._storage_type
        if self._scope is not None:
            as_dict[self._SCOPE_KEY] = self._scope
        as_dict.update(self._properties)
        return as_dict

    @classmethod
    def _from_dict(cls, as_dict: Dict[str, Any], id: str, config: Optional[_Config] = None):
        as_dict.pop(cls._ID_KEY, id)
        storage_type = as_dict.pop(cls._STORAGE_TYPE_KEY, None)
        scope = as_dict.pop(cls._SCOPE_KEY, None)
        return DataNodeConfig(id=id, storage_type=storage_type, scope=scope, **as_dict)

    def _update(self, as_dict, default_section=None):
        self._storage_type = as_dict.pop(self._STORAGE_TYPE_KEY, self._storage_type)
        if self._storage_type is None and default_section:
            self._storage_type = default_section.storage_type

        self._scope = as_dict.pop(self._SCOPE_KEY, self._scope)
        if self._scope is None and default_section:
            if default_section.scope and self._storage_type == default_section.storage_type:
                self._scope = default_section.scope
            else:
                self._scope = self._DEFAULT_SCOPE

        self._properties.update(as_dict)
        if default_section and self._storage_type == default_section.storage_type:
            self._properties = {**default_section.properties, **self._properties}

        # Assign default value to optional properties if not defined by user
        if self._OPTIONAL_PROPERTIES.get(self._storage_type):
            for optional_property, default_value in self._OPTIONAL_PROPERTIES[self._storage_type].items():
                if default_value is not None and self._properties.get(optional_property) is None:
                    self._properties[optional_property] = default_value

    @staticmethod
    def _configure_default(storage_type: str, scope: Optional[Scope] = None, **properties) -> "DataNodeConfig":
        """Configure the default values for data node configurations.

        This function creates the _default data node configuration_ object,
        where all data node configuration objects will find their default
        values when needed.

        Parameters:
            storage_type (str): The default storage type for all data node configurations.
                The possible values are *"pickle"* (the default value), *"csv"*, *"excel"*,
                *"sql"*, *"mongo_collection"*, *"in_memory"*, *"json"*, *"parquet"* or
                *"generic"*.
            scope (Optional[Scope^]): The default scope for all data node configurations.<br/>
                The default value is `Scope.SCENARIO`.
            **properties (dict[str, any]): A keyworded variable length list of additional arguments.

        Returns:
            The default data node configuration.
        """
        section = DataNodeConfig(_Config.DEFAULT_KEY, storage_type, scope, **properties)
        Config._register_default(section)
        return Config.sections[DataNodeConfig.name][_Config.DEFAULT_KEY]

    @classmethod
    def _configure(
        cls, id: str, storage_type: Optional[str] = None, scope: Optional[Scope] = None, **properties
    ) -> "DataNodeConfig":
        """Configure a new data node configuration.

        Parameters:
            id (str): The unique identifier of the new data node configuration.
            storage_type (Optional[str]): The data node configuration storage type. The possible values
                are None (which is the default value of *"pickle"*, unless it has been overloaded by the
                *storage_type* value set in the default data node configuration
                (see `(Config.)configure_default_data_node()^`)), *"pickle"*, *"csv"*, *"excel"*,
                *"sql_table"*, *"sql"*, *"json"*, *"parquet"*, *"mongo_collection"*, *"in_memory"*, or
                *"generic"*.
            scope (Optional[Scope^]): The scope of the data node configuration.<br/>
                The default value is `Scope.SCENARIO` (or the one specified in
                `(Config.)configure_default_data_node()^`).
            **properties (dict[str, any]): A keyworded variable length list of additional arguments.

        Returns:
            The new data node configuration.
        """
        configuration_map: Dict[str, Callable] = {
            cls._STORAGE_TYPE_VALUE_PICKLE: cls._configure_pickle,
            cls._STORAGE_TYPE_VALUE_SQL_TABLE: cls._configure_sql_table,
            cls._STORAGE_TYPE_VALUE_SQL: cls._configure_sql,
            cls._STORAGE_TYPE_VALUE_MONGO_COLLECTION: cls._configure_mongo_collection,
            cls._STORAGE_TYPE_VALUE_CSV: cls._configure_csv,
            cls._STORAGE_TYPE_VALUE_EXCEL: cls._configure_excel,
            cls._STORAGE_TYPE_VALUE_IN_MEMORY: cls._configure_in_memory,
            cls._STORAGE_TYPE_VALUE_GENERIC: cls._configure_generic,
            cls._STORAGE_TYPE_VALUE_JSON: cls._configure_json,
            cls._STORAGE_TYPE_VALUE_PARQUET: cls._configure_parquet,
        }

        if storage_type in cls._ALL_STORAGE_TYPES:
            return configuration_map[storage_type](id=id, scope=scope, **properties)

        return cls.__configure(id, storage_type, scope, **properties)

    @classmethod
    def _configure_csv(
        cls,
        id: str,
        default_path: Optional[str] = None,
        has_header: Optional[bool] = None,
        exposed_type: Optional[str] = None,
        scope: Optional[Scope] = None,
        **properties,
    ) -> "DataNodeConfig":
        """Configure a new CSV data node configuration.

        Parameters:
            id (str): The unique identifier of the new CSV data node configuration.
            default_path (Optional[str]): The default path of the CSV file.
            has_header (Optional[bool]): If True, indicates that the CSV file has a header.
            exposed_type (Optional[str]): The exposed type of the data read from CSV file.<br/>
                The default value is `pandas`.
            scope (Optional[Scope^]): The scope of the CSV data node configuration.<br/>
                The default value is `Scope.SCENARIO`.
            **properties (dict[str, any]): A keyworded variable length list of additional arguments.

        Returns:
            The new CSV data node configuration.
        """
        if default_path is not None:
            properties[cls._OPTIONAL_DEFAULT_PATH_CSV_PROPERTY] = default_path
        if has_header is not None:
            properties[cls._OPTIONAL_HAS_HEADER_CSV_PROPERTY] = has_header
        if exposed_type is not None:
            properties[cls._OPTIONAL_EXPOSED_TYPE_CSV_PROPERTY] = exposed_type

        return cls.__configure(id, DataNodeConfig._STORAGE_TYPE_VALUE_CSV, scope, **properties)

    @classmethod
    def _configure_json(
        cls,
        id: str,
        default_path: Optional[str] = None,
        encoder: Optional[json.JSONEncoder] = None,
        decoder: Optional[json.JSONDecoder] = None,
        scope: Optional[Scope] = None,
        **properties,
    ) -> "DataNodeConfig":
        """Configure a new JSON data node configuration.

        Parameters:
            id (str): The unique identifier of the new JSON data node configuration.
            default_path (Optional[str]): The default path of the JSON file.
            encoder (Optional[json.JSONEncoder]): The JSON encoder used to write data into the JSON file.
            decoder (Optional[json.JSONDecoder]): The JSON decoder used to read data from the JSON file.
            scope (Optional[Scope^]): The scope of the JSON data node configuration.<br/>
                The default value is `Scope.SCENARIO`.
            **properties (dict[str, any]): A keyworded variable length list of additional arguments.
        Returns:
            The new JSON data node configuration.
        """
        if default_path is not None:
            properties[cls._OPTIONAL_DEFAULT_PATH_JSON_PROPERTY] = default_path
        if encoder is not None:
            properties[cls._OPTIONAL_ENCODER_JSON_PROPERTY] = encoder
        if decoder is not None:
            properties[cls._OPTIONAL_DECODER_JSON_PROPERTY] = decoder

        return cls.__configure(id, DataNodeConfig._STORAGE_TYPE_VALUE_JSON, scope, **properties)

    @classmethod
    def _configure_parquet(
        cls,
        id: str,
        default_path: Optional[str] = None,
        engine: Optional[str] = None,
        compression: Optional[str] = None,
        read_kwargs: Optional[Dict] = None,
        write_kwargs: Optional[Dict] = None,
        exposed_type: Optional[str] = None,
        scope: Optional[Scope] = None,
        **properties,
    ) -> "DataNodeConfig":
        """Configure a new Parquet data node configuration.

        Parameters:
            id (str): The unique identifier of the new Parquet data node configuration.
            default_path (Optional[str]): The default path of the Parquet file.
            engine (Optional[str]): Parquet library to use. Possible values are *"fastparquet"* or
                *"pyarrow"*.<br/>
                The default value is *"pyarrow"*.
            compression (Optional[str]): Name of the compression to use. Possible values are *"snappy"*,
                *"gzip"*, *"brotli"*, or *"none"* (no compression). The default value is *"snappy"*.
            read_kwargs (Optional[dict]): Additional parameters passed to the `pandas.read_parquet()`
                function.
            write_kwargs (Optional[dict]): Additional parameters passed to the
                `pandas.DataFrame.write_parquet()` function.<br/>
                The parameters in *read_kwargs* and *write_kwargs* have a **higher precedence** than the
                top-level parameters which are also passed to Pandas.
            exposed_type (Optional[str]): The exposed type of the data read from Parquet file.<br/>
                The default value is `pandas`.
            scope (Optional[Scope^]): The scope of the Parquet data node configuration.<br/>
                The default value is `Scope.SCENARIO`.
            **properties (dict[str, any]): A keyworded variable length list of additional arguments.

        Returns:
            The new Parquet data node configuration.
        """
        if default_path is not None:
            properties[cls._OPTIONAL_DEFAULT_PATH_PARQUET_PROPERTY] = default_path
        if engine is not None:
            properties[cls._OPTIONAL_ENGINE_PARQUET_PROPERTY] = engine
        if compression is not None:
            properties[cls._OPTIONAL_COMPRESSION_PARQUET_PROPERTY] = compression
        if read_kwargs is not None:
            properties[cls._OPTIONAL_READ_KWARGS_PARQUET_PROPERTY] = read_kwargs
        if write_kwargs is not None:
            properties[cls._OPTIONAL_WRITE_KWARGS_PARQUET_PROPERTY] = write_kwargs
        if exposed_type is not None:
            properties[cls._OPTIONAL_EXPOSED_TYPE_PARQUET_PROPERTY] = exposed_type

        return cls.__configure(id, DataNodeConfig._STORAGE_TYPE_VALUE_PARQUET, scope, **properties)

    @classmethod
    def _configure_excel(
        cls,
        id: str,
        default_path: Optional[str] = None,
        has_header: Optional[bool] = None,
        sheet_name: Optional[Union[List[str], str]] = None,
        exposed_type: Optional[str] = None,
        scope: Optional[Scope] = None,
        **properties,
    ) -> "DataNodeConfig":
        """Configure a new Excel data node configuration.

        Parameters:
            id (str): The unique identifier of the new Excel data node configuration.
            default_path (Optional[str]): The path of the Excel file.
            has_header (Optional[bool]): If True, indicates that the Excel file has a header.
            sheet_name (Optional[Union[List[str], str]]): The list of sheet names to be used.
                This can be a unique name.
            exposed_type (Optional[str]): The exposed type of the data read from Excel file.<br/>
                The default value is `pandas`.
            scope (Optional[Scope^]): The scope of the Excel data node configuration.<br/>
                The default value is `Scope.SCENARIO`.
            **properties (dict[str, any]): A keyworded variable length list of additional arguments.

        Returns:
            The new Excel data node configuration.
        """
        if default_path is not None:
            properties[cls._OPTIONAL_DEFAULT_PATH_EXCEL_PROPERTY] = default_path
        if has_header is not None:
            properties[cls._OPTIONAL_HAS_HEADER_EXCEL_PROPERTY] = has_header
        if sheet_name is not None:
            properties[cls._OPTIONAL_SHEET_NAME_EXCEL_PROPERTY] = sheet_name
        if exposed_type is not None:
            properties[cls._OPTIONAL_EXPOSED_TYPE_EXCEL_PROPERTY] = exposed_type

        return cls.__configure(id, DataNodeConfig._STORAGE_TYPE_VALUE_EXCEL, scope, **properties)

    @classmethod
    def _configure_generic(
        cls,
        id: str,
        read_fct: Optional[Callable] = None,
        write_fct: Optional[Callable] = None,
        read_fct_args: Optional[List] = None,
        write_fct_args: Optional[List] = None,
        scope: Optional[Scope] = None,
        **properties,
    ) -> "DataNodeConfig":
        """Configure a new generic data node configuration.

        Parameters:
            id (str): The unique identifier of the new generic data node configuration.
            read_fct (Optional[Callable]): The Python function called to read the data.
            write_fct (Optional[Callable]): The Python function called to write the data.
                The provided function must have at least one parameter that receives the data to be written.
            read_fct_args (Optional[List]): The list of arguments that are passed to the function
                *read_fct* to read data.
            write_fct_args (Optional[List]): The list of arguments that are passed to the function
                *write_fct* to write the data.
            scope (Optional[Scope^]): The scope of the Generic data node configuration.<br/>
                The default value is `Scope.SCENARIO`.
            **properties (dict[str, any]): A keyworded variable length list of additional arguments.
        Returns:
            The new Generic data node configuration.
        """
        if read_fct is not None:
            properties[cls._OPTIONAL_READ_FUNCTION_GENERIC_PROPERTY] = read_fct
        if write_fct is not None:
            properties[cls._OPTIONAL_WRITE_FUNCTION_GENERIC_PROPERTY] = write_fct
        if read_fct_args is not None:
            properties[cls._OPTIONAL_READ_FUNCTION_ARGS_GENERIC_PROPERTY] = read_fct_args
        if write_fct_args is not None:
            properties[cls._OPTIONAL_WRITE_FUNCTION_ARGS_GENERIC_PROPERTY] = write_fct_args

        return cls.__configure(id, DataNodeConfig._STORAGE_TYPE_VALUE_GENERIC, scope, **properties)

    @classmethod
    def _configure_in_memory(
        cls, id: str, default_data: Optional[Any] = None, scope: Optional[Scope] = None, **properties
    ) -> "DataNodeConfig":
        """Configure a new *in-memory* data node configuration.

        Parameters:
            id (str): The unique identifier of the new in_memory data node configuration.
            default_data (Optional[any]): The default data of the data nodes instantiated from
                this in_memory data node configuration.
            scope (Optional[Scope^]): The scope of the in_memory data node configuration.<br/>
                The default value is `Scope.SCENARIO`.
            **properties (dict[str, any]): A keyworded variable length list of additional arguments.

        Returns:
            The new *in-memory* data node configuration.
        """
        if default_data is not None:
            properties[cls._OPTIONAL_DEFAULT_DATA_IN_MEMORY_PROPERTY] = default_data

        return cls.__configure(id, DataNodeConfig._STORAGE_TYPE_VALUE_IN_MEMORY, scope, **properties)

    @classmethod
    def _configure_pickle(
        cls,
        id: str,
        default_path: Optional[str] = None,
        default_data: Optional[Any] = None,
        scope: Optional[Scope] = None,
        **properties,
    ) -> "DataNodeConfig":
        """Configure a new pickle data node configuration.

        Parameters:
            id (str): The unique identifier of the new pickle data node configuration.
            default_path (Optional[str]): The path of the pickle file.
            default_data (Optional[any]): The default data of the data nodes instantiated from
                this pickle data node configuration.
            scope (Optional[Scope^]): The scope of the pickle data node configuration.<br/>
                The default value is `Scope.SCENARIO`.
            **properties (dict[str, any]): A keyworded variable length list of additional arguments.

        Returns:
            The new pickle data node configuration.
        """
        if default_path is not None:
            properties[cls._OPTIONAL_DEFAULT_PATH_PICKLE_PROPERTY] = default_path
        if default_data is not None:
            properties[cls._OPTIONAL_DEFAULT_DATA_PICKLE_PROPERTY] = default_data

        return cls.__configure(id, DataNodeConfig._STORAGE_TYPE_VALUE_PICKLE, scope, **properties)

    @classmethod
    def _configure_sql_table(
        cls,
        id: str,
        db_name: str,
        db_engine: str,
        table_name: str,
        db_username: Optional[str] = None,
        db_password: Optional[str] = None,
        db_host: Optional[str] = None,
        db_port: Optional[int] = None,
        db_driver: Optional[str] = None,
        sqlite_folder_path: Optional[str] = None,
        sqlite_file_extension: Optional[str] = None,
        db_extra_args: Optional[Dict[str, Any]] = None,
        exposed_type: Optional[str] = None,
        scope: Optional[Scope] = None,
        **properties,
    ) -> "DataNodeConfig":
        """Configure a new SQL table data node configuration.

        Parameters:
            id (str): The unique identifier of the new SQL data node configuration.
            db_name (str): The database name, or the name of the SQLite database file.
            db_engine (str): The database engine. Possible values are *"sqlite"*, *"mssql"*, *"mysql"*,
                or *"postgresql"*.
            table_name (str): The name of the SQL table.
            db_username (Optional[str]): The database username. Required by the *"mssql"*, *"mysql"*, and
                *"postgresql"* engines.
            db_password (Optional[str]): The database password. Required by the *"mssql"*, *"mysql"*, and
                *"postgresql"* engines.
            db_host (Optional[str]): The database host.<br/>
                The default value is "localhost".
            db_port (Optional[int]): The database port.<br/>
                The default value is 1433.
            db_driver (Optional[str]): The database driver.
            sqlite_folder_path (Optional[str]): The path to the folder that contains SQLite file.<br/>
                The default value is the current working folder.
            sqlite_file_extension (Optional[str]): The file extension of the SQLite file.<br/>
                The default value is ".db".
            db_extra_args (Optional[dict[str, any]]): A dictionary of additional arguments to be passed
                into database connection string.
            exposed_type (Optional[str]): The exposed type of the data read from SQL table.<br/>
                The default value is "pandas".
            scope (Optional[Scope^]): The scope of the SQL data node configuration.<br/>
                The default value is `Scope.SCENARIO`.
            **properties (dict[str, any]): A keyworded variable length list of additional arguments.

        Returns:
            The new SQL data node configuration.
        """
        properties.update(
            {
                cls._REQUIRED_DB_NAME_SQL_PROPERTY: db_name,
                cls._REQUIRED_DB_ENGINE_SQL_PROPERTY: db_engine,
                cls._REQUIRED_TABLE_NAME_SQL_TABLE_PROPERTY: table_name,
            }
        )

        if db_username is not None:
            properties[cls._OPTIONAL_DB_USERNAME_SQL_PROPERTY] = db_username
        if db_password is not None:
            properties[cls._OPTIONAL_DB_PASSWORD_SQL_PROPERTY] = db_password
        if db_host is not None:
            properties[cls._OPTIONAL_HOST_SQL_PROPERTY] = db_host
        if db_port is not None:
            properties[cls._OPTIONAL_PORT_SQL_PROPERTY] = db_port
        if db_driver is not None:
            properties[cls._OPTIONAL_DRIVER_SQL_PROPERTY] = db_driver
        if sqlite_folder_path is not None:
            properties[cls._OPTIONAL_FOLDER_PATH_SQLITE_PROPERTY] = sqlite_folder_path
        if sqlite_file_extension is not None:
            properties[cls._OPTIONAL_FILE_EXTENSION_SQLITE_PROPERTY] = sqlite_file_extension
        if db_extra_args is not None:
            properties[cls._OPTIONAL_DB_EXTRA_ARGS_SQL_PROPERTY] = db_extra_args
        if exposed_type is not None:
            properties[cls._OPTIONAL_EXPOSED_TYPE_SQL_PROPERTY] = exposed_type

        return cls.__configure(id, DataNodeConfig._STORAGE_TYPE_VALUE_SQL_TABLE, scope, **properties)

    @classmethod
    def _configure_sql(
        cls,
        id: str,
        db_name: str,
        db_engine: str,
        read_query: str,
        write_query_builder: Callable,
        db_username: Optional[str] = None,
        db_password: Optional[str] = None,
        db_host: Optional[str] = None,
        db_port: Optional[int] = None,
        db_driver: Optional[str] = None,
        sqlite_folder_path: Optional[str] = None,
        sqlite_file_extension: Optional[str] = None,
        db_extra_args: Optional[Dict[str, Any]] = None,
        exposed_type: Optional[str] = None,
        scope: Optional[Scope] = None,
        **properties,
    ) -> "DataNodeConfig":
        """Configure a new SQL data node configuration.

        Parameters:
            id (str): The unique identifier of the new SQL data node configuration.
            db_name (str): The database name, or the name of the SQLite database file.
            db_engine (str): The database engine. Possible values are *"sqlite"*, *"mssql"*, *"mysql"*,
                or *"postgresql"*.
            read_query (str): The SQL query string used to read the data from the database.
            write_query_builder (Callable): A callback function that takes the data as an input parameter
                and returns a list of SQL queries.
            db_username (Optional[str]): The database username. Required by the *"mssql"*, *"mysql"*, and
                *"postgresql"* engines.
            db_password (Optional[str]): The database password. Required by the *"mssql"*, *"mysql"*, and
                *"postgresql"* engines.
            db_host (Optional[str]): The database host.<br/>
                The default value is "localhost".
            db_port (Optional[int]): The database port.<br/>
                The default value is 1433.
            db_driver (Optional[str]): The database driver.
            sqlite_folder_path (Optional[str]): The path to the folder that contains SQLite file.<br/>
                The default value is the current working folder.
            sqlite_file_extension (Optional[str]): The file extension of the SQLite file.<br/>
                The default value is ".db".
            db_extra_args (Optional[dict[str, any]]): A dictionary of additional arguments to be passed
                into database connection string.
            exposed_type (Optional[str]): The exposed type of the data read from SQL query.<br/>
                The default value is "pandas".
            scope (Optional[Scope^]): The scope of the SQL data node configuration.<br/>
                The default value is `Scope.SCENARIO`.
            **properties (dict[str, any]): A keyworded variable length list of additional arguments.
        Returns:
            The new SQL data node configuration.
        """
        properties.update(
            {
                cls._REQUIRED_DB_NAME_SQL_PROPERTY: db_name,
                cls._REQUIRED_DB_ENGINE_SQL_PROPERTY: db_engine,
                cls._REQUIRED_READ_QUERY_SQL_PROPERTY: read_query,
                cls._REQUIRED_WRITE_QUERY_BUILDER_SQL_PROPERTY: write_query_builder,
            }
        )

        if db_username is not None:
            properties[cls._OPTIONAL_DB_USERNAME_SQL_PROPERTY] = db_username
        if db_password is not None:
            properties[cls._OPTIONAL_DB_PASSWORD_SQL_PROPERTY] = db_password
        if db_host is not None:
            properties[cls._OPTIONAL_HOST_SQL_PROPERTY] = db_host
        if db_port is not None:
            properties[cls._OPTIONAL_PORT_SQL_PROPERTY] = db_port
        if db_driver is not None:
            properties[cls._OPTIONAL_DRIVER_SQL_PROPERTY] = db_driver
        if sqlite_folder_path is not None:
            properties[cls._OPTIONAL_FOLDER_PATH_SQLITE_PROPERTY] = sqlite_folder_path
        if sqlite_file_extension is not None:
            properties[cls._OPTIONAL_FILE_EXTENSION_SQLITE_PROPERTY] = sqlite_file_extension
        if db_extra_args is not None:
            properties[cls._OPTIONAL_DB_EXTRA_ARGS_SQL_PROPERTY] = db_extra_args
        if exposed_type is not None:
            properties[cls._OPTIONAL_EXPOSED_TYPE_SQL_PROPERTY] = exposed_type

        return cls.__configure(id, DataNodeConfig._STORAGE_TYPE_VALUE_SQL, scope, **properties)

    @classmethod
    def _configure_mongo_collection(
        cls,
        id: str,
        db_name: str,
        collection_name: str,
        custom_document: Optional[Any] = None,
        db_username: Optional[str] = None,
        db_password: Optional[str] = None,
        db_host: Optional[str] = None,
        db_port: Optional[int] = None,
        db_extra_args: Optional[Dict[str, Any]] = None,
        scope: Optional[Scope] = None,
        **properties,
    ) -> "DataNodeConfig":
        """Configure a new Mongo collection data node configuration.

        Parameters:
            id (str): The unique identifier of the new Mongo collection data node configuration.
            db_name (str): The database name.
            collection_name (str): The collection in the database to read from and to write the data to.
            custom_document (Optional[any]): The custom document class to store, encode, and decode data
                when reading and writing to a Mongo collection. The custom_document can have an optional
                *decode()* method to decode data in the Mongo collection to a custom object, and an
                optional *encode()*) method to encode the object's properties to the Mongo collection
                when writing.
            db_username (Optional[str]): The database username.
            db_password (Optional[str]): The database password.
            db_host (Optional[str]): The database host.<br/>
                The default value is "localhost".
            db_port (Optional[int]): The database port.<br/>
                The default value is 27017.
            db_extra_args (Optional[dict[str, any]]): A dictionary of additional arguments to be passed
                into database connection string.
            scope (Optional[Scope^]): The scope of the Mongo collection data node configuration.<br/>
                The default value is `Scope.SCENARIO`.
            **properties (dict[str, any]): A keyworded variable length list of additional arguments.

        Returns:
            The new Mongo collection data node configuration.
        """
        properties.update(
            {
                cls._REQUIRED_DB_NAME_MONGO_PROPERTY: db_name,
                cls._REQUIRED_COLLECTION_NAME_MONGO_PROPERTY: collection_name,
            }
        )

        if custom_document is not None:
            properties[cls._OPTIONAL_CUSTOM_DOCUMENT_MONGO_PROPERTY] = custom_document
        if db_username is not None:
            properties[cls._OPTIONAL_USERNAME_MONGO_PROPERTY] = db_username
        if db_password is not None:
            properties[cls._OPTIONAL_PASSWORD_MONGO_PROPERTY] = db_password
        if db_host is not None:
            properties[cls._OPTIONAL_HOST_MONGO_PROPERTY] = db_host
        if db_port is not None:
            properties[cls._OPTIONAL_PORT_MONGO_PROPERTY] = db_port
        if db_extra_args is not None:
            properties[cls._OPTIONAL_DB_EXTRA_ARGS_MONGO_PROPERTY] = db_extra_args

        return cls.__configure(id, DataNodeConfig._STORAGE_TYPE_VALUE_MONGO_COLLECTION, scope, **properties)

    @staticmethod
    def __configure(
        id: str,
        storage_type: Optional[str] = None,
        scope: Optional[Scope] = None,
        **properties,
    ):
        section = DataNodeConfig(id, storage_type, scope, **properties)
        Config._register(section)
        return Config.sections[DataNodeConfig.name][id]
