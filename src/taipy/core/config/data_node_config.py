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
from copy import copy
from typing import Any, Callable, Dict, List, Optional, Union

from taipy.config._config import _Config
from taipy.config.common._template_handler import _TemplateHandler as _tpl
from taipy.config.common.scope import Scope
from taipy.config.config import Config
from taipy.config.section import Section


class DataNodeConfig(Section):
    """
    Configuration fields needed to instantiate an actual `DataNode^` from the DataNodeConfig.

    A Data Node config is made to be used as a generator for actual data nodes. It holds configuration information
    needed to create an actual data node.

    Attributes:
        id (str):  Unique identifier of the data node config. It must be a valid Python variable name.
        storage_type (str): Storage type of the data nodes created from the data node config. The possible values
            are : "csv", "excel", "pickle", "sql", "mongo", "generic", "json" and "in_memory". The default value is "pickle".
            Note that the "in_memory" value can only be used when `JobConfig^`.mode is "standalone".
        scope (Scope^):  The `Scope^` of the data nodes instantiated from the data node config. The default value is
            SCENARIO.
        **properties: A dictionary of additional properties.
    """

    name = "DATA_NODE"

    _STORAGE_TYPE_KEY = "storage_type"
    _STORAGE_TYPE_VALUE_PICKLE = "pickle"
    _STORAGE_TYPE_VALUE_SQL = "sql"
    _STORAGE_TYPE_VALUE_MONGO = "mongo"
    _STORAGE_TYPE_VALUE_CSV = "csv"
    _STORAGE_TYPE_VALUE_EXCEL = "excel"
    _STORAGE_TYPE_VALUE_IN_MEMORY = "in_memory"
    _STORAGE_TYPE_VALUE_GENERIC = "generic"
    _STORAGE_TYPE_VALUE_JSON = "json"
    _DEFAULT_STORAGE_TYPE = _STORAGE_TYPE_VALUE_PICKLE
    _ALL_STORAGE_TYPES = [
        _STORAGE_TYPE_VALUE_PICKLE,
        _STORAGE_TYPE_VALUE_SQL,
        _STORAGE_TYPE_VALUE_MONGO,
        _STORAGE_TYPE_VALUE_CSV,
        _STORAGE_TYPE_VALUE_EXCEL,
        _STORAGE_TYPE_VALUE_IN_MEMORY,
        _STORAGE_TYPE_VALUE_GENERIC,
        _STORAGE_TYPE_VALUE_JSON,
    ]

    _EXPOSED_TYPE_KEY = "exposed_type"
    _EXPOSED_TYPE_PANDAS = "pandas"
    _EXPOSED_TYPE_NUMPY = "numpy"
    _DEFAULT_EXPOSED_TYPE = _EXPOSED_TYPE_PANDAS

    _ALL_EXPOSED_TYPES = [
        _EXPOSED_TYPE_PANDAS,
        _EXPOSED_TYPE_NUMPY,
    ]
    # Generic
    _REQUIRED_READ_FUNCTION_GENERIC_PROPERTY = "read_fct"
    _OPTIONAL_READ_FUNCTION_PARAMS_GENERIC_PROPERTY = "read_fct_params"
    _REQUIRED_WRITE_FUNCTION_GENERIC_PROPERTY = "write_fct"
    _OPTIONAL_WRITE_FUNCTION_PARAMS_GENERIC_PROPERTY = "write_fct_params"
    # CSV
    _OPTIONAL_EXPOSED_TYPE_CSV_PROPERTY = "exposed_type"
    _OPTIONAL_EXPOSED_TYPE_CSV_NUMPY = "numpy"
    _OPTIONAL_DEFAULT_PATH_CSV_PROPERTY = "default_path"
    _OPTIONAL_HAS_HEADER_CSV_PROPERTY = "has_header"
    # Excel
    _OPTIONAL_EXPOSED_TYPE_EXCEL_PROPERTY = "exposed_type"
    _OPTIONAL_EXPOSED_TYPE_EXCEL_NUMPY = "numpy"
    _OPTIONAL_DEFAULT_PATH_EXCEL_PROPERTY = "default_path"
    _OPTIONAL_HAS_HEADER_EXCEL_PROPERTY = "has_header"
    _OPTIONAL_SHEET_NAME_EXCEL_PROPERTY = "sheet_name"
    # In memory
    _OPTIONAL_DEFAULT_DATA_IN_MEMORY_PROPERTY = "default_data"
    # SQL
    _OPTIONAL_EXPOSED_TYPE_SQL_PROPERTY = "exposed_type"
    _OPTIONAL_EXPOSED_TYPE_SQL_NUMPY = "numpy"
    _REQUIRED_DB_USERNAME_SQL_PROPERTY = "db_username"
    _REQUIRED_DB_PASSWORD_SQL_PROPERTY = "db_password"
    _REQUIRED_DB_NAME_SQL_PROPERTY = "db_name"
    _REQUIRED_DB_ENGINE_SQL_PROPERTY = "db_engine"
    _REQUIRED_DB_ENGINE_SQLITE = "sqlite"
    _REQUIRED_DB_ENGINE_MSSQL = "mssql"
    _REQUIRED_READ_QUERY_SQL_PROPERTY = "read_query"
    _REQUIRED_WRITE_TABLE_SQL_PROPERTY = "write_table"
    _OPTIONAL_DB_EXTRA_ARGS_SQL_PROPERTY = "db_extra_args"
    # MONGO
    _OPTIONAL_EXPOSED_TYPE_MONGO_PROPERTY = "exposed_type"
    _OPTIONAL_EXPOSED_TYPE_MONGO_NUMPY = "numpy"
    _OPTIONAL_ENCODER_MONGO_PROPERTY = "encoder"
    _OPTIONAL_DECODER_TYPE_MONGO_PROPERTY = "decoder"
    _REQUIRED_DB_USERNAME_MONGO_PROPERTY = "db_username"
    _REQUIRED_DB_PASSWORD_MONGO_PROPERTY = "db_password"
    _REQUIRED_DB_NAME_MONGO_PROPERTY = "db_name"
    _REQUIRED_READ_COLLECTION_MONGO_PROPERTY = "read_collection"
    _REQUIRED_READ_QUERY_MONGO_PROPERTY = "read_query"
    _REQUIRED_WRITE_COLLECTION_MONGO_PROPERTY = "write_collection"
    # Pickle
    _OPTIONAL_DEFAULT_PATH_PICKLE_PROPERTY = "default_path"
    _OPTIONAL_DEFAULT_DATA_PICKLE_PROPERTY = "default_data"
    # JSON
    _OPTIONAL_ENCODER_JSON_PROPERTY = "encoder"
    _OPTIONAL_DECODER_TYPE_JSON_PROPERTY = "decoder"
    _REQUIRED_DEFAULT_PATH_JSON_PROPERTY = "default_path"

    _REQUIRED_PROPERTIES: Dict[str, List] = {
        _STORAGE_TYPE_VALUE_PICKLE: [],
        _STORAGE_TYPE_VALUE_SQL: [
            _REQUIRED_DB_USERNAME_SQL_PROPERTY,
            _REQUIRED_DB_PASSWORD_SQL_PROPERTY,
            _REQUIRED_DB_NAME_SQL_PROPERTY,
            _REQUIRED_DB_ENGINE_SQL_PROPERTY,
            _REQUIRED_READ_QUERY_SQL_PROPERTY,
            _REQUIRED_WRITE_TABLE_SQL_PROPERTY,
        ],
        _STORAGE_TYPE_VALUE_MONGO: [
            _REQUIRED_DB_USERNAME_MONGO_PROPERTY,
            _REQUIRED_DB_PASSWORD_MONGO_PROPERTY,
            _REQUIRED_DB_NAME_MONGO_PROPERTY,
            _REQUIRED_READ_COLLECTION_MONGO_PROPERTY,
            _REQUIRED_READ_QUERY_MONGO_PROPERTY,
            _REQUIRED_WRITE_COLLECTION_MONGO_PROPERTY,
        ],
        _STORAGE_TYPE_VALUE_CSV: [],
        _STORAGE_TYPE_VALUE_EXCEL: [],
        _STORAGE_TYPE_VALUE_IN_MEMORY: [],
        _STORAGE_TYPE_VALUE_GENERIC: [
            _REQUIRED_READ_FUNCTION_GENERIC_PROPERTY,
            _REQUIRED_WRITE_FUNCTION_GENERIC_PROPERTY,
        ],
        _STORAGE_TYPE_VALUE_JSON: [_REQUIRED_DEFAULT_PATH_JSON_PROPERTY],
    }

    _OPTIONAL_PROPERTIES = {
        _STORAGE_TYPE_VALUE_GENERIC: [
            _OPTIONAL_READ_FUNCTION_PARAMS_GENERIC_PROPERTY,
            _OPTIONAL_WRITE_FUNCTION_PARAMS_GENERIC_PROPERTY,
        ],
        _STORAGE_TYPE_VALUE_CSV: [
            _OPTIONAL_EXPOSED_TYPE_CSV_PROPERTY,
            _OPTIONAL_DEFAULT_PATH_CSV_PROPERTY,
            _OPTIONAL_HAS_HEADER_CSV_PROPERTY,
        ],
        _STORAGE_TYPE_VALUE_EXCEL: [
            _OPTIONAL_EXPOSED_TYPE_EXCEL_PROPERTY,
            _OPTIONAL_DEFAULT_PATH_EXCEL_PROPERTY,
            _OPTIONAL_HAS_HEADER_EXCEL_PROPERTY,
            _OPTIONAL_SHEET_NAME_EXCEL_PROPERTY,
        ],
        _STORAGE_TYPE_VALUE_IN_MEMORY: [_OPTIONAL_DEFAULT_DATA_IN_MEMORY_PROPERTY],
        _STORAGE_TYPE_VALUE_SQL: [_OPTIONAL_EXPOSED_TYPE_SQL_PROPERTY, _OPTIONAL_DB_EXTRA_ARGS_SQL_PROPERTY],
        _STORAGE_TYPE_VALUE_MONGO: [
            _OPTIONAL_EXPOSED_TYPE_MONGO_PROPERTY,
            _OPTIONAL_ENCODER_MONGO_PROPERTY,
            _OPTIONAL_DECODER_TYPE_MONGO_PROPERTY,
        ],
        _STORAGE_TYPE_VALUE_PICKLE: [_OPTIONAL_DEFAULT_PATH_PICKLE_PROPERTY, _OPTIONAL_DEFAULT_DATA_PICKLE_PROPERTY],
        _STORAGE_TYPE_VALUE_JSON: [_OPTIONAL_ENCODER_JSON_PROPERTY, _OPTIONAL_DECODER_TYPE_JSON_PROPERTY],
    }

    _SCOPE_KEY = "scope"
    _DEFAULT_SCOPE = Scope.SCENARIO

    _IS_CACHEABLE_KEY = "cacheable"
    _DEFAULT_IS_CACHEABLE_VALUE = False

    def __init__(self, id: str, storage_type: str = None, scope: Scope = None, **properties):
        self._storage_type = storage_type
        self._scope = scope
        super().__init__(id, **properties)
        if self._properties.get(self._IS_CACHEABLE_KEY) is None:
            self._properties[self._IS_CACHEABLE_KEY] = self._DEFAULT_IS_CACHEABLE_VALUE

    def __copy__(self):
        return DataNodeConfig(self.id, self._storage_type, self._scope, **copy(self._properties))

    def __getattr__(self, item: str) -> Optional[Any]:
        return _tpl._replace_templates(self._properties.get(item))

    @property
    def storage_type(self):
        return _tpl._replace_templates(self._storage_type)

    @storage_type.setter  # type: ignore
    def storage_type(self, val):
        self._storage_type = val

    @property
    def scope(self):
        return _tpl._replace_templates(self._scope)

    @scope.setter  # type: ignore
    def scope(self, val):
        self._scope = val

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
            self._scope = default_section.scope
        self._properties.update(as_dict)
        if default_section:
            self._properties = {**default_section.properties, **self._properties}

    @staticmethod
    def _configure_default(storage_type: str, scope: Scope = _DEFAULT_SCOPE, **properties):
        """Configure the default values for data node configurations.
        This function creates the _default data node configuration_ object,
        where all data node configuration objects will find their default
        values when needed.
        Parameters:
            storage_type (str): The default storage type for all data node configurations.
                The possible values are _"pickle"_ (the default value), _"csv"_, _"excel"_,
                _"sql"_, _"mongo"_, _"in_memory"_, _"json"_ or _"generic"_.
            scope (Scope^): The default scope fot all data node configurations.
                The default value is `Scope.SCENARIO`.
            **properties (Dict[str, Any]): A keyworded variable length list of additional
                arguments.
        Returns:
            DataNodeConfig^: The default data node configuration.
        """
        section = DataNodeConfig(_Config.DEFAULT_KEY, storage_type, scope, **properties)
        Config._register(section)
        return Config.sections[DataNodeConfig.name][_Config.DEFAULT_KEY]

    @staticmethod
    def _configure(id: str, storage_type: str = _DEFAULT_STORAGE_TYPE, scope: Scope = _DEFAULT_SCOPE, **properties):
        """Configure a new data node configuration.
        Parameters:
            id (str): The unique identifier of the new data node configuration.
            storage_type (str): The data node configuration storage type. The possible values
                are _"pickle"_ (which the default value, unless it has been overloaded by the
                _storage_type_ value set in the default data node configuration
                (see `(Config.)configure_default_data_node()^`)), _"csv"_, _"excel"_, _"sql"_, _"mongo"_,
                _"in_memory"_, or _"generic"_.
            scope (Scope^): The scope of the data node configuration. The default value is
                `Scope.SCENARIO` (or the one specified in
                `(Config.)configure_default_data_node()^`).
            **properties (Dict[str, Any]): A keyworded variable length list of additional
                arguments.
        Returns:
            DataNodeConfig^: The new data node configuration.
        """
        section = DataNodeConfig(id, storage_type, scope, **properties)
        Config._register(section)
        return Config.sections[DataNodeConfig.name][id]

    @staticmethod
    def _configure_csv(
        id: str,
        default_path: str = None,
        has_header: bool = True,
        exposed_type=_EXPOSED_TYPE_PANDAS,
        scope=_DEFAULT_SCOPE,
        **properties,
    ):
        """Configure a new CSV data node configuration.

        Parameters:
            id (str): The unique identifier of the new CSV data node configuration.
            default_path (str): The default path of the CSV file.
            has_header (bool): If True, indicates that the CSV file has a header.
            exposed_type: The exposed type of the data read from CSV file. The default value is `pandas`.
            scope (Scope^): The scope of the CSV data node configuration. The default value
                is `Scope.SCENARIO`.
            **properties (Dict[str, Any]): A keyworded variable length list of additional
                arguments.
        Returns:
            DataNodeConfig^: The new CSV data node configuration.
        """
        section = DataNodeConfig(
            id,
            DataNodeConfig._STORAGE_TYPE_VALUE_CSV,
            scope=scope,
            default_path=default_path,
            has_header=has_header,
            exposed_type=exposed_type,
            **properties,
        )
        Config._register(section)
        return Config.sections[DataNodeConfig.name][id]

    @staticmethod
    def _configure_json(
        id: str,
        default_path: str = None,
        encoder: json.JSONEncoder = None,
        decoder: json.JSONDecoder = None,
        scope=_DEFAULT_SCOPE,
        **properties,
    ):
        """Configure a new JSON data node configuration.

        Parameters:
            id (str): The unique identifier of the new JSON data node configuration.
            default_path (str): The default path of the JSON file.
            encoder (json.JSONEncoder): The JSON encoder used to write data into the JSON file.
            decoder (json.JSONDecoder): The JSON decoder used to read data from the JSON file.
            scope (Scope^): The scope of the JSON data node configuration. The default value
                is `Scope.SCENARIO`.
            **properties (Dict[str, Any]): A keyworded variable length list of additional
                arguments.
        Returns:
            DataNodeConfig^: The new JSON data node configuration.
        """
        section = DataNodeConfig(
            id,
            DataNodeConfig._STORAGE_TYPE_VALUE_JSON,
            scope=scope,
            default_path=default_path,
            encoder=encoder,
            decoder=decoder,
            **properties,
        )
        Config._register(section)
        return Config.sections[DataNodeConfig.name][id]

    @staticmethod
    def _configure_excel(
        id: str,
        default_path: str = None,
        has_header: bool = True,
        sheet_name: Union[List[str], str] = None,
        exposed_type=_EXPOSED_TYPE_PANDAS,
        scope: Scope = _DEFAULT_SCOPE,
        **properties,
    ):
        """Configure a new Excel data node configuration.

        Parameters:
            id (str): The unique identifier of the new Excel data node configuration.
            default_path (str): The path of the Excel file.
            has_header (bool): If True, indicates that the Excel file has a header.
            sheet_name (Union[List[str], str]): The list of sheet names to be used. This
                can be a unique name.
            exposed_type: The exposed type of the data read from Excel file. The default value is `pandas`.
            scope (Scope^): The scope of the Excel data node configuration. The default
                value is `Scope.SCENARIO`.
            **properties (Dict[str, Any]): A keyworded variable length list of additional
                arguments.
        Returns:
            DataNodeConfig^: The new CSV data node configuration.
        """
        section = DataNodeConfig(
            id,
            DataNodeConfig._STORAGE_TYPE_VALUE_EXCEL,
            scope=scope,
            default_path=default_path,
            has_header=has_header,
            sheet_name=sheet_name,
            exposed_type=exposed_type,
            **properties,
        )
        Config._register(section)
        return Config.sections[DataNodeConfig.name][id]

    @staticmethod
    def _configure_generic(
        id: str,
        read_fct: Callable = None,
        write_fct: Callable = None,
        read_fct_params: List = None,
        write_fct_params: List = None,
        scope: Scope = _DEFAULT_SCOPE,
        **properties,
    ):
        """Configure a new generic data node configuration.

        Parameters:
            id (str): The unique identifier of the new generic data node configuration.
            read_fct (Optional[Callable]): The Python function called to read the data.
            write_fct (Optional[Callable]): The Python function called to write the data.
                The provided function must have at least one parameter that receives the data
                to be written.
            read_fct_params (Optional[List]): The parameters that are passed to _read_fct_
                to read the data.
            write_fct_params (Optional[List]): The parameters that are passed to _write_fct_
                to write the data.
            scope (Optional[Scope^]): The scope of the Generic data node configuration.
                The default value is `Scope.SCENARIO`.
            **properties (Dict[str, Any]): A keyworded variable length list of additional
                arguments.
        Returns:
            DataNodeConfig^: The new Generic data node configuration.
        """
        section = DataNodeConfig(
            id,
            DataNodeConfig._STORAGE_TYPE_VALUE_GENERIC,
            scope=scope,
            read_fct=read_fct,
            write_fct=write_fct,
            read_fct_params=read_fct_params,
            write_fct_params=write_fct_params,
            **properties,
        )
        Config._register(section)
        return Config.sections[DataNodeConfig.name][id]

    @staticmethod
    def _configure_in_memory(id: str, default_data: Optional[Any] = None, scope: Scope = _DEFAULT_SCOPE, **properties):
        """Configure a new _in_memory_ data node configuration.

        Parameters:
            id (str): The unique identifier of the new in_memory data node configuration.
            default_data (Optional[Any]): The default data of the data nodes instantiated from
                this in_memory data node configuration.
            scope (Scope^): The scope of the in_memory data node configuration. The default
                value is `Scope.SCENARIO`.
            **properties (Dict[str, Any]): A keyworded variable length list of additional
                arguments.
        Returns:
            DataNodeConfig^: The new _in_memory_ data node configuration.
        """
        section = DataNodeConfig(
            id, DataNodeConfig._STORAGE_TYPE_VALUE_IN_MEMORY, scope=scope, default_data=default_data, **properties
        )
        Config._register(section)
        return Config.sections[DataNodeConfig.name][id]

    @staticmethod
    def _configure_pickle(id: str, default_data: Optional[Any] = None, scope: Scope = _DEFAULT_SCOPE, **properties):
        """Configure a new pickle data node configuration.

        Parameters:
            id (str): The unique identifier of the new pickle data node configuration.
            default_data (Optional[Any]): The default data of the data nodes instantiated from
                this pickle data node configuration.
            scope (Scope^): The scope of the pickle data node configuration. The default value
                is `Scope.SCENARIO`.
            **properties (Dict[str, Any]): A keyworded variable length list of additional
                arguments.
        Returns:
            DataNodeConfig^: The new pickle data node configuration.
        """
        section = DataNodeConfig(
            id, DataNodeConfig._STORAGE_TYPE_VALUE_PICKLE, scope=scope, default_data=default_data, **properties
        )
        Config._register(section)
        return Config.sections[DataNodeConfig.name][id]

    @staticmethod
    def _configure_sql(
        id: str,
        db_username: str,
        db_password: str,
        db_name: str,
        db_engine: str,
        read_query: str,
        write_table: str = None,
        db_port: int = 1433,
        db_host: str = "localhost",
        db_driver: str = "ODBC Driver 17 for SQL Server",
        db_extra_args: Dict[str, Any] = None,
        exposed_type=_EXPOSED_TYPE_PANDAS,
        scope: Scope = _DEFAULT_SCOPE,
        **properties,
    ):
        """Configure a new SQL data node configuration.

        Parameters:
            id (str): The unique identifier of the new SQL data node configuration.
            db_username (str): The database username.
            db_password (str): The database password.
            db_name (str): The database name.
            db_engine (str): The database engine. Possible values are _"sqlite"_ or _"mssql"_.
            read_query (str): The SQL query string used to read the data from the database.
            write_table (str): The name of the table in the database to write the data to.
            db_port (int): The database port. The default value is 1433.
            db_host (str): The database host. The default value is _"localhost"_.
            db_driver (str): The database driver. The default value is
                _"ODBC Driver 17 for SQL Server"_.
            db_extra_args (Dict[str, Any]): A dictionary of additional arguments to be passed into database
                connection string.
            exposed_type: The exposed type of the data read from SQL query. The default value is `pandas`.
            scope (Scope^): The scope of the SQL data node configuration. The default value is
                `Scope.SCENARIO`.
            **properties (Dict[str, Any]): A keyworded variable length list of additional
                arguments.
        Returns:
            DataNodeConfig^: The new SQL data node configuration.
        """
        section = DataNodeConfig(
            id,
            DataNodeConfig._STORAGE_TYPE_VALUE_SQL,
            scope=scope,
            db_username=db_username,
            db_password=db_password,
            db_name=db_name,
            db_host=db_host,
            db_engine=db_engine,
            db_driver=db_driver,
            read_query=read_query,
            write_table=write_table,
            db_port=db_port,
            db_extra_args=db_extra_args,
            exposed_type=exposed_type,
            **properties,
        )
        Config._register(section)
        return Config.sections[DataNodeConfig.name][id]

    @staticmethod
    def _configure_mongo(
        id: str,
        db_username: str,
        db_password: str,
        db_name: str,
        read_collection: str,
        read_query: str,
        write_collection: str = None,
        db_port: int = 27017,
        db_host: str = "localhost",
        encoder: json.JSONEncoder = None,
        decoder: json.JSONDecoder = None,
        exposed_type=_EXPOSED_TYPE_PANDAS,
        scope: Scope = _DEFAULT_SCOPE,
        **properties,
    ):
        """Configure a new Mongo data node configuration.

        Parameters:
            id (str): The unique identifier of the new Mongo data node configuration.
            db_username (str): The database username.
            db_password (str): The database password.
            db_name (str): The database name.
            read_collection (str): The collection in the database to read from.
            read_query (str): The Mongo query string used to read the data from the database.
            write_collection (str): The collection in the database to write the data to.
            db_port (int): The database port. The default value is 27017.
            db_host (str): The database host. The default value is _"localhost"_.
            encoder (json.JSONEncoder): The JSON encoder that is used to encode different object to acceptable JSON object.
            decoder (json.JSONDecoder): The JSON decoder that is used to read from JSON object.
            exposed_type: The exposed type of the data read from Mongo query. The default value is `pandas`.
            scope (Scope^): The scope of the Mongo data node configuration. The default value is
                `Scope.SCENARIO`.
            **properties (Dict[str, Any]): A keyworded variable length list of additional
                arguments.
        Returns:
            DataNodeConfig^: The new Mongo data node configuration.
        """
        section = DataNodeConfig(
            id,
            DataNodeConfig._STORAGE_TYPE_VALUE_MONGO,
            scope=scope,
            db_username=db_username,
            db_password=db_password,
            db_name=db_name,
            read_collection=read_collection,
            db_host=db_host,
            read_query=read_query,
            write_collection=write_collection,
            db_port=db_port,
            encoder=encoder,
            decoder=decoder,
            exposed_type=exposed_type,
            **properties,
        )
        Config._register(section)
        return Config.sections[DataNodeConfig.name][id]
