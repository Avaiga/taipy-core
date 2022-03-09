import os
import urllib.parse
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

import numpy as np
import numpy.typing as npt
import pandas as pd
from sqlalchemy import create_engine, table, text

from taipy.core.common.alias import DataNodeId, JobId
from taipy.core.data.data_node import DataNode
from taipy.core.data.scope import Scope
from taipy.core.exceptions.data_node import MissingRequiredProperty, UnknownDatabaseEngine


class SQLDataNode(DataNode):
    """
    A Data Node stored as an SQL database.

    Attributes:
        config_id (str): Identifier of the data node configuration. It must be a valid Python variable name.
        scope (`Scope^`): The `Scope^` of the data node.
        id (str): The unique identifier of the data node.
        name (str): A user-readable name of the data node.
        parent_id (str): The identifier of the parent (pipeline_id, scenario_id, cycle_id) or `None`.
        last_edition_date (datetime): The date and time of the last edition.
        job_ids (List[str]): The ordered list of jobs that have written this data node.
        validity_period (Optional[timedelta]): The validity period of a cacheable data node. Implemented as a
            timedelta. If _validity_period_ is set to None, the data_node is always up-to-date.
        edition_in_progress (bool): True if a task computing the data node has been submitted and not completed yet.
            False otherwise.
        properties (dict[str, Any]): A dictionary of additional properties. Note that the _properties_ parameter must
            at least contain an entry for "db_username", "db_password", "db_name", "db_engine", "read_query", and
            "write_table".
    """

    __STORAGE_TYPE = "sql"
    __EXPOSED_TYPE_NUMPY = "numpy"
    __EXPOSED_TYPE_PROPERTY = "exposed_type"
    _REQUIRED_PROPERTIES: List[str] = [
        "db_username",
        "db_password",
        "db_name",
        "db_engine",
        "read_query",
        "write_table",
    ]

    def __init__(
        self,
        config_id: str,
        scope: Scope,
        id: Optional[DataNodeId] = None,
        name: Optional[str] = None,
        parent_id: Optional[str] = None,
        last_edition_date: Optional[datetime] = None,
        job_ids: List[JobId] = None,
        validity_period: Optional[timedelta] = None,
        edition_in_progress: bool = False,
        properties: Dict = None,
    ):
        if properties is None:
            properties = {}
        required = (
            self._REQUIRED_PROPERTIES
            if properties.get("db_engine") != "sqlite"
            else ["db_name", "read_query", "write_table"]
        )
        if missing := set(required) - set(properties.keys()):
            raise MissingRequiredProperty(
                f"The following properties " f"{', '.join(x for x in missing)} were not informed and are required"
            )

        self.__engine = self.__create_engine(
            properties.get("db_engine"),
            properties.get("db_username"),
            properties.get("db_host", "localhost"),
            properties.get("db_password"),
            properties.get("db_name"),
            properties.get("db_port", 1433),
            properties.get("path", ""),
        )

        super().__init__(
            config_id,
            scope,
            id,
            name,
            parent_id,
            last_edition_date,
            job_ids,
            validity_period,
            edition_in_progress,
            **properties,
        )
        if not self._last_edition_date:
            self.unlock_edition()

    @staticmethod
    def __build_conn_string(engine, username, host, password, database, port, path) -> str:
        # TODO: Add support to other SQL engines
        if engine == "mssql":
            return "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(
                f"DRIVER=FreeTDS;SERVER={host};PORT={port};DATABASE={database};UID={username};PWD={password};TDS_Version=8.0;"
            )
        elif engine == "sqlite":
            return os.path.join("sqlite:///", path, f"{database}.sqlite3")
        raise UnknownDatabaseEngine(f"Unknown engine: {engine}")

    def __create_engine(self, engine, username, host, password, database, port=1433, path=""):
        conn_str = self.__build_conn_string(engine, username, host, password, database, port, path)
        return create_engine(conn_str)

    @classmethod
    def storage_type(cls) -> str:
        return cls.__STORAGE_TYPE

    def _read(self):
        if self.__EXPOSED_TYPE_PROPERTY in self.properties:
            if self.properties[self.__EXPOSED_TYPE_PROPERTY] == self.__EXPOSED_TYPE_NUMPY:
                return self._read_as_numpy()
            return self._read_as(self.properties[self.__EXPOSED_TYPE_PROPERTY])
        return self._read_as_pandas_dataframe()

    def _read_as(self, query, custom_class):
        with self.__engine.connect() as connection:
            query_result = connection.dispatch(text(query))
        return list(map(lambda row: custom_class(**row), query_result))

    def _read_as_numpy(self):
        return self._read_as_pandas_dataframe().to_numpy()

    def _read_as_pandas_dataframe(self, columns: Optional[List[str]] = None):
        if columns:
            return pd.read_sql_query(self.read_query, con=self.__engine)[columns]
        return pd.read_sql_query(self.read_query, con=self.__engine)

    def _write(self, data) -> None:
        """
        Check data against a collection of types to handle insertion on the database.
        """
        with self.__engine.connect() as connection:
            write_table = table(self.write_table)
            if isinstance(data, pd.DataFrame):
                self.__insert_dicts(data.to_dict(orient="records"), write_table, connection)
            elif isinstance(data, dict):
                self.__insert_dicts([data], write_table, connection)
            elif isinstance(data, tuple):
                self.__insert_tuples([data], write_table, connection)
            elif isinstance(data, list) or isinstance(data, np.ndarray):
                if isinstance(data[0], tuple):
                    self.__insert_tuples(data, write_table, connection)
                elif isinstance(data[0], dict):
                    self.__insert_dicts(data, write_table, connection)
                else:
                    self.__insert_list(data, write_table, connection)
                    return
            else:
                # if data is a single primitive value (int, str, etc), pass it as a list of tuples
                # with only one value
                self.__insert_tuples([(data,)], write_table, connection)

    @staticmethod
    def __insert_list(data: Union[List[Any], npt.NDArray[Any]], write_table: Any, connection: Any) -> None:
        """
        :param data: a list of values
        :param write_table: a SQLAlchemy object that represents a table
        :param connection: a SQLAlchemy connection to write the data

        This method will lookup the length of the first object of the list and build the insert through
        creation of a string of '?' equivalent to the length of the element. The '?' character is used as
        placeholder for a tuple of same size.
        """
        with connection.begin() as transaction:
            try:
                markers = ",".join(["(?)"] * len(data))
                markers = markers
                ins = "INSERT INTO {tablename} VALUES {markers}"
                ins = ins.format(tablename=write_table.name, markers=markers)
                connection.dispatch(ins, list(data))
            except:
                transaction.rollback()
                raise
            else:
                transaction.commit()

    @staticmethod
    def __insert_tuples(data: Union[List[Any], npt.NDArray[Any]], write_table: Any, connection: Any) -> None:
        """
        :param data: a list of tuples
        :param write_table: a SQLAlchemy object that represents a table
        :param connection: a SQLAlchemy connection to write the data

        This method will lookup the length of the first object of the list and build the insert through
        creation of a string of '?' equivalent to the length of the element. The '?' character is used as
        placeholder for a tuple of same size.
        """
        with connection.begin() as transaction:
            try:
                markers = ",".join([f'({",".join("?" * len(data[0]))})'] * len(data))
                markers = markers
                ins = "INSERT INTO {tablename} VALUES ({markers})"
                ins = ins.format(tablename=write_table.name, markers=markers)

                connection.dispatch(ins, data)
            except:
                transaction.rollback()
                raise
            else:
                transaction.commit()

    @staticmethod
    def __insert_dicts(data: Union[List[Any], npt.NDArray[Any]], write_table: Any, connection: Any) -> None:
        """
        :param data: a list of tuples
        :param write_table: a SQLAlchemy object that represents a table
        :param connection: a SQLAlchemy connection to write the data

        This method will insert the data contained in a list of dictionaries into a table. The query itself is handled
        by SQLAlchemy, so it's only needed to pass the correctly data type.
        """
        with connection.begin() as transaction:
            try:
                connection.dispatch(write_table.insert(), data)
            except:
                transaction.rollback()
                raise
            else:
                transaction.commit()
