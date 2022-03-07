import uuid
from abc import abstractmethod
from datetime import datetime, timedelta
from functools import reduce
from typing import List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from taipy.core.common._properties import _Properties
from taipy.core.common._reload import reload, self_reload
from taipy.core.common._taipy_logger import _TaipyLogger
from taipy.core.common._validate_id import _validate_id
from taipy.core.common.alias import DataNodeId, JobId
from taipy.core.config.data_node_config import DataNodeConfig
from taipy.core.data.filter import FilterDataNode
from taipy.core.data.operator import JoinOperator, Operator
from taipy.core.data.scope import Scope
from taipy.core.exceptions.data_node import NoData


class DataNode:
    """
    Data Node represents a reference to a dataset but not the data itself.

    A Data Node holds meta data related to the dataset it refers. In particular, a data node holds the name, the
    scope, the parent_id, the last edition date and additional properties of the data. A data Node is also made to
    contain information and methods needed to access the dataset. This information depends on the type of storage and it
    is hold by children classes (such as SQL Data Node, CSV Data Node, ...). It is strongly recommended to
    only instantiate children classes of Data Node through a Data Manager.

    Attributes:
        config_id (str):  Identifier of the data node configuration. Must be a valid Python variable name.
        scope (Scope):  The usage scope of this data node.
        id (str): Unique identifier of this data node.
        name (str): User-readable name of the data node.
        parent_id (str): Identifier of the parent (pipeline_id, scenario_id, cycle_id) or `None`.
        last_edition_date (datetime):  Date and time of the last edition.
        job_ids (List[str]): Ordered list of jobs that have written this data node.
        validity_period (Optional[timedelta]): Number of weeks, days, hours, minutes, and seconds as a
            timedelta object to represent the data node validity duration. If validity_period is set to None,
            the data_node is always up to date.
        edition_in_progress (bool) : True if a task computing the data node has been submitted and not completed yet.
            False otherwise.
        properties (dict): Dict of additional arguments.
    """

    ID_PREFIX = "DATANODE"
    __ID_SEPARATOR = "_"
    __logger = _TaipyLogger._get_logger()
    REQUIRED_PROPERTIES: List[str] = []

    def __init__(
        self,
        config_id,
        scope: Scope = Scope(Scope.PIPELINE),
        id: Optional[DataNodeId] = None,
        name: Optional[str] = None,
        parent_id: Optional[str] = None,
        last_edition_date: Optional[datetime] = None,
        job_ids: List[JobId] = None,
        validity_period: Optional[timedelta] = None,
        edition_in_progress: bool = False,
        **kwargs,
    ):
        self.config_id = _validate_id(config_id)
        self.id = id or DataNodeId(self.__ID_SEPARATOR.join([self.ID_PREFIX, self.config_id, str(uuid.uuid4())]))
        self.parent_id = parent_id
        self.scope = scope

        self._last_edition_date = last_edition_date
        self._name = name or self.id
        self._edition_in_progress = edition_in_progress
        self._job_ids = job_ids or []

        self._validity_period = validity_period

        self._properties = _Properties(self, **kwargs)

    @property  # type: ignore
    @self_reload("data")
    def validity_period(self) -> Optional[timedelta]:
        """
        Number of minutes where the Data Node is up-to-date.
        """
        return self._validity_period if self._validity_period else None

    @property  # type: ignore
    @self_reload("data")
    def expiration_date(self) -> datetime:
        if not self._last_edition_date:
            raise NoData

        return self._last_edition_date + self.validity_period if self.validity_period else self._last_edition_date

    @property  # type: ignore
    @self_reload("data")
    def name(self):
        return self._name

    @property  # type: ignore
    @self_reload("data")
    def edition_in_progress(self):
        return self._edition_in_progress

    @property  # type: ignore
    @self_reload("data")
    def job_ids(self):
        return self._job_ids

    @property  # type: ignore
    def properties(self):
        r = reload("data", self)
        self._properties = r._properties
        return self._properties

    @property  # type: ignore
    @self_reload("data")
    def last_edition_date(self):
        return self._last_edition_date

    def __eq__(self, other):
        return self.id == other.id

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.id)

    def __getstate__(self):
        return vars(self)

    def __setstate__(self, state):
        vars(self).update(state)

    def __getattr__(self, attribute_name):
        protected_attribute_name = _validate_id(attribute_name)
        if protected_attribute_name in self.properties:
            return self.properties[protected_attribute_name]
        raise AttributeError(f"{attribute_name} is not an attribute of data node {self.id}")

    @classmethod
    @abstractmethod
    def storage_type(cls) -> str:
        return NotImplemented

    def read_or_raise(self):
        if not self.last_edition_date:
            raise NoData
        return self._read()

    def read(self):
        try:
            return self.read_or_raise()
        except NoData:
            self.__logger.warning(f"Data node {self.id} is being read but has never been written.")
            return None

    def write(self, data, job_id: Optional[JobId] = None):
        from taipy.core.data.data_manager import DataManager

        self._write(data)
        self.unlock_edition(job_id=job_id)
        DataManager._set(self)

    def lock_edition(self):
        self._edition_in_progress = True

    def unlock_edition(self, at: datetime = None, job_id: JobId = None):
        self._last_edition_date = at or datetime.now()
        self._edition_in_progress = False
        if job_id:
            self._job_ids.append(job_id)

    def filter(self, operators: Union[List, Tuple], join_operator=JoinOperator.AND):
        """
        Filter data based on the provided list of tuples (key, value, operator)
        If mulitple filter operators, filtered data will be joined based on the join operator (AND or OR)
        """
        data = self._read()
        if len(operators) == 0:
            return data
        if not ((type(operators[0]) == list) or (type(operators[0]) == tuple)):
            if isinstance(data, pd.DataFrame):
                return DataNode.filter_dataframe_per_key_value(data, operators[0], operators[1], operators[2])
            if isinstance(data, List):
                return DataNode.filter_list_per_key_value(data, operators[0], operators[1], operators[2])
        else:
            if isinstance(data, pd.DataFrame):
                return DataNode.filter_dataframe(data, operators, join_operator=join_operator)
            if isinstance(data, List):
                return DataNode.filter_list(data, operators, join_operator=join_operator)
        return NotImplemented

    @staticmethod
    def filter_dataframe(df_data: pd.DataFrame, operators: Union[List, Tuple], join_operator=JoinOperator.AND):
        filtered_df_data = []
        if join_operator == JoinOperator.AND:
            how = "inner"
        elif join_operator == JoinOperator.OR:
            how = "outer"
        else:
            return NotImplemented
        for key, value, operator in operators:
            filtered_df_data.append(DataNode.filter_dataframe_per_key_value(df_data, key, value, operator))
        return DataNode.dataframe_merge(filtered_df_data, how) if filtered_df_data else pd.DataFrame()

    @staticmethod
    def filter_dataframe_per_key_value(df_data: pd.DataFrame, key: str, value, operator: Operator):
        df_by_col = df_data[key]
        if operator == Operator.EQUAL:
            df_by_col = df_by_col == value
        if operator == Operator.NOT_EQUAL:
            df_by_col = df_by_col != value
        if operator == Operator.LESS_THAN:
            df_by_col = df_by_col < value
        if operator == Operator.LESS_OR_EQUAL:
            df_by_col = df_by_col <= value
        if operator == Operator.GREATER_THAN:
            df_by_col = df_by_col > value
        if operator == Operator.GREATER_OR_EQUAL:
            df_by_col = df_by_col >= value
        return df_data[df_by_col]

    @staticmethod
    def dataframe_merge(df_list: List, how="inner"):
        return reduce(lambda df1, df2: pd.merge(df1, df2, how=how), df_list)

    @staticmethod
    def filter_list(list_data: List, operators: Union[List, Tuple], join_operator=JoinOperator.AND):
        filtered_list_data = []
        for key, value, operator in operators:
            filtered_list_data.append(DataNode.filter_list_per_key_value(list_data, key, value, operator))
        if len(filtered_list_data) == 0:
            return filtered_list_data
        if join_operator == JoinOperator.AND:
            return DataNode.list_intersect(filtered_list_data)
        elif join_operator == JoinOperator.OR:
            return list(set(np.concatenate(filtered_list_data)))
        else:
            return NotImplemented

    @staticmethod
    def filter_list_per_key_value(list_data: List, key: str, value, operator: Operator):
        filtered_list = []
        for row in list_data:
            row_value = getattr(row, key)
            if operator == Operator.EQUAL and row_value == value:
                filtered_list.append(row)
            if operator == Operator.NOT_EQUAL and row_value != value:
                filtered_list.append(row)
            if operator == Operator.LESS_THAN and row_value < value:
                filtered_list.append(row)
            if operator == Operator.LESS_OR_EQUAL and row_value <= value:
                filtered_list.append(row)
            if operator == Operator.GREATER_THAN and row_value > value:
                filtered_list.append(row)
            if operator == Operator.GREATER_OR_EQUAL and row_value >= value:
                filtered_list.append(row)
        return filtered_list

    @staticmethod
    def list_intersect(list_data):
        return list(set(list_data.pop()).intersection(*map(set, list_data)))

    @abstractmethod
    def _read(self):
        return NotImplemented

    @abstractmethod
    def _write(self, data):
        return NotImplemented

    def __getitem__(self, items):
        return FilterDataNode(self.id, self._read())[items]

    @property  # type: ignore
    @self_reload("data")
    def is_ready_for_reading(self):
        if self._edition_in_progress:
            return False
        if not self._last_edition_date:
            # Never been written so it is not up-to-date
            return False
        return True

    @property  # type: ignore
    @self_reload("data")
    def is_in_cache(self):
        if not self._properties.get(DataNodeConfig.IS_CACHEABLE_KEY):
            return False
        if not self._last_edition_date:
            # Never been written so it is not up-to-date
            return False
        if not self._validity_period:
            # No validity period and cacheable so it is up-to-date
            return True
        if datetime.now() > self.expiration_date:
            # expiration_date has been passed
            return False
        return True
