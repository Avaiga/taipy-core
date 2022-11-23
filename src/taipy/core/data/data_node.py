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

import uuid
from abc import abstractmethod
from datetime import datetime, timedelta
from functools import reduce
from typing import Any, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from taipy.config.common._validate_id import _validate_id
from taipy.config.common.scope import Scope
from taipy.logger._taipy_logger import _TaipyLogger

from ..common._entity import _Entity
from ..common._listattributes import _ListAttributes
from ..common._properties import _Properties
from ..common._reload import _reload, _self_reload, _self_setter
from ..common._warnings import _warn_deprecated
from ..common.alias import DataNodeId, JobId
from ..exceptions.exceptions import NoData
from ._filter import _FilterDataNode
from .operator import JoinOperator, Operator


class DataNode(_Entity):
    """Reference to a dataset.

    A Data Node is an abstract class that holds metadata related to the dataset it refers to.
    In particular, a data node holds the name, the scope, the parent identifier, the last
    edit date, and some additional properties of the data.<br/>
    A Data Node also contains information and methods needed to access the dataset. This
    information depends on the type of storage, and it is held by subclasses (such as
    SQL Data Node, CSV Data Node, ...).

    !!! note
        It is recommended not to instantiate subclasses of DataNode directly.

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
        kwargs: A dictionary of additional properties.
    """

    _ID_PREFIX = "DATANODE"
    __ID_SEPARATOR = "_"
    __logger = _TaipyLogger._get_logger()
    _REQUIRED_PROPERTIES: List[str] = []
    _MANAGER_NAME = "data"

    def __init__(
        self,
        config_id,
        scope: Scope = Scope(Scope.PIPELINE),
        id: Optional[DataNodeId] = None,
        name: Optional[str] = None,
        parent_id: Optional[str] = None,
        last_edit_date: Optional[datetime] = None,
        job_ids: List[JobId] = None,
        cacheable: bool = False,
        validity_period: Optional[timedelta] = None,
        edit_in_progress: bool = False,
        **kwargs,
    ):
        self.config_id = _validate_id(config_id)
        self.id = id or DataNodeId(self.__ID_SEPARATOR.join([self._ID_PREFIX, self.config_id, str(uuid.uuid4())]))
        self.parent_id = parent_id
        self._scope = scope
        self._last_edit_date = last_edit_date
        self._name = name or self.id
        self._edit_in_progress = edit_in_progress
        self._job_ids = _ListAttributes(self, job_ids or list())

        self._cacheable = cacheable
        self._validity_period = validity_period

        self._properties = _Properties(self, **kwargs)

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def last_edit_date(self):
        return self._last_edit_date

    @last_edit_date.setter  # type: ignore
    @_self_setter(_MANAGER_NAME)
    def last_edit_date(self, val):
        self._last_edit_date = val

    @property  # type: ignore
    def last_edition_date(self):
        """
        Deprecated. Use last_edit_date instead.
        """
        _warn_deprecated("last_edition_date", suggest="last_edit_date")
        return self.last_edit_date

    @last_edition_date.setter  # type: ignore
    def last_edition_date(self, val):
        _warn_deprecated("last_edition_date", suggest="last_edit_date")
        self.last_edit_date = val

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def scope(self):
        return self._scope

    @scope.setter  # type: ignore
    @_self_setter(_MANAGER_NAME)
    def scope(self, val):
        self._scope = val

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def validity_period(self) -> Optional[timedelta]:
        return self._validity_period if self._validity_period else None

    @validity_period.setter  # type: ignore
    @_self_setter(_MANAGER_NAME)
    def validity_period(self, val):
        self._validity_period = val

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def expiration_date(self) -> datetime:
        if not self._last_edit_date:
            raise NoData(f"Data node {self.id} has not been written yet.")

        return self._last_edit_date + self.validity_period if self.validity_period else self._last_edit_date

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def name(self):
        return self._name

    @name.setter  # type: ignore
    @_self_setter(_MANAGER_NAME)
    def name(self, val):
        self._name = val

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def cacheable(self):
        return self._cacheable

    @cacheable.setter  # type: ignore
    @_self_setter(_MANAGER_NAME)
    def cacheable(self, val):
        self._cacheable = val

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def edit_in_progress(self):
        return self._edit_in_progress

    @edit_in_progress.setter  # type: ignore
    @_self_setter(_MANAGER_NAME)
    def edit_in_progress(self, val):
        self._edit_in_progress = val

    @property  # type: ignore
    def edition_in_progress(self):
        """
        Deprecated. Use edit_in_progress instead.
        """
        _warn_deprecated("edition_in_progress", suggest="edit_in_progress")
        return self.edit_in_progress

    @edition_in_progress.setter  # type: ignore
    def edition_in_progress(self, val):
        _warn_deprecated("edition_in_progress", suggest="edit_in_progress")
        self.edit_in_progress = val

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def job_ids(self):
        return self._job_ids

    @job_ids.setter  # type: ignore
    @_self_setter(_MANAGER_NAME)
    def job_ids(self, val):
        self._job_ids = _ListAttributes(self, val)

    @property  # type: ignore
    def properties(self):
        r = _reload(self._MANAGER_NAME, self)
        self._properties = r._properties
        return self._properties

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
        if protected_attribute_name in self._properties:
            return self._properties[protected_attribute_name]
        raise AttributeError(f"{attribute_name} is not an attribute of data node {self.id}")

    @classmethod
    @abstractmethod
    def storage_type(cls) -> str:
        return NotImplemented

    def read_or_raise(self) -> Any:
        """Read the data referenced by this data node.

        Returns:
            Any: The data referenced by this data node.
        Raises:
            NoData^: If the data has not been written yet.
        """
        if not self.last_edit_date:
            raise NoData(f"Data node {self.id} has not been written yet.")
        return self._read()

    def read(self) -> Any:
        """Read the data referenced by this data node.

        Returns:
            Any: The data referenced by this data node. None if the data has not been written yet.
        """
        try:
            return self.read_or_raise()
        except NoData:
            self.__logger.warning(f"Data node {self.id} is being read but has never been written.")
            return None

    def write(self, data, job_id: Optional[JobId] = None):
        """Write some data to this data node.

        Parameters:
            data (Any): The data to write to this data node.
            job_id (JobId^): An optional identifier of the writer.
        """
        from ._data_manager_factory import _DataManagerFactory

        self._write(data)
        self.last_edit_date = datetime.now()  # type: ignore
        if job_id:
            self._job_ids.append(job_id)
        self.unlock_edit()
        _DataManagerFactory._build_manager()._set(self)

    def lock_edit(self):
        """Lock the edit of this data node.

        Note:
            The data node can be unlocked with the method `(DataNode.)unlock_edit()^`.
        """
        self.edit_in_progress = True

    def lock_edition(self):
        """
        Deprecated. Use lock_edit instead.
        """
        _warn_deprecated("lock_edition", suggest="lock_edit")
        self.lock_edit()

    def unlock_edit(self, at: datetime = None, job_id: JobId = None):
        """Unlocks the edit of the data node.

        Parameters:
            at (datetime): Deprecated.
            job_id (JobId^): Deprecated.
        Note:
            The data node  can be locked with the method `(DataNode.)lock_edit()^`.
        """
        self.edit_in_progress = False  # type: ignore

    def unlock_edition(self, at: datetime = None, job_id: JobId = None):
        """
        Deprecated. Use (DataNode.)unlock_edit()^` instead.
        """
        _warn_deprecated("unlock_edition", suggest="unlock_edit")
        self.unlock_edit()

    def filter(self, operators: Union[List, Tuple], join_operator=JoinOperator.AND):
        """Read the data referenced by the data node, appying a filter.

        The data is filtered by the provided list of 3-tuples (key, value, `Operator^`).

        If multiple filter operators are provided, filtered data will be joined based on the
        join operator (_AND_ or _OR_).

        Parameters:
            operators (Union[List[Tuple], Tuple]): TODO
            join_operator (JoinOperator^): The operator used to join the multiple filter
                3-tuples.
        """
        data = self._read()
        if len(operators) == 0:
            return data
        if not ((type(operators[0]) == list) or (type(operators[0]) == tuple)):
            if isinstance(data, pd.DataFrame):
                return DataNode.__filter_dataframe_per_key_value(data, operators[0], operators[1], operators[2])
            if isinstance(data, List):
                return DataNode.__filter_list_per_key_value(data, operators[0], operators[1], operators[2])
        else:
            if isinstance(data, pd.DataFrame):
                return DataNode.__filter_dataframe(data, operators, join_operator=join_operator)
            if isinstance(data, List):
                return DataNode.__filter_list(data, operators, join_operator=join_operator)
        return NotImplemented

    @staticmethod
    def __filter_dataframe(df_data: pd.DataFrame, operators: Union[List, Tuple], join_operator=JoinOperator.AND):
        filtered_df_data = []
        if join_operator == JoinOperator.AND:
            how = "inner"
        elif join_operator == JoinOperator.OR:
            how = "outer"
        else:
            return NotImplemented
        for key, value, operator in operators:
            filtered_df_data.append(DataNode.__filter_dataframe_per_key_value(df_data, key, value, operator))
        return DataNode.__dataframe_merge(filtered_df_data, how) if filtered_df_data else pd.DataFrame()

    @staticmethod
    def __filter_dataframe_per_key_value(df_data: pd.DataFrame, key: str, value, operator: Operator):
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
    def __dataframe_merge(df_list: List, how="inner"):
        return reduce(lambda df1, df2: pd.merge(df1, df2, how=how), df_list)

    @staticmethod
    def __filter_list(list_data: List, operators: Union[List, Tuple], join_operator=JoinOperator.AND):
        filtered_list_data = []
        for key, value, operator in operators:
            filtered_list_data.append(DataNode.__filter_list_per_key_value(list_data, key, value, operator))
        if len(filtered_list_data) == 0:
            return filtered_list_data
        if join_operator == JoinOperator.AND:
            return DataNode.__list_intersect(filtered_list_data)
        elif join_operator == JoinOperator.OR:
            return list(set(np.concatenate(filtered_list_data)))
        else:
            return NotImplemented

    @staticmethod
    def __filter_list_per_key_value(list_data: List, key: str, value, operator: Operator):
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
    def __list_intersect(list_data):
        return list(set(list_data.pop()).intersection(*map(set, list_data)))

    @abstractmethod
    def _read(self):
        return NotImplemented

    @abstractmethod
    def _write(self, data):
        return NotImplemented

    def __getitem__(self, items):
        return _FilterDataNode(self.id, self._read())[items]

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def is_ready_for_reading(self) -> bool:
        """Indicate if this data node is ready for reading.

        Returns:
            bool: False if the data is locked for modification or if the data has never been written.
                True otherwise.
        """
        if self._edit_in_progress:
            return False
        if not self._last_edit_date:
            # Never been written so it is not up-to-date
            return False
        return True

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def _is_in_cache(self):
        if not self._cacheable:
            return False
        if not self._last_edit_date:
            # Never been written so it is not up-to-date
            return False
        if not self._validity_period:
            # No validity period and cacheable so it is up-to-date
            return True
        if datetime.now() > self.expiration_date:
            # expiration_date has been passed
            return False
        return True

    @staticmethod
    def _class_map():
        def all_subclasses(cls):
            subclasses = set(cls.__subclasses__())
            for s in cls.__subclasses__():
                subclasses.update(all_subclasses(s))
            return subclasses

        return {c.storage_type(): c for c in all_subclasses(DataNode) if c.storage_type() is not None}
