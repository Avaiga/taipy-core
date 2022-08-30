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
from typing import Callable, Dict, Iterable, List, Optional, Union

from taipy.config.common._validate_id import _validate_id
from taipy.config.common.scope import Scope

from ..common._entity import _Entity
from ..common._reload import _self_reload, _self_setter
from ..common.alias import TaskId
from ..data.data_node import DataNode


class Task(_Entity):
    """Hold a user function that will be executed, its parameters and the results.

    A `Task` brings together the user code as function, the inputs and the outputs as data nodes
    (instances of the `DataNode^` class).

    Attributes:
        config_id (str): The identifier of the `TaskConfig^`.
        function (callable): The python function to execute. The _function_ must take as parameter the
            data referenced by inputs data nodes, and must return the data referenced by outputs data nodes.
        input (Union[DataNode^, List[DataNode^]]): The list of inputs.
        output (Union[DataNode^, List[DataNode^]]): The list of outputs.
        id (str): The unique identifier of the task.
        parent_id (str):  The identifier of the parent (pipeline_id, scenario_id, cycle_id) or None.
    """

    _ID_PREFIX = "TASK"
    __ID_SEPARATOR = "_"
    _MANAGER_NAME = "task"

    def __init__(
        self,
        config_id: str,
        function,
        input: Optional[Iterable[DataNode]] = None,
        output: Optional[Iterable[DataNode]] = None,
        id: TaskId = None,
        parent_id: Optional[str] = None,
    ):
        self.config_id = _validate_id(config_id)
        self.id = id or TaskId(self.__ID_SEPARATOR.join([self._ID_PREFIX, self.config_id, str(uuid.uuid4())]))
        self.parent_id = parent_id
        self.__input = {dn.config_id: dn for dn in input or []}
        self.__output = {dn.config_id: dn for dn in output or []}
        self._function = function

    def __hash__(self):
        return hash(self.id)

    def __getstate__(self):
        return vars(self)

    def __setstate__(self, state):
        vars(self).update(state)

    @property  # type: ignore
    def input(self) -> Dict[str, DataNode]:
        return self.__input

    @property  # type: ignore
    def output(self) -> Dict[str, DataNode]:
        return self.__output

    @property  # type: ignore
    def data_nodes(self) -> Dict[str, DataNode]:
        return {**self.input, **self.output}

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def function(self):
        return self._function

    @function.setter  # type: ignore
    @_self_setter(_MANAGER_NAME)
    def function(self, val):
        self._function = val

    def __getattr__(self, attribute_name):
        protected_attribute_name = _validate_id(attribute_name)
        if protected_attribute_name in self.input:
            return self.input[protected_attribute_name]
        if protected_attribute_name in self.output:
            return self.output[protected_attribute_name]
        raise AttributeError(f"{attribute_name} is not an attribute of task {self.id}")

    @property
    def scope(self) -> Scope:
        """Retrieve the lowest scope of the task based on its data nodes.

        Returns:
            Scope^: Lowest scope present in input and output data nodes or GLOBAL if there are
                either no input or no output.
        """
        data_nodes = list(self.__input.values()) + list(self.__output.values())
        scope = min(dn.scope for dn in data_nodes) if len(data_nodes) != 0 else Scope.GLOBAL
        return Scope(scope)

    def submit(
        self,
        callbacks: Optional[List[Callable]] = None,
        force: bool = False,
        wait: bool = False,
        timeout: Optional[Union[float, int]] = None,
    ):
        """Submit the task for execution.

        Parameters:
            callbacks (List[Callable]): The list of callable functions to be called on status
                change.
            force (bool): Force execution even if the data nodes are in cache.
            wait (bool): Wait for the scheduled job created from the task submission to be finished in asynchronous mode.
            timeout (Union[float, int]): The maximum number of seconds to wait for the job to be finished before returning.
        """
        from ._task_manager_factory import _TaskManagerFactory

        _TaskManagerFactory._build_manager()._submit(self, callbacks, force, wait, timeout)
