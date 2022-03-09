import uuid
from typing import Dict, Iterable, Optional

from taipy.core.common._entity import _Entity
from taipy.core.common._reload import self_reload, self_setter
from taipy.core.common._validate_id import _validate_id
from taipy.core.common.alias import TaskId
from taipy.core.data.data_node import DataNode
from taipy.core.data.scope import Scope


class Task(_Entity):
    """Holds user function that will be executed, its parameters qs data nodes and outputs as data nodes.

    This element bring together the user code as function, parameters and outputs.

    Attributes:
        config_id: Identifier of the task configuration. Must be a valid Python variable name.
        input:
            Data node input as list.
        function:
            Taking data from input data node and return data that should go inside of the output data node.
        output:
            Data node output result of the function as optional list.
        id:
            Unique identifier of this task. Generated if `None`.
        parent_id:
            Identifier of the parent (pipeline_id, scenario_id, cycle_id) or `None`.
    """

    ID_PREFIX = "TASK"
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
        self._config_id = _validate_id(config_id)
        self.id = id or TaskId(self.__ID_SEPARATOR.join([self.ID_PREFIX, self._config_id, str(uuid.uuid4())]))
        self._parent_id = parent_id
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
    @self_reload(_MANAGER_NAME)
    def config_id(self):
        return self._config_id

    @config_id.setter  # type: ignore
    @self_setter(_MANAGER_NAME)
    def config_id(self, val):
        self._config_id = val

    @property  # type: ignore
    def input(self):
        return self.__input

    @property  # type: ignore
    def output(self):
        return self.__output

    @property  # type: ignore
    @self_reload(_MANAGER_NAME)
    def parent_id(self):
        return self._parent_id

    @parent_id.setter  # type: ignore
    @self_setter(_MANAGER_NAME)
    def parent_id(self, val):
        self._parent_id = val

    @property  # type: ignore
    @self_reload(_MANAGER_NAME)
    def function(self):
        return self._function

    @function.setter  # type: ignore
    @self_setter(_MANAGER_NAME)
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
        """Retrieve the lowest scope of the task based on its data node.

        Returns:
           Lowest `scope` present in input and output data node or GLOBAL if there are no neither input or output.
        """
        data_nodes = list(self.__input.values()) + list(self.__output.values())
        scope = min(dn.scope for dn in data_nodes) if len(data_nodes) != 0 else Scope.GLOBAL
        return Scope(scope)
