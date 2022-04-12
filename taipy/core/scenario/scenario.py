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

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set

from taipy.core.common._entity import _Entity
from taipy.core.common._listattributes import _ListAttributes
from taipy.core.common._properties import _Properties
from taipy.core.common._reload import _reload, _self_reload, _self_setter
from taipy.core.common._validate_id import _validate_id
from taipy.core.common.alias import ScenarioId
from taipy.core.cycle.cycle import Cycle
from taipy.core.job.job import Job
from taipy.core.pipeline.pipeline import Pipeline


class Scenario(_Entity):
    """Instance of a Business case to solve.

    A scenario holds a list of pipelines (instances of `Pipeline^` class) to submit for execution
    in order to solve the Business case.

    Attributes:
        config_id (str): The identifier of the `ScenarioConfig^`.
        pipelines (List[Pipeline^]): The list of pipelines.
        properties (dict[str, Any]): A dictionary of additional properties.
        scenario_id (str): The unique identifier of this scenario.
        creation_date (datetime): The date and time of the scenario's creation.
        is_primary (bool): True if the scenario is the primary of its cycle. False otherwise.
        cycle (Cycle^): The cycle of the scenario.
        subscribers (Set[Callable]): The set of callbacks to be called on `Job^`'s status change.
        tags (Set[str]): The list of scenario's tags.
    """

    _ID_PREFIX = "SCENARIO"
    _MANAGER_NAME = "scenario"
    __SEPARATOR = "_"

    def __init__(
        self,
        config_id: str,
        pipelines: List[Pipeline],
        properties: Dict[str, Any],
        scenario_id: ScenarioId = None,
        creation_date=None,
        is_primary: bool = False,
        cycle: Cycle = None,
        subscribers: List[Callable] = None,
        tags: Set[str] = None,
    ):
        self.config_id = _validate_id(config_id)
        self.id: ScenarioId = scenario_id or self._new_id(self.config_id)
        self._pipelines = {p.config_id: p for p in pipelines}
        self._creation_date = creation_date or datetime.now()
        self._cycle = cycle
        self._subscribers = _ListAttributes(self, subscribers or list())
        self._primary_scenario = is_primary
        self._tags = tags or set()

        self._properties = _Properties(self, **properties)

    def __getstate__(self):
        return self.id

    def __setstate__(self, id):
        from taipy.core.scenario._scenario_manager import _ScenarioManager

        sc = _ScenarioManager._get(id)
        self.__dict__ = sc.__dict__

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def pipelines(self):
        return self._pipelines

    @pipelines.setter  # type: ignore
    @_self_setter(_MANAGER_NAME)
    def pipelines(self, val: List[Pipeline]):
        self._pipelines = {p.config_id: p for p in val}

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def creation_date(self):
        return self._creation_date

    @creation_date.setter  # type: ignore
    @_self_setter(_MANAGER_NAME)
    def creation_date(self, val):
        self._creation_date = val

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def cycle(self):
        return self._cycle

    @cycle.setter  # type: ignore
    @_self_setter(_MANAGER_NAME)
    def cycle(self, val):
        self._cycle = val

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def is_primary(self):
        return self._primary_scenario

    @is_primary.setter  # type: ignore
    @_self_setter(_MANAGER_NAME)
    def is_primary(self, val):
        self._primary_scenario = val

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def subscribers(self):
        return self._subscribers

    @subscribers.setter  # type: ignore
    @_self_setter(_MANAGER_NAME)
    def subscribers(self, val):
        self._subscribers = _ListAttributes(self, val)

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def tags(self):
        return self._tags

    @tags.setter  # type: ignore
    @_self_setter(_MANAGER_NAME)
    def tags(self, val):
        self._tags = val or set()

    @property  # type: ignore
    def properties(self):
        self._properties = _reload(self._MANAGER_NAME, self)._properties
        return self._properties

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def name(self) -> Optional[str]:
        return self._properties.get("name")

    @name.setter  # type: ignore
    @_self_setter(_MANAGER_NAME)
    def name(self, val):
        self._properties["name"] = val

    def __eq__(self, other):
        return self.id == other.id

    @staticmethod
    def _new_id(config_id: str) -> ScenarioId:
        """Generate a unique scenario identifier."""
        return ScenarioId(Scenario.__SEPARATOR.join([Scenario._ID_PREFIX, _validate_id(config_id), str(uuid.uuid4())]))

    def __getattr__(self, attribute_name):
        protected_attribute_name = _validate_id(attribute_name)
        if protected_attribute_name in self.properties:
            return self.properties[protected_attribute_name]
        if protected_attribute_name in self.pipelines:
            return self.pipelines[protected_attribute_name]
        for pipeline in self.pipelines.values():
            if protected_attribute_name in pipeline.tasks:
                return pipeline.tasks[protected_attribute_name]
            for task in pipeline.tasks.values():
                if protected_attribute_name in task.input:
                    return task.input[protected_attribute_name]
                if protected_attribute_name in task.output:
                    return task.output[protected_attribute_name]
        raise AttributeError(f"{attribute_name} is not an attribute of scenario {self.id}")

    def _add_subscriber(self, callback: Callable):
        self._subscribers.append(callback)

    def _add_tag(self, tag: str):
        self._tags = _reload("scenario", self)._tags
        self._tags.add(tag)

    def has_tag(self, tag: str) -> bool:
        """Indicate if the scenario has a given tag.

        Parameters:
            tag (str): The tag to search among the set of scenario's tags.
        Returns:
            bool: True if the scenario has the tag given as parameter. False otherwise.
        """
        return tag in self.tags

    def _remove_subscriber(self, callback: Callable):
        self._subscribers.remove(callback)

    def _remove_tag(self, tag: str):
        self._tags = _reload("scenario", self)._tags
        if self.has_tag(tag):
            self._tags.remove(tag)

    def subscribe(self, callback: Callable[[Scenario, Job], None]):
        """Subscribe a function to be called on `Job^` status change.

        The subscription is applied to all jobs created from the scenario's execution.

        Parameters:
            callback (Callable[[Scenario^, Job^], None]): The callable function to be called
                on status change.

        Note:
            Notification will be available only for jobs created after this subscription.
        """
        from taipy.core.scenario._scenario_manager import _ScenarioManager

        return _ScenarioManager._subscribe(callback, self)

    def unsubscribe(self, callback: Callable[[Scenario, Job], None]):
        """Unsubscribe a function that is called when the status of a `Job^` changes.

        Parameters:
            callback (Callable[[Scenario^, Job^], None]): The callable function to unsubscribe.

        Note:
            The function will continue to be called for ongoing jobs.
        """
        from taipy.core.scenario._scenario_manager import _ScenarioManager

        return _ScenarioManager._unsubscribe(callback, self)

    def submit(self, force: bool = False):
        """Submit this scenario for execution.

        All the `Task^`s of the scenario will be submitted for execution.

        Parameters:
            force (bool): Force execution even if the data nodes are in cache.
        """
        from taipy.core.scenario._scenario_manager import _ScenarioManager

        return _ScenarioManager._submit(self, force)

    def set_primary(self):
        """Promote the scenario as the primary scenario of its cycle.
        
        If the cycle already has a primary scenario, it will be demoted, and it will no longer
        be primary for the cycle.
        """
        from taipy.core.scenario._scenario_manager import _ScenarioManager

        return _ScenarioManager._set_primary(self)

    def add_tag(self, tag: str):
        """Add a tag to this scenario.
        
        If the scenario's cycle already have another scenario tagged with _tag_ the other
        scenario will be untagged.

        Parameters:
            tag (str): The tag to add to this scenario.
        """
        from taipy.core.scenario._scenario_manager import _ScenarioManager

        return _ScenarioManager._tag(self, tag)

    def remove_tag(self, tag: str):
        """Remove a tag from this scenario.

        Parameters:
            tag (str): The tag to remove from the set of the scenario's tags.
        """
        from taipy.core.scenario._scenario_manager import _ScenarioManager

        return _ScenarioManager._untag(self, tag)
