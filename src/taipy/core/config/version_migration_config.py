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

from copy import copy
from typing import Any, Callable, Dict, Optional

from taipy.config._config import _Config
from taipy.config.common._template_handler import _TemplateHandler as _tpl
from taipy.config.config import Config
from taipy.config.section import Section

from .._version._version_manager import _VersionManager


class VersionMigrationConfig(Section):
    """
    Holds all the configuration fields needed to register migration functions from an old version to newer one.

    Attributes:
        id (str): Identifier of the version migration configuration. It must be a valid Python variable name.
        source_version (str): The version that entities are migrated from.
        target_version (str): The version that entities are migrated to.
        data_node_migration_fct (Optional[Callable]): Migration function that takes a DataNode entity from `source_version` as input
            and returns a compatible DataNode entity with `target_version`. This can be a Python function or None if there is none.
        task_migration_fct (Optional[Callable]): Migration function that takes a Task from `source_version` as input
            and returns a compatible Task with `target_version`. This can be a Python function or None if there is none.
        pipeline_migration_fct (Optional[Callable]): Migration function that takes a Pipeline entity from `source_version` as input
            and returns a compatible Pipeline entity with `target_version`. This can be a Python function or None if there is none.
        scenario_migration_fct (Optional[Callable]): Migration function that takes a Scenario entity from `source_version` as input
            and returns a compatible Scenario entity with `target_version`. This can be a Python function or None if there is none.
        **properties (dict[str, Any]): A dictionary of additional properties.
    """

    name = "VERSION_MIGRATION"

    _SOURCE_VERISON_KEY = "source_version"
    _TARGET_VERISON_KEY = "target_version"
    _DATA_NODE_MIGRATION_FCT_KEY = "data_node_migration_fct"
    _TASK_MIGRATION_FCT_KEY = "task_migration_fct"
    _PIPELINE_MIGRATION_FCT_KEY = "pipeline_migration_fct"
    _SCENARIO_MIGRATION_FCT_KEY = "scenario_migration_fct"

    def __init__(
        self,
        id: str,
        source_version: str,
        target_version: str,
        data_node_migration_fct: Optional[Callable] = None,
        task_migration_fct: Optional[Callable] = None,
        pipeline_migration_fct: Optional[Callable] = None,
        scenario_migration_fct: Optional[Callable] = None,
        **properties,
    ):
        self.source_version = _VersionManager._replace_version_number(source_version)
        self.target_version = _VersionManager._replace_version_number(target_version)
        self.data_node_migration_fct = data_node_migration_fct
        self.task_migration_fct = task_migration_fct
        self.pipeline_migration_fct = pipeline_migration_fct
        self.scenario_migration_fct = scenario_migration_fct

        super().__init__(id, **properties)

    def __copy__(self):
        return VersionMigrationConfig(
            self.id,
            self.source_version,
            self.target_version,
            self.data_node_migration_fct,
            self.task_migration_fct,
            self.pipeline_migration_fct,
            self.scenario_migration_fct,
            **copy(self._properties),
        )

    def __getattr__(self, item: str) -> Optional[Any]:
        return _tpl._replace_templates(self._properties.get(item))  # type: ignore

    @classmethod
    def default_config(cls):
        return VersionMigrationConfig(
            cls._DEFAULT_KEY,
            _VersionManager._DEFAULT_VERSION,
            _VersionManager._DEFAULT_VERSION,
            None,
            None,
            None,
            None,
        )

    def _to_dict(self):
        return {
            self._SOURCE_VERISON_KEY: self.source_version,
            self._TARGET_VERISON_KEY: self.target_version,
            self._DATA_NODE_MIGRATION_FCT_KEY: self.data_node_migration_fct,
            self._TASK_MIGRATION_FCT_KEY: self.task_migration_fct,
            self._PIPELINE_MIGRATION_FCT_KEY: self.pipeline_migration_fct,
            self._SCENARIO_MIGRATION_FCT_KEY: self.scenario_migration_fct,
            **self._properties,
        }

    @classmethod
    def _from_dict(cls, as_dict: Dict[str, Any], id: str, config: Optional[_Config]):
        as_dict.pop(cls._ID_KEY, id)
        return VersionMigrationConfig(id=id, **as_dict)

    def _update(self, as_dict, default_section=None):
        self.source_version = _VersionManager._replace_version_number(
            as_dict.pop(self._SOURCE_VERISON_KEY, self.source_version)
        )
        self.target_version = _VersionManager._replace_version_number(
            as_dict.pop(self._SOURCE_VERISON_KEY, self.target_version)
        )
        self.data_node_migration_fct = as_dict.pop(self._DATA_NODE_MIGRATION_FCT_KEY, self.data_node_migration_fct)
        self.task_migration_fct = as_dict.pop(self._TASK_MIGRATION_FCT_KEY, self.task_migration_fct)
        self.pipeline_migration_fct = as_dict.pop(self._PIPELINE_MIGRATION_FCT_KEY, self.pipeline_migration_fct)
        self.scenario_migration_fct = as_dict.pop(self._SCENARIO_MIGRATION_FCT_KEY, self.scenario_migration_fct)

        self._properties.update(as_dict)

    @staticmethod
    def _set(
        id: str,
        source_version: str,
        target_version: str,
        data_node_migration_fct: Optional[Callable] = None,
        task_migration_fct: Optional[Callable] = None,
        pipeline_migration_fct: Optional[Callable] = None,
        scenario_migration_fct: Optional[Callable] = None,
        **properties,
    ):
        """Set a new version migration configuration.

        Parameters:
            id (str): The unique identifier of the new pipeline configuration. It must be a valid Python variable name.
            source_version (str): The version that entities are migrated from.
            target_version (str): The version that entities are migrated to.
            data_node_migration_fct (Optional[Callable]): Migration function that takes a DataNode entity from `source_version` as input
                and returns a compatible DataNode entity with `target_version`. The default value is None.
            task_migration_fct (Optional[Callable]): Migration function that takes a Task from `source_version` as input
                and returns a compatible Task with `target_version`. The default value is None.
            pipeline_migration_fct (Optional[Callable]): Migration function that takes a Pipeline entity from `source_version` as input
                and returns a compatible Pipeline entity with `target_version`. The default value is None.
            scenario_migration_fct (Optional[Callable]): Migration function that takes a Scenario entity from `source_version` as input
                and returns a compatible Scenario entity with `target_version`. The default value is None.
            **properties (Dict[str, Any]): A keyworded variable length list of additional
                arguments.
        Returns:
            `VersionMigrationConfig^`: The new pipeline configuration.
        """
        section = VersionMigrationConfig(
            id,
            source_version,
            target_version,
            data_node_migration_fct,
            task_migration_fct,
            pipeline_migration_fct,
            scenario_migration_fct,
            **properties,
        )
        Config._register(section)
        return Config.sections[VersionMigrationConfig.name][id]
