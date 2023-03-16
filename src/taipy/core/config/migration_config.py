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

import collections.abc
from copy import deepcopy
from typing import Any, Callable, Dict, List, Optional, Union

from taipy.config._config import _Config
from taipy.config.common._template_handler import _TemplateHandler as _tpl
from taipy.config.config import Config
from taipy.config.unique_section import UniqueSection

from .._version._version_manager import _VersionManager
from ..common.alias import DataNodeId, PipelineId, ScenarioId, TaskId
from .data_node_config import DataNodeConfig
from .pipeline_config import PipelineConfig
from .scenario_config import ScenarioConfig
from .task_config import TaskConfig


class MigrationConfig(UniqueSection):
    """
    Configuration fields needed to register migration functions from an old version to newer one.

    Attributes:
        migration_fcts (Dict[str, Dict[str, Callable]]): A dictionary that maps the version that entities are
            migrated from to the migration functions.
        **properties (dict[str, Any]): A dictionary of additional properties.
    """

    name = "VERSION_MIGRATION"

    _MIGRATION_FCTS_KEY = "migration_fcts"

    def __init__(
        self,
        migration_fcts: Dict[str, Dict[str, Callable]],
        **properties,
    ):
        self.migration_fcts = migration_fcts

        super().__init__(**properties)

    def __copy__(self):
        return MigrationConfig(
            deepcopy(self.migration_fcts),
            **deepcopy(self._properties),
        )

    def __getattr__(self, item: str) -> Optional[Any]:
        return _tpl._replace_templates(self._properties.get(item))  # type: ignore

    @classmethod
    def default_config(cls):
        return MigrationConfig({})

    def _to_dict(self):
        return {
            self._MIGRATION_FCTS_KEY: self.migration_fcts,
            **self._properties,
        }

    @classmethod
    def _from_dict(cls, as_dict: Dict[str, Any], id: str, config: Optional[_Config]):
        return MigrationConfig(**as_dict)

    def _update(self, as_dict, default_section=None):
        def deep_update(d, u):
            for k, v in u.items():
                if isinstance(v, collections.abc.Mapping):
                    d[k] = deep_update(d.get(k, {}), v)
                else:
                    d[k] = v
            return d

        migration_fcts = as_dict.pop(self._MIGRATION_FCTS_KEY)
        deep_update(self.migration_fcts, migration_fcts)

        self._properties.update(as_dict)

    @staticmethod
    def _add_data_node_migration_function(
        target_version: str,
        data_node_config: Union[DataNodeConfig, DataNodeId],
        migration_fct: Callable,
        **properties,
    ):
        """Add a data node migration function to the configuration.

        Parameters:
            target_version (str): The version that entities are migrated to.
            data_node_config (Union[DataNodeConfig, DataNodeId]): The data node configuration or the `id` of
                the data node config that need to migrate.
            migration_fct (Callable): Migration function that takes a DataNode entity from `target_version` as input
                and returns a compatible DataNode with the next production version.
            **properties (Dict[str, Any]): A keyworded variable length list of additional arguments.
        Returns:
            `MigrationConfig^`: The Migration configuration.
        """
        return MigrationConfig.__add_migration_function(target_version, data_node_config, migration_fct, **properties)

    @staticmethod
    def _add_task_migration_function(
        target_version: str,
        task_config: Union[TaskConfig, TaskId],
        migration_fct: Callable,
        **properties,
    ):
        """Add a task migration function to the configuration.

        Parameters:
            target_version (str): The version that entities are migrated to.
            task_config (Union[TaskConfig, TaskId]): The task configuration or the `id` of
                the task config that need to migrate.
            migration_fct (Callable): Migration function that takes a DataNode entity from `target_version` as input
                and returns a compatible DataNode with the next production version.
            **properties (Dict[str, Any]): A keyworded variable length list of additional arguments.
        Returns:
            `MigrationConfig^`: The Migration configuration.
        """
        return MigrationConfig.__add_migration_function(target_version, task_config, migration_fct, **properties)

    @staticmethod
    def _add_pipeline_migration_function(
        target_version: str,
        pipeline_config: Union[PipelineConfig, PipelineId],
        migration_fct: Callable,
        **properties,
    ):
        """Add a pipeline migration function to the configuration.

        Parameters:
            target_version (str): The version that entities are migrated to.
            pipeline_config (Union[PipelineConfig, PipelineId]): The pipeline configuration or the `id` of
                the pipeline config that need to migrate.
            migration_fct (Callable): Migration function that takes a DataNode entity from `target_version` as input
                and returns a compatible DataNode with the next production version.
            **properties (Dict[str, Any]): A keyworded variable length list of additional arguments.
        Returns:
            `MigrationConfig^`: The Migration configuration.
        """
        return MigrationConfig.__add_migration_function(target_version, pipeline_config, migration_fct, **properties)

    @staticmethod
    def _add_scenario_migration_function(
        target_version: str,
        scenario_config: Union[ScenarioConfig, ScenarioId],
        migration_fct: Callable,
        **properties,
    ):
        """Add a scenario migration function to the configuration.

        Parameters:
            target_version (str): The version that entities are migrated to.
            scenario_config (Union[ScenarioConfig, ScenarioId]): The scenario configuration or the `id` of
                the scenario config that need to migrate.
            migration_fct (Callable): Migration function that takes a DataNode entity from `target_version` as input
                and returns a compatible DataNode with the next production version.
            **properties (Dict[str, Any]): A keyworded variable length list of additional arguments.
        Returns:
            `MigrationConfig^`: The Migration configuration.
        """
        return MigrationConfig.__add_migration_function(target_version, scenario_config, migration_fct, **properties)

    @staticmethod
    def __add_migration_function(
        target_version: str,
        config: Any,
        migration_fct: Callable,
        **properties,
    ):
        config_id = config if isinstance(config, str) else config.id
        migration_fcts = {target_version: {config_id: migration_fct}}

        section = MigrationConfig(
            migration_fcts,
            **properties,
        )
        Config._register(section)
        return Config.unique_sections[MigrationConfig.name]
