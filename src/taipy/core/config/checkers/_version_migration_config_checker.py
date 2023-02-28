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

from taipy.config._config import _Config
from taipy.config.checker._checkers._config_checker import _ConfigChecker
from taipy.config.checker.issue_collector import IssueCollector

from ..version_migration_config import VersionMigrationConfig


class _VersionMigrationConfigChecker(_ConfigChecker):
    def __init__(self, config: _Config, collector: IssueCollector):
        super().__init__(config, collector)

    def _check(self) -> IssueCollector:
        version_migration_configs = self._config._sections[VersionMigrationConfig.name]
        for version_migration_config_id, version_migration_config in version_migration_configs.items():
            if version_migration_config_id != _Config.DEFAULT_KEY:
                self._check_existing_config_id(version_migration_config)
                self._check_if_entity_property_key_used_is_predefined(version_migration_config)
                self._check_migration_fcts(version_migration_config_id, version_migration_config)
        return self._collector

    def _check_migration_fcts(self, version_migration_config_id: str, version_migration_config: VersionMigrationConfig):
        properties_to_check = [
            VersionMigrationConfig._DATA_NODE_MIGRATION_FCT_KEY,
            VersionMigrationConfig._TASK_MIGRATION_FCT_KEY,
            VersionMigrationConfig._PIPELINE_MIGRATION_FCT_KEY,
            VersionMigrationConfig._SCENARIO_MIGRATION_FCT_KEY,
        ]

        for property_to_check in properties_to_check:
            prop_value = getattr(version_migration_config, property_to_check)

            if prop_value and not callable(prop_value):
                self._error(
                    property_to_check,
                    prop_value,
                    f"{property_to_check} field of VersionMigrationConfig `{version_migration_config_id}`"
                    f" must be populated with Callable value.",
                )

        if all(
            getattr(version_migration_config, property_to_check) is None for property_to_check in properties_to_check
        ):
            self._warning(
                "migration_fct",
                None,
                f"There is no migration function defined in VersionMigrationConfig `{version_migration_config_id}`"
                f' from version "{getattr(version_migration_config, VersionMigrationConfig._SOURCE_VERISON_KEY)}"'
                f' to version "{getattr(version_migration_config, VersionMigrationConfig._TARGET_VERISON_KEY)}".',
            )
