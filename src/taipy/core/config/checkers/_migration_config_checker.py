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

from ..._version._version_manager import _VersionManager
from ...exceptions import NonExistingVersion
from ..migration_config import MigrationConfig


class _MigrationConfigChecker(_ConfigChecker):
    def __init__(self, config: _Config, collector: IssueCollector):
        super().__init__(config, collector)

    def _check(self) -> IssueCollector:
        if migration_config := self._config._unique_sections.get(MigrationConfig.name):
            self._check_if_entity_property_key_used_is_predefined(migration_config)
            for source_version, migration_functions in migration_config.migration_fcts.items():
                self._check_valid_production_version(source_version)
                for config_id, migration_function in migration_functions.items():
                    self._check_callable(source_version, config_id, migration_function)
        return self._collector

    def _check_callable(self, source_version, config_id, migration_function):
        if not callable(migration_function):
            self._error(
                MigrationConfig._MIGRATION_FCTS_KEY,
                migration_function,
                f"The migration function of config `{config_id}` from version {source_version}"
                f" must be populated with Callable value.",
            )

    def _check_valid_production_version(self, source_version):
        try:
            version_number = _VersionManager._replace_version_number(source_version)
            if not _VersionManager._is_production_version(version_number):
                self._error(
                    MigrationConfig._MIGRATION_FCTS_KEY,
                    version_number,
                    "The source version for a migration function must be a production version.",
                )
        except NonExistingVersion:
            self._error(
                MigrationConfig._MIGRATION_FCTS_KEY,
                source_version,
                "The source version for a migration function must be a valid version number.",
            )
