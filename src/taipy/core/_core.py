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

import uuid
from typing import Optional

from taipy.config import Config
from taipy.config.checker.issue_collector import IssueCollector
from taipy.logger._taipy_logger import _TaipyLogger

from ._orchestrator._dispatcher._job_dispatcher import _JobDispatcher
from ._orchestrator._orchestrator import _Orchestrator
from ._orchestrator._orchestrator_factory import _OrchestratorFactory
from ._version._version_cli import _VersioningCLI
from ._version._version_manager_factory import _VersionManagerFactory
from .config.checkers._migration_config_checker import _MigrationConfigChecker
from .taipy import clean_all_entities_by_version


class Core:
    """
    Core service
    """

    _orchestrator: Optional[_Orchestrator] = None
    _dispatcher: Optional[_JobDispatcher] = None

    def __init__(self):
        """
        Initialize a Core service.
        """
        _VersioningCLI._create_parser()
        self.__logger = _TaipyLogger._get_logger()
        self.cli_args = _VersioningCLI._parse_arguments()
        self._orchestrator = _OrchestratorFactory._build_orchestrator()

    def run(self, force_restart=False):
        """
        Start a Core service.

        This function check the configuration, start a dispatcher and lock the Config.
        """
        self.__manage_version(*self.cli_args)
        self.__check_config()
        self.__start_dispatcher(force_restart)

    def stop(self):
        """
        Stop the Core service.

        This function stops the dispatcher and unblock the Config for update.
        """
        Config.unblock_update()

        if self._dispatcher:
            self._dispatcher = _OrchestratorFactory._remove_dispatcher()
            self.__logger.info("Core service has been stopped.")

    def __check_config(self):
        Config.check()
        Config.block_update()

    def __manage_version(self, mode, _version_number, force, clean_entities):
        if mode == "development":
            current_version_number = _VersionManagerFactory._build_manager()._get_development_version()

            clean_all_entities_by_version(current_version_number)
            self.__logger.info(f"Development mode: Clean all entities of version {current_version_number}")

            _VersionManagerFactory._build_manager()._set_development_version(current_version_number)

        elif mode in ["experiment", "production"]:
            default_version_number = {
                "experiment": str(uuid.uuid4()),
                "production": _VersionManagerFactory._build_manager()._get_latest_version(),
            }
            version_setter = {
                "experiment": _VersionManagerFactory._build_manager()._set_experiment_version,
                "production": _VersionManagerFactory._build_manager()._set_production_version,
            }

            if _version_number:
                current_version_number = _version_number
            else:
                current_version_number = default_version_number[mode]

            if clean_entities:
                clean_all_entities_by_version(current_version_number)
                self.__logger.info(f"Clean all entities of version {current_version_number}")

            version_setter[mode](current_version_number, force)
            if mode == "production":
                self.__check_production_migration_config()
        else:
            raise SystemExit(f"Undefined execution mode: {mode}.")

    def __start_dispatcher(self, force_restart):
        if dispatcher := _OrchestratorFactory._build_dispatcher(force_restart=force_restart):
            self._dispatcher = dispatcher

        if Config.job_config.is_development:
            _Orchestrator._check_and_execute_jobs_if_development_mode()

    def __check_production_migration_config(self):
        collector = _MigrationConfigChecker(Config._applied_config, IssueCollector())._check()
        for issue in collector._warnings:
            self.__logger.warning(str(issue))
        for issue in collector._infos:
            self.__logger.info(str(issue))
        for issue in collector._errors:
            self.__logger.error(str(issue))
        if len(collector._errors) != 0:
            raise SystemExit("Configuration errors found. Please check the error log for more information.")
