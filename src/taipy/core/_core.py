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
from taipy.logger._taipy_logger import _TaipyLogger

from ._core_cli import _CoreCLI
from ._orchestrator._dispatcher._job_dispatcher import _JobDispatcher
from ._orchestrator._orchestrator import _Orchestrator
from ._orchestrator._orchestrator_factory import _OrchestratorFactory
from ._version._version_manager_factory import _VersionManagerFactory
from .config._service_config import default_service_config
from .taipy import clean_all_entities_by_version


class Core:
    """
    Core service
    """

    __logger = _TaipyLogger._get_logger()

    _orchestrator: Optional[_Orchestrator] = None
    _dispatcher: Optional[_JobDispatcher] = None

    def __init__(self):
        """
        Initialize a Core service.
        """
        self._service_config = {}
        self._service_config.update(default_service_config)

        self._orchestrator = _OrchestratorFactory._build_orchestrator()

    def run(self, force_restart=False):
        """
        Start a Core service.

        This function checks the configuration, manages application's version,
        and starts a dispatcher and lock the Config.
        """
        self.__build_service_config()

        self.__check_config()
        self.__manage_version()
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

    def __build_service_config(self):
        try:
            core_section = Config.unique_sections["core"]
            self._service_config.update(core_section._to_dict())
        except KeyError:
            self.__logger.warn("taipy-config section for taipy-core is not initialized")

        self.__handle_argparse()

    def __handle_argparse(self):
        """Load from system arguments"""
        _CoreCLI.create_parser()
        args = _CoreCLI.parse_arguments()

        if args.development:
            self._service_config["mode"] = "development"
        elif args.experiment is not None:
            self._service_config["mode"] = "experiment"
            self._service_config["version_number"] = args.experiment
        elif args.production is not None:
            self._service_config["mode"] = "production"
            self._service_config["version_number"] = args.production

        if args.taipy_force:
            self._service_config["force"] = True
        elif args.no_taipy_force:
            self._service_config["force"] = False

        if args.clean_entities:
            self._service_config["clean_entities"] = True
        elif args.no_clean_entities:
            self._service_config["clean_entities"] = False

    def __check_config(self):
        Config.check()
        Config.block_update()

    def __manage_version(self):
        mode = self._service_config["mode"]
        version_number = self._service_config["version_number"]
        clean_entities = self._service_config["clean_entities"]
        force = self._service_config["force"]

        if mode == "development":
            current_version_number = _VersionManagerFactory._build_manager()._get_development_version()

            self.__logger.info(f"Development mode: Clean all entities of version {current_version_number}")
            clean_all_entities_by_version(current_version_number)

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

            if version_number:
                current_version_number = version_number
            else:
                current_version_number = default_version_number[mode]

            if clean_entities:
                if clean_all_entities_by_version(current_version_number):
                    self.__logger.info(f"Clean all entities of version {current_version_number}")

            version_setter[mode](current_version_number, force)

        else:
            raise SystemExit(f"Undefined execution mode: {mode}.")

    def __start_dispatcher(self, force_restart):
        if dispatcher := _OrchestratorFactory._build_dispatcher(force_restart=force_restart):
            self._dispatcher = dispatcher

        if Config.job_config.is_development:
            _Orchestrator._check_and_execute_jobs_if_development_mode()
