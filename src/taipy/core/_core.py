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

from multiprocessing import Lock
from typing import Optional

from taipy.config import Config
from taipy.logger._taipy_logger import _TaipyLogger

from ._backup._backup import _init_backup_file_with_storage_folder
from ._core_cli import _CoreCLI
from ._orchestrator._dispatcher._job_dispatcher import _JobDispatcher
from ._orchestrator._orchestrator import _Orchestrator
from ._orchestrator._orchestrator_factory import _OrchestratorFactory
from ._version._version_manager_factory import _VersionManagerFactory
from .config import CoreSection
from .exceptions.exceptions import CoreServiceIsAlreadyRunning


class Core:
    """
    Core service
    """

    _is_running = False
    __lock_is_running = Lock()

    __logger = _TaipyLogger._get_logger()

    _orchestrator: Optional[_Orchestrator] = None
    _dispatcher: Optional[_JobDispatcher] = None

    def __init__(self):
        """
        Initialize a Core service.
        """
        pass

    def run(self, force_restart=False):
        """
        Start a Core service.

        This function checks the configuration, manages application's version,
        and starts a dispatcher and lock the Config.
        """
        if self.__class__._is_running:
            raise CoreServiceIsAlreadyRunning

        with self.__class__.__lock_is_running:
            self.__class__._is_running = True

        self.__update_core_section()
        self.__manage_version()
        self.__check_and_block_config()

        if self._orchestrator is None:
            self._orchestrator = _OrchestratorFactory._build_orchestrator()

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

        with self.__class__.__lock_is_running:
            self.__class__._is_running = False

    @staticmethod
    def __update_core_section():
        _CoreCLI.create_parser()
        Config._applied_config._unique_sections[CoreSection.name]._update(_CoreCLI.parse_arguments())

    @staticmethod
    def __manage_version():
        _VersionManagerFactory._build_manager()._manage_version()
        Config._applied_config._unique_sections[CoreSection.name]._update(
            {"version_number": _VersionManagerFactory._build_manager()._get_latest_version()}
        )

    @staticmethod
    def __check_and_block_config():
        Config.check()
        Config.block_update()
        _init_backup_file_with_storage_folder()

    def __start_dispatcher(self, force_restart):
        if dispatcher := _OrchestratorFactory._build_dispatcher(force_restart=force_restart):
            self._dispatcher = dispatcher

        if Config.job_config.is_development:
            _Orchestrator._check_and_execute_jobs_if_development_mode()
