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
from typing import Optional

from taipy.logger._taipy_logger import _TaipyLogger

from ._scheduler._dispatcher._job_dispatcher import _JobDispatcher
from ._scheduler._scheduler import _Scheduler
from ._scheduler._scheduler_factory import _SchedulerFactory
from ._version._version_cli import version_cli
from ._version._version_manager import _VersionManager
from .exceptions.exceptions import VersionAlreadyExists
from .taipy import clean_all_entities_by_version


class Core:
    """
    Core service
    """

    _scheduler: Optional[_Scheduler] = None
    _dispatcher: Optional[_JobDispatcher] = None

    def __init__(self):
        """
        Initialize a Core service.
        """
        self._scheduler = _SchedulerFactory._build_scheduler()

    def run(self, force_restart=False):
        """
        Start a Core service. This method is blocking.
        """
        cli_args = version_cli(standalone_mode=False)
        if cli_args == 0:  # run --help command
            exit()

        self.__setup_versioning_module(*cli_args)

        if dispatcher := _SchedulerFactory._build_dispatcher(force_restart=force_restart):
            self._dispatcher = dispatcher

    def __setup_versioning_module(self, mode, _version_number, _override):
        if mode == "experiment":
            if _version_number:
                curren_version_number = _version_number
            else:
                curren_version_number = str(uuid.uuid4())

            override = _override

        elif mode == "development":
            curren_version_number = _VersionManager.get_development_version()
            _VersionManager.set_development_version(curren_version_number)
            override = True

            _TaipyLogger._get_logger().info(
                f"Development mode: Clean all entities with version {curren_version_number}"
            )

        else:
            _TaipyLogger._get_logger().error("Undefined execution mode.")
            return

        if override:
            clean_all_entities_by_version(curren_version_number)

        try:
            _VersionManager.set_current_version(curren_version_number, override)
        except VersionAlreadyExists as e:
            raise SystemExit(e)
