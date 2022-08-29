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

from taipy.config._toml_serializer import _TomlSerializer
from taipy.config.config import Config

from ...job.job import Job
from .._abstract_scheduler import _AbstractScheduler
from ._job_dispatcher import _JobDispatcher


class _DevelopmentJobDispatcher(_JobDispatcher):
    """Manages job dispatching (instances of `Job^` class) in a synchronous way."""

    def __init__(self, scheduler: _AbstractScheduler):
        super().__init__(scheduler)

    def start(self):
        return NotImplemented

    def is_running(self) -> bool:
        return True

    def stop(self):
        return NotImplemented

    def run(self):
        return NotImplemented

    def _dispatch(self, job: Job):
        """Dispatches the given `Job^` on an available worker for execution.

        Parameters:
            job (Job^): The job to submit on an executor with an available worker.
        """
        config_as_string = _TomlSerializer()._serialize(Config._applied_config)

        rs = self._run_wrapped_function(Config.job_config.mode, config_as_string, job.id, job.task)
        self._update_job_status(job, rs)
