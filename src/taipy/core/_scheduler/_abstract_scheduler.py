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

from abc import abstractmethod
from typing import Callable, Iterable, List, Optional, Union

from ..job.job import Job
from ..task.task import Task


class _AbstractScheduler:
    """Creates, Enqueues and schedules jobs as instances of `Job^` class."""

    @classmethod
    @abstractmethod
    def initialize(cls):
        return NotImplemented

    @classmethod
    @abstractmethod
    def submit(
        cls,
        pipeline,
        callbacks: Optional[Iterable[Callable]],
        force: bool = False,
        wait: bool = False,
        timeout: Optional[Union[float, int]] = None,
    ) -> List[Job]:
        return NotImplemented

    @classmethod
    @abstractmethod
    def submit_task(
        cls,
        task: Task,
        submit_id: str = None,
        callbacks: Optional[Iterable[Callable]] = None,
        force: bool = False,
        wait=False,
        timeout: Optional[Union[float, int]] = None,
    ) -> Job:
        return NotImplemented

    @classmethod
    @abstractmethod
    def cancel_job(cls, job):
        return NotImplemented
