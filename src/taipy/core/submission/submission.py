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
from datetime import datetime
from typing import Any, List, Optional, Union

from .._entity._entity import _Entity
from .._entity._labeled import _Labeled
from .._entity._reload import _self_reload, _self_setter
from .._version._version_manager_factory import _VersionManagerFactory
from ..job._job_manager_factory import _JobManagerFactory
from ..job.job import Job, JobId
from ..notification.event import Event, EventEntityType, EventOperation, _make_event
from .submission_id import SubmissionId
from .submission_status import SubmissionStatus


class Submission(_Entity, _Labeled):
    """Hold the jobs and submission status when a Scenario^, Sequence^ or Task^ is submitted.

    Attributes:
        entity_id (str): The identifier of the entity that was submitted.
        id (str): The identifier of the `Submission^` entity.
        jobs (Optional[Union[List[Job], List[JobId]]]): A list of jobs.
        creation_date (Optional[datetime]): The date of this submission's creation.
        submission_status (Optional[SubmissionStatus]): The current status of this submission.
        version (Optional[str]): The string indicates the application version of the submission to instantiate.
            If not provided, the latest version is used.
    """

    _ID_PREFIX = "SUBMISSION"
    _MANAGER_NAME = "submission"
    __SEPARATOR = "_"

    def __init__(
        self,
        entity_id: str,
        entity_type: str,
        id: Optional[str] = None,
        jobs: Optional[Union[List[Job], List[JobId]]] = None,
        creation_date: Optional[datetime] = None,
        submission_status: Optional[SubmissionStatus] = None,
        version: Optional[str] = None,
    ):
        self._entity_id = entity_id
        self._entity_type = entity_type
        self.id = id or self.__new_id()
        self._jobs: Union[List[Job], List[JobId], List] = jobs or []
        self._creation_date = creation_date or datetime.now()
        self._submission_status = submission_status or SubmissionStatus.SUBMITTED
        self._version = version or _VersionManagerFactory._build_manager()._get_latest_version()

    @staticmethod
    def __new_id() -> str:
        """Generate a unique Submission identifier."""
        return SubmissionId(Submission.__SEPARATOR.join([Submission._ID_PREFIX, str(uuid.uuid4())]))

    @property
    def entity_id(self) -> str:
        return self._entity_id

    @property
    def entity_type(self) -> str:
        return self._entity_type

    @property
    def creation_date(self):
        return self._creation_date

    def get_label(self) -> str:
        """Returns the submission simple label prefixed by its owner label.

        Returns:
            The label of the submission as a string.
        """
        return self._get_label()

    def get_simple_label(self) -> str:
        """Returns the submission simple label.

        Returns:
            The simple label of the submission as a string.
        """
        return self._get_simple_label()

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def jobs(self) -> List[Job]:
        jobs = []
        job_manager = _JobManagerFactory._build_manager()

        for job in self._jobs:
            jobs.append(job_manager._get(job))

        return jobs

    @jobs.setter  # type: ignore
    @_self_setter(_MANAGER_NAME)
    def jobs(self, jobs: Union[List[Job], List[JobId]]):
        self._jobs = jobs

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id

    @property  # type: ignore
    @_self_reload(_MANAGER_NAME)
    def submission_status(self):
        return self._submission_status

    @submission_status.setter  # type: ignore
    @_self_setter(_MANAGER_NAME)
    def submission_status(self, submission_status):
        self._submission_status = submission_status

    def __lt__(self, other):
        return self.creation_date.timestamp() < other.creation_date.timestamp()

    def __le__(self, other):
        return self.creation_date.timestamp() <= other.creation_date.timestamp()

    def __gt__(self, other):
        return self.creation_date.timestamp() > other.creation_date.timestamp()

    def __ge__(self, other):
        return self.creation_date.timestamp() >= other.creation_date.timestamp()

    def _update_submission_status(self, j: Job):
        abandoned = False
        canceled = False
        blocked = False
        pending = False
        running = False
        completed = False

        for job in self.jobs:
            if not job:
                continue
            if job.is_failed():
                self.submission_status = SubmissionStatus.FAILED  # type: ignore
                return
            if job.is_canceled():
                canceled = True
                continue
            if job.is_blocked():
                blocked = True
                continue
            if job.is_pending() or job.is_submitted():
                pending = True
                continue
            if job.is_running():
                running = True
                continue
            if job.is_completed() or job.is_skipped():
                completed = True
                continue
            if job.is_abandoned():
                abandoned = True
        if canceled:
            self.submission_status = SubmissionStatus.CANCELED  # type: ignore
            return
        if abandoned:
            self.submission_status = SubmissionStatus.UNDEFINED  # type: ignore
            return
        if running:
            self.submission_status = SubmissionStatus.RUNNING  # type: ignore
            return
        if pending:
            self.submission_status = SubmissionStatus.PENDING  # type: ignore
            return
        if blocked:
            self.submission_status = SubmissionStatus.BLOCKED  # type: ignore
            return
        if completed:
            self.submission_status = SubmissionStatus.COMPLETED  # type: ignore
            return
        self.submission_status = SubmissionStatus.UNDEFINED  # type: ignore


@_make_event.register(Submission)
def _make_event_for_submission(
    submission: Submission,
    operation: EventOperation,
    /,
    attribute_name: Optional[str] = None,
    attribute_value: Optional[Any] = None,
    **kwargs,
) -> Event:
    metadata = {"creation_date": submission.creation_date, "version": submission._version}
    return Event(
        entity_type=EventEntityType.SUBMISSION,
        entity_id=submission.id,
        operation=operation,
        attribute_name=attribute_name,
        attribute_value=attribute_value,
        metadata={**metadata, **kwargs},
    )
