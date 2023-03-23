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

from datetime import datetime
from typing import List

from .._repository._v2._abstract_converter import _AbstractConverter
from ..common._utils import _fcts_to_dict, _load_fct
from ..exceptions import InvalidSubscriber
from ..job._job_model import _JobModel
from ..job.job import Job
from ..task._task_repository_factory import _TaskRepositoryFactory


class _JobConverter(_AbstractConverter):
    @classmethod
    def _entity_to_model(cls, job: Job) -> _JobModel:
        return _JobModel(
            job.id,
            job._task.id,
            job._status,
            job._force,
            job.submit_id,
            job._creation_date.isoformat(),
            cls._serialize_subscribers(job._subscribers),
            job._stacktrace,
            version=job.version,
        )

    @classmethod
    def _model_to_entity(cls, model: _JobModel) -> Job:
        task_repository = _TaskRepositoryFactory._build_repository()
        job = Job(
            id=model.id, task=task_repository.load(model.task_id), submit_id=model.submit_id, version=model.version
        )

        job.status = model.status  # type: ignore
        job.force = model.force  # type: ignore
        job.creation_date = datetime.fromisoformat(model.creation_date)  # type: ignore
        for it in model.subscribers:
            try:
                job._subscribers.append(_load_fct(it.get("fct_module"), it.get("fct_name")))  # type:ignore
            except AttributeError:
                raise InvalidSubscriber(f"The subscriber function {it.get('fct_name')} cannot be loaded.")
        job._stacktrace = model.stacktrace

        return job

    @staticmethod
    def _serialize_subscribers(subscribers: List) -> List:
        return _fcts_to_dict(subscribers)