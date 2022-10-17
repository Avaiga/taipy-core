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

from typing import Callable, List, Optional, Type, Union

from taipy.config.common.scope import Scope

from .._manager._manager import _Manager
from .._scheduler._abstract_scheduler import _AbstractScheduler
from .._scheduler._scheduler_factory import _SchedulerFactory
from ..common._entity_ids import _EntityIds
from ..common.alias import PipelineId, ScenarioId, TaskId
from ..config.task_config import TaskConfig
from ..data._data_manager_factory import _DataManagerFactory
from ..exceptions.exceptions import NonExistingTask
from ..job._job_manager_factory import _JobManagerFactory
from ..task._task_repository_factory import _TaskRepositoryFactory
from ..task.task import Task


class _TaskManager(_Manager[Task]):

    _repository = _TaskRepositoryFactory._build_repository()  # type: ignore
    _ENTITY_NAME = Task.__name__

    @classmethod
    def _scheduler(cls) -> Type[_AbstractScheduler]:
        return _SchedulerFactory._build_scheduler()

    @classmethod
    def _set(cls, task: Task):
        cls.__save_data_nodes(task.input.values())
        cls.__save_data_nodes(task.output.values())
        super()._set(task)

    @classmethod
    def _bulk_get_or_create(
        cls,
        task_configs: List[TaskConfig],
        scenario_id: Optional[ScenarioId] = None,
        pipeline_id: Optional[PipelineId] = None,
    ) -> List[Task]:
        data_node_configs = set()
        for task_config in task_configs:
            data_node_configs.update(task_config.input_configs)
            data_node_configs.update(task_config.output_configs)

        data_nodes = _DataManagerFactory._build_manager()._bulk_get_or_create(
            data_node_configs, scenario_id, pipeline_id
        )

        tasks_configs_and_parent_id = []
        for task_config in task_configs:
            task_dn_configs = task_config.output_configs + task_config.input_configs
            task_config_data_nodes = [data_nodes[dn_config] for dn_config in task_dn_configs]

            scope = min(dn.scope for dn in task_config_data_nodes) if len(task_config_data_nodes) != 0 else Scope.GLOBAL
            parent_id = pipeline_id if scope == Scope.PIPELINE else scenario_id if scope == Scope.SCENARIO else None

            tasks_configs_and_parent_id.append((task_config, parent_id))

        tasks_by_config = cls._repository._get_by_configs_and_parent_ids(tasks_configs_and_parent_id)  # type: ignore

        tasks = []
        for task_config, parent_id in tasks_configs_and_parent_id:
            if task := tasks_by_config.get((task_config, parent_id)):
                tasks.append(task)
            else:
                inputs = [data_nodes[input_config] for input_config in task_config.input_configs]
                outputs = [data_nodes[output_config] for output_config in task_config.output_configs]
                task = Task(str(task_config.id), task_config.function, inputs, outputs, parent_id=parent_id)
                cls._set(task)
                tasks.append(task)

        return tasks

    @classmethod
    def __save_data_nodes(cls, data_nodes):
        data_manager = _DataManagerFactory._build_manager()
        for i in data_nodes:
            data_manager._set(i)

    @classmethod
    def _hard_delete(cls, task_id: TaskId):
        task = cls._get(task_id)
        entity_ids_to_delete = cls._get_owned_entity_ids(task)
        entity_ids_to_delete.task_ids.add(task.id)
        cls._delete_entities_of_multiple_types(entity_ids_to_delete)

    @classmethod
    def _get_owned_entity_ids(cls, task: Task):
        entity_ids = _EntityIds()
        jobs = _JobManagerFactory._build_manager()._get_all()
        for job in jobs:
            if job.task.id == task.id:
                entity_ids.job_ids.add(job.id)
        return entity_ids

    @classmethod
    def _submit(
        cls,
        task: Union[TaskId, Task],
        callbacks: Optional[List[Callable]] = None,
        force: bool = False,
        wait: bool = False,
        timeout: Optional[Union[float, int]] = None,
    ):
        task_id = task.id if isinstance(task, Task) else task
        task = cls._get(task_id)
        if task is None:
            raise NonExistingTask(task_id)
        return cls._scheduler().submit_task(task, callbacks=callbacks, force=force, wait=wait, timeout=timeout)
