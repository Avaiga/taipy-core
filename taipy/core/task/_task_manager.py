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

import itertools
from collections import defaultdict
from typing import Callable, List, Optional, Type, Union

from taipy.core._manager._manager import _Manager
from taipy.core._scheduler._abstract_scheduler import _AbstractScheduler
from taipy.core._scheduler._scheduler_factory import _SchedulerFactory
from taipy.core.common._entity_ids import _EntityIds
from taipy.core.common.alias import PipelineId, ScenarioId, TaskId
from taipy.core.common.scope import Scope
from taipy.core.config.task_config import TaskConfig
from taipy.core.data._data_manager_factory import _DataManagerFactory
from taipy.core.exceptions.exceptions import NonExistingTask
from taipy.core.job._job_manager_factory import _JobManagerFactory
from taipy.core.task._task_repository import _TaskRepository
from taipy.core.task.task import Task


class _TaskManager(_Manager[Task]):

    _repository: _TaskRepository = _TaskRepository()
    _ENTITY_NAME = Task.__name__
    __scheduler = None

    @classmethod
    def _scheduler(cls) -> Type[_AbstractScheduler]:
        if not cls.__scheduler:
            cls.__scheduler = _SchedulerFactory._build_scheduler()
        return cls.__scheduler

    @classmethod
    def _set(cls, task: Task, *args, **kwargs):  # type: ignore
        cls.__save_data_nodes(task.input.values())
        cls.__save_data_nodes(task.output.values())
        super()._set(task, *args, **kwargs)

    @classmethod
    def _get_or_create(
        cls,
        task_config: TaskConfig,
        scenario_id: Optional[ScenarioId] = None,
        pipeline_id: Optional[PipelineId] = None,
        *args,
        **kwargs,
    ) -> Task:
        dn_configs = set(itertools.chain(task_config.input_configs, task_config.output_configs))
        _data_nodes = _DataManagerFactory._build_manager()._get_or_creates(dn_configs, scenario_id, pipeline_id, *args,
                                                                           **kwargs)
        data_nodes = {dn_config: dn for dn_config, dn in _data_nodes}

        scope = min(dn.scope for dn in data_nodes.values()) if len(data_nodes) != 0 else Scope.GLOBAL
        parent_id = pipeline_id if scope == Scope.PIPELINE else scenario_id if scope == Scope.SCENARIO else None

        if tasks_from_parent := cls._repository._get_by_config_and_parent_id(task_config.id, parent_id):
            return tasks_from_parent

        inputs = [data_nodes[input_config] for input_config in task_config.input_configs]
        outputs = [data_nodes[output_config] for output_config in task_config.output_configs]
        task = Task(task_config.id, task_config.function, inputs, outputs, parent_id=parent_id)
        cls._set(task, *args, **kwargs)
        return task

    @classmethod
    def _get_or_creates(
        cls,
        task_configs: List[TaskConfig],
        scenario_id: Optional[ScenarioId] = None,
        pipeline_id: Optional[PipelineId] = None,
        *args,
        **kwargs,
    ) -> List[Task]:
        data_node_configs = []
        for task_config in task_configs:
            data_node_configs.extend(task_config.input_configs)
            data_node_configs.extend(task_config.output_configs)
        data_node_configs = set(data_node_configs)
        _data_nodes = _DataManagerFactory._build_manager()._get_or_creates(data_node_configs, scenario_id, pipeline_id,
                                                                           *args, **kwargs)
        data_nodes = {dn_config: dn for dn_config, dn in _data_nodes}

        tasks_configs_and_parent_id = []
        for task_config in task_configs:
            task_dn_configs = task_config.output_configs + task_config.input_configs
            task_config_data_nodes = [data_nodes[dn_config] for dn_config in task_dn_configs]

            scope = min(dn.scope for dn in task_config_data_nodes) if len(task_config_data_nodes) != 0 else Scope.GLOBAL
            parent_id = pipeline_id if scope == Scope.PIPELINE else scenario_id if scope == Scope.SCENARIO else None

            tasks_configs_and_parent_id.append((task_config, parent_id))

        task_config_and_tasks = cls._repository._get_by_configs_and_parent_ids(tasks_configs_and_parent_id)

        tasks = []
        for task_config, parent_id in tasks_configs_and_parent_id:
            if task := task_config_and_tasks.get((task_config, parent_id)):
                tasks.append(task)
            else:
                inputs = [data_nodes[input_config] for input_config in task_config.input_configs]
                outputs = [data_nodes[output_config] for output_config in task_config.output_configs]
                task = Task(task_config.id, task_config.function, inputs, outputs, parent_id=parent_id)
                cls._set(task, *args, **kwargs)
                tasks.append(task)

        return tasks

    @classmethod
    def __save_data_nodes(cls, data_nodes, *args, **kwargs):
        data_manager = _DataManagerFactory._build_manager()
        for i in data_nodes:
            data_manager._set(i, *args, **kwargs)

    @classmethod
    def _hard_delete(cls, task_id: TaskId, *args, **kwargs):
        task = cls._get(task_id, *args, **kwargs)
        entity_ids_to_delete = cls._get_owned_entity_ids(task, *args, **kwargs)
        entity_ids_to_delete.task_ids.add(task.id)
        cls._delete_entities_of_multiple_types(entity_ids_to_delete, *args, **kwargs)

    @classmethod
    def _get_owned_entity_ids(cls, task: Task, *args, **kwargs):
        entity_ids = _EntityIds()
        jobs = _JobManagerFactory._build_manager()._get_all(*args, **kwargs)
        for job in jobs:
            if job.task.id == task.id:
                entity_ids.job_ids.add(job.id)
        return entity_ids

    @classmethod
    def _submit(
        cls, task: Union[TaskId, Task], callbacks: Optional[List[Callable]] = None, force: bool = False, *args, **kwargs
    ):
        task_id = task.id if isinstance(task, Task) else task
        task = cls._get(task_id, *args, **kwargs)
        if task is None:
            raise NonExistingTask(task_id)
        cls._scheduler().submit_task(task, callbacks, force)
