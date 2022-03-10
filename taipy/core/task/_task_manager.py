import itertools
from typing import List, Optional

from taipy.core.common._manager import _Manager
from taipy.core.common.alias import PipelineId, ScenarioId, TaskId
from taipy.core.config.task_config import TaskConfig
from taipy.core.data._data_manager import _DataManager
from taipy.core.data.scope import Scope
from taipy.core.exceptions.exceptions import MultipleTaskFromSameConfigWithSameParent
from taipy.core.job._job_manager import _JobManager
from taipy.core.scheduler.abstract_scheduler import AbstractScheduler
from taipy.core.scheduler.scheduler_factory import SchedulerFactory
from taipy.core.task._task_repository import _TaskRepository
from taipy.core.task.task import Task


class _TaskManager(_Manager[Task]):

    _repository: _TaskRepository = _TaskRepository()
    _ENTITY_NAME = Task.__name__
    __scheduler = None

    @classmethod
    def _scheduler(cls) -> AbstractScheduler:
        if not cls.__scheduler:
            cls.__scheduler = SchedulerFactory.build_scheduler()
        return cls.__scheduler

    @classmethod
    def _set(cls, task: Task):
        cls.__save_data_nodes(task.input.values())
        cls.__save_data_nodes(task.output.values())
        super()._set(task)

    @classmethod
    def _get_or_create(
        cls,
        task_config: TaskConfig,
        scenario_id: Optional[ScenarioId] = None,
        pipeline_id: Optional[PipelineId] = None,
    ) -> Task:
        data_nodes = {
            dn_config: _DataManager._get_or_create(dn_config, scenario_id, pipeline_id)
            for dn_config in set(itertools.chain(task_config.input_configs, task_config.output_configs))
        }
        scope = min(dn.scope for dn in data_nodes.values()) if len(data_nodes) != 0 else Scope.GLOBAL
        parent_id = pipeline_id if scope == Scope.PIPELINE else scenario_id if scope == Scope.SCENARIO else None
        tasks_from_config_id = cls._get_all_by_config_id(task_config.id)
        tasks_from_parent = [task for task in tasks_from_config_id if task.parent_id == parent_id]
        if len(tasks_from_parent) == 1:
            return tasks_from_parent[0]
        elif len(tasks_from_parent) > 1:
            raise MultipleTaskFromSameConfigWithSameParent
        else:
            inputs = [data_nodes[input_config] for input_config in task_config.input_configs]
            outputs = [data_nodes[output_config] for output_config in task_config.output_configs]
            task = Task(task_config.id, task_config.function, inputs, outputs, parent_id=parent_id)
            cls._set(task)
            return task

    @classmethod
    def __save_data_nodes(cls, data_nodes):
        for i in data_nodes:
            _DataManager._set(i)

    @classmethod
    def _hard_delete(
        cls, task_id: TaskId, scenario_id: Optional[ScenarioId] = None, pipeline_id: Optional[PipelineId] = None
    ):
        task = cls._get(task_id)
        jobs = _JobManager._get_all()
        for job in jobs:
            if job.task.id == task.id:
                _JobManager._delete(job)
        if scenario_id:
            cls._remove_if_parent_id_eq(task.input.values(), scenario_id)
            cls._remove_if_parent_id_eq(task.output.values(), scenario_id)
        if pipeline_id:
            cls._remove_if_parent_id_eq(task.input.values(), pipeline_id)
            cls._remove_if_parent_id_eq(task.output.values(), pipeline_id)
        cls._delete(task_id)

    @classmethod
    def _remove_if_parent_id_eq(cls, data_nodes, id_):
        for data_node in data_nodes:
            if data_node.parent_id == id_:
                _DataManager._delete(data_node.id)

    @classmethod
    def _get_all_by_config_id(cls, config_id: str) -> List[Task]:
        return cls._repository._search_all("config_id", config_id)
