from functools import partial
from typing import Callable, List, Optional, Union

from taipy.core.common._entity_ids import _EntityIds
from taipy.core.common._manager import _Manager
from taipy.core.common.alias import PipelineId, ScenarioId
from taipy.core.config.pipeline_config import PipelineConfig
from taipy.core.data._data_manager import _DataManager
from taipy.core.data.scope import Scope
from taipy.core.exceptions.exceptions import MultiplePipelineFromSameConfigWithSameParent, NonExistingPipeline
from taipy.core.job._job_manager import _JobManager
from taipy.core.job.job import Job
from taipy.core.pipeline._pipeline_repository import _PipelineRepository
from taipy.core.pipeline.pipeline import Pipeline
from taipy.core.task._task_manager import _TaskManager


class _PipelineManager(_Manager[Pipeline]):
    _repository = _PipelineRepository()
    _ENTITY_NAME = Pipeline.__name__

    @classmethod
    def _subscribe(cls, callback: Callable[[Pipeline, Job], None], pipeline: Optional[Pipeline] = None):
        if pipeline is None:
            pipelines = cls._get_all()
            for pln in pipelines:
                cls.__add_subscriber(callback, pln)
            return

        cls.__add_subscriber(callback, pipeline)

    @classmethod
    def _unsubscribe(cls, callback: Callable[[Pipeline, Job], None], pipeline: Optional[Pipeline] = None):

        if pipeline is None:
            pipelines = cls._get_all()
            for pln in pipelines:
                cls.__remove_subscriber(callback, pln)
            return

        cls.__remove_subscriber(callback, pipeline)

    @classmethod
    def __add_subscriber(cls, callback, pipeline):
        pipeline._add_subscriber(callback)
        cls._set(pipeline)

    @classmethod
    def __remove_subscriber(cls, callback, pipeline):
        pipeline._remove_subscriber(callback)
        cls._set(pipeline)

    @classmethod
    def _get_or_create(cls, pipeline_config: PipelineConfig, scenario_id: Optional[ScenarioId] = None) -> Pipeline:
        pipeline_id = Pipeline._new_id(pipeline_config.id)
        tasks = [
            _TaskManager._get_or_create(t_config, scenario_id, pipeline_id) for t_config in pipeline_config.task_configs
        ]
        scope = min(task.scope for task in tasks) if len(tasks) != 0 else Scope.GLOBAL
        parent_id = scenario_id if scope == Scope.SCENARIO else pipeline_id if scope == Scope.PIPELINE else None
        pipelines_from_config_id = cls._get_all_by_config_id(pipeline_config.id)
        pipelines_from_parent = [pipeline for pipeline in pipelines_from_config_id if pipeline.parent_id == parent_id]
        if len(pipelines_from_parent) == 1:
            return pipelines_from_parent[0]
        elif len(pipelines_from_parent) > 1:
            raise MultiplePipelineFromSameConfigWithSameParent
        else:
            pipeline = Pipeline(pipeline_config.id, dict(**pipeline_config.properties), tasks, pipeline_id, parent_id)
            cls._set(pipeline)
            return pipeline

    @classmethod
    def _submit(
        cls, pipeline: Union[PipelineId, Pipeline], callbacks: Optional[List[Callable]] = None, force: bool = False
    ):
        callbacks = callbacks or []
        pipeline_id = pipeline.id if isinstance(pipeline, Pipeline) else pipeline
        pipeline = cls._get(pipeline_id)
        if pipeline is None:
            raise NonExistingPipeline(pipeline_id)
        pipeline_subscription_callback = cls.__get_status_notifier_callbacks(pipeline) + callbacks
        _TaskManager._scheduler().submit(pipeline, callbacks=pipeline_subscription_callback, force=force)

    @staticmethod
    def __get_status_notifier_callbacks(pipeline: Pipeline) -> List:
        return [partial(c, pipeline) for c in pipeline.subscribers]

    @classmethod
    def _hard_delete(cls, pipeline_id: PipelineId):
        pipeline = cls._get(pipeline_id)
        owned_entity_ids = cls._get_owned_entity_ids(pipeline)
        cls._delete_entities_of_multiple_types(owned_entity_ids)

    @classmethod
    def _get_owned_entity_ids(cls, pipeline: Pipeline) -> _EntityIds:
        entity_ids = _EntityIds()
        entity_ids.pipeline_ids.add(pipeline.id)

        for task in pipeline._tasks.values():
            if task.parent_id == pipeline.id:
                entity_ids.task_ids.add(task.id)
            for data_node in task.data_nodes.values():
                if data_node.parent_id == pipeline.id:
                    entity_ids.data_node_ids.add(data_node.id)
        jobs = _JobManager._get_all()
        for task_id in entity_ids.task_ids:
            for job in jobs:
                if job.task.id == task_id:
                    entity_ids.job_ids.add(job.id)
        return entity_ids

    @classmethod
    def _get_all_by_config_id(cls, config_id: str) -> List[Pipeline]:
        return cls._repository._search_all("config_id", config_id)
