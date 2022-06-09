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

import pathlib
from collections import defaultdict
from typing import List, Optional

from taipy.core.common.alias import TaskId
from taipy.core.pipeline._pipeline_model import _PipelineModel
from taipy.core.task._task_manager_factory import _TaskManagerFactory

from taipy.core._repository import _FileSystemRepository
from taipy.core.common import _utils
from taipy.core.config.config import Config
from taipy.core.exceptions.exceptions import NonExistingPipeline, NonExistingTask
from taipy.core.pipeline.pipeline import Pipeline
from taipy.core.task.task import Task


class _PipelineRepository(_FileSystemRepository[_PipelineModel, Pipeline]):
    def __init__(self):
        super().__init__(model=_PipelineModel, dir_name="pipelines")

    def _to_model(self, pipeline: Pipeline) -> _PipelineModel:
        datanode_task_edges = defaultdict(list)
        task_datanode_edges = defaultdict(list)
        for task in pipeline._tasks.values():
            task_id = str(task.id)
            for predecessor in task.input.values():
                datanode_task_edges[str(predecessor.id)].append(task_id)
            for successor in task.output.values():
                task_datanode_edges[task_id].append(str(successor.id))
        return _PipelineModel(
            pipeline.id,
            pipeline.parent_id,
            pipeline.config_id,
            pipeline._properties.data,
            [task.id for task in pipeline._tasks.values()],
            _utils._fcts_to_dict(pipeline._subscribers),
        )

    def _from_model(self, model: _PipelineModel, org_entity: Pipeline = None, lazy_loading: bool = True) -> Pipeline:
        try:
            tasks = self.__to_tasks(model.tasks, list(org_entity._tasks.values()) if org_entity else None, lazy_loading)
            pipeline = Pipeline(
                model.config_id,
                model.properties,
                tasks,
                model.id,
                model.parent_id,
                {_utils._load_fct(it["fct_module"], it["fct_name"]) for it in model.subscribers},  # type: ignore
            )
            return pipeline
        except NonExistingTask as err:
            raise err
        except KeyError:
            pipeline_err = NonExistingPipeline(model.id)
            raise pipeline_err

    @property
    def _storage_folder(self) -> pathlib.Path:
        return pathlib.Path(Config.global_config.storage_folder)  # type: ignore

    @staticmethod
    def __to_tasks(task_ids: List[TaskId], org_tasks: Optional[List[Task]] = None, lazy_loading: bool = True):
        tasks = []
        task_manager = _TaskManagerFactory._build_manager()

        if not lazy_loading or org_tasks is None:
            for _id in task_ids:
                if task := task_manager._get(_id):
                    tasks.append(task)
                else:
                    raise NonExistingTask(_id)
        else:
            org_tasks_dict = {t.id: t for t in org_tasks}

            for _id in task_ids:
                if _id in org_tasks_dict.keys():
                    print("lazy org")
                    task = org_tasks_dict[_id]
                else:
                    task = task_manager._get(_id)
                    print("lazy manager")
                if task:
                    tasks.append(task)
                else:
                    raise NonExistingTask(_id)
        return tasks
