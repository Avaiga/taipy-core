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

from unittest import mock

from taipy.core.data._data_repository import _DataRepository
from taipy.core.pipeline._pipeline_repository import _PipelineRepository
from taipy.core.task._task_repository import _TaskRepository

from taipy.core.pipeline.pipeline import Pipeline
from taipy.core.task.task import Task


def test_save_and_load(tmpdir, pipeline):
    repository = _PipelineRepository()
    repository.base_path = tmpdir
    repository._save(pipeline)
    loaded_pipeline = repository.load("pipeline_id")

    assert isinstance(loaded_pipeline, Pipeline)
    assert pipeline.id == loaded_pipeline.id


def test_from_and_to_model(pipeline, pipeline_model):
    repository = _PipelineRepository()
    assert repository._to_model(pipeline) == pipeline_model
    assert repository._from_model(pipeline_model) == pipeline


def test_eager_loading(pipeline):
    pl_rp = _PipelineRepository()
    ta_rp = _TaskRepository()

    task = Task("task_config_id", print, [], [])
    task_1 = Task("task_config_id_1", print, [], [])

    pl_rp._save(pipeline)
    ta_rp._save(task)
    ta_rp._save(task_1)

    assert len(pipeline._tasks) == 0
    pipeline.tasks = [task]

    with mock.patch("taipy.core.task._task_manager._TaskManager._get") as mck:
        pl_rp.load(pipeline.id, eager_loading=True)
        mck.assert_called_once()

    pipeline.tasks = [task, task_1]
    with mock.patch("taipy.core.task._task_manager._TaskManager._get") as mck:
        pl_rp.load(pipeline.id, eager_loading=True)
        assert mck.call_count == 2

    pipeline_1 = pl_rp.load(pipeline.id, eager_loading=True)
    assert len(pipeline_1._tasks) == 2

    pipeline.tasks = [task]
    with mock.patch("taipy.core.task._task_manager._TaskManager._get") as mck:
        pl_rp.load(pipeline.id, pipeline_1, eager_loading=True)
        mck.assert_called_once()


def test_lazy_loading(pipeline):
    pl_rp = _PipelineRepository()
    ta_rp = _TaskRepository()

    task = Task("task_config_id", print, [], [])
    task_1 = Task("task_config_id_1", print, [], [])

    pl_rp._save(pipeline)
    ta_rp._save(task)
    ta_rp._save(task_1)

    assert len(pipeline._tasks) == 0
    pipeline.tasks = [task]

    # with mock.patch('taipy.core.task._task_manager._TaskManager._get') as mck:
    #     pl_rp.load(pipeline.id, pipeline)
    #     mck.assert_not_called()

    # with mock.patch('taipy.core.task._task_manager._TaskManager._get') as mck:
    #     pl_rp.load(pipeline.id)
    #     mck.assert_called_once()

    pipeline_1 = pl_rp.load(pipeline.id, pipeline)
    assert len(pipeline_1._tasks) == 1

    pipeline.tasks = [task, task_1]
    with mock.patch("taipy.core.task._task_manager._TaskManager._get") as mck:
        pl_rp.load(pipeline.id, pipeline)
        mck.assert_not_called()

    with mock.patch("taipy.core.task._task_manager._TaskManager._get") as mck:
        pl_rp.load(pipeline.id, pipeline_1)
        mck.assert_called_once()

    pipeline.tasks = [task]
    with mock.patch("taipy.core.task._task_manager._TaskManager._get") as mck:
        pl_rp.load(pipeline.id, pipeline_1)
        mck.assert_not_called()

    pipeline.tasks = [task_1]
    with mock.patch("taipy.core.task._task_manager._TaskManager._get") as mck:
        pl_rp.load(pipeline.id, pipeline_1)
        mck.assert_called_once()

    pipeline.tasks = []
    with mock.patch("taipy.core.task._task_manager._TaskManager._get") as mck:
        pl_rp.load(pipeline.id, pipeline_1)
        mck.assert_not_called()

    with mock.patch("taipy.core.task._task_manager._TaskManager._get") as mck:
        pl_rp.load(pipeline.id, pipeline)
        mck.assert_not_called()
