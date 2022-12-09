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

import glob
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from functools import partial
from time import sleep
from unittest import mock
from unittest.mock import MagicMock

import pytest

from src.taipy.core._scheduler._dispatcher._development_job_dispatcher import _DevelopmentJobDispatcher
from src.taipy.core._scheduler._dispatcher._standalone_job_dispatcher import _StandaloneJobDispatcher
from src.taipy.core._scheduler._scheduler_factory import _SchedulerFactory
from src.taipy.core.common.alias import DataNodeId, JobId, TaskId
from src.taipy.core.config.job_config import JobConfig
from src.taipy.core.data._data_manager import _DataManager
from src.taipy.core.job.job import Job
from src.taipy.core.task.task import Task
from taipy.config.config import Config


def execute(lock):
    with lock:
        ...
    return None


def _error():
    raise RuntimeError("Something bad has happened")


def test_build_development_job_dispatcher():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)
    _SchedulerFactory._build_dispatcher()
    dispatcher = _SchedulerFactory._dispatcher

    assert isinstance(dispatcher, _DevelopmentJobDispatcher)
    assert dispatcher._nb_available_workers == 1

    assert dispatcher.start() == NotImplemented
    assert dispatcher.is_running()
    assert dispatcher.stop() == NotImplemented


def test_build_standalone_job_dispatcher():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)
    _SchedulerFactory._build_dispatcher()
    dispatcher = _SchedulerFactory._dispatcher

    assert not isinstance(dispatcher, _DevelopmentJobDispatcher)
    assert isinstance(dispatcher, _StandaloneJobDispatcher)
    assert isinstance(dispatcher._executor, ProcessPoolExecutor)
    assert dispatcher._nb_available_workers == 2
    assert_true_after_120_second_max(dispatcher.is_running)
    dispatcher.stop()
    dispatcher.join()
    assert_true_after_120_second_max(lambda: not dispatcher.is_running())


def test_can_execute_2_workers():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)

    m = multiprocessing.Manager()
    lock = m.Lock()

    task_id = TaskId("task_id1")
    output = list(_DataManager._bulk_get_or_create([Config.configure_data_node("input1", default_data=21)]).values())

    _SchedulerFactory._build_dispatcher()

    task = Task(
        config_id="name",
        input=[],
        function=partial(execute, lock),
        output=output,
        id=task_id,
    )
    job_id = JobId("id1")
    job = Job(job_id, task, "submit_id")

    dispatcher = _StandaloneJobDispatcher(_SchedulerFactory._scheduler)

    with lock:
        assert dispatcher._can_execute()
        dispatcher._dispatch(job)
        assert dispatcher._can_execute()
        dispatcher._dispatch(job)
        assert not dispatcher._can_execute()

    assert_true_after_120_second_max(lambda: dispatcher._can_execute())


def test_can_execute_synchronous():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)
    _SchedulerFactory._build_dispatcher()

    task_id = TaskId("task_id1")
    task = Task(config_id="name", input=[], function=print, output=[], id=task_id)
    job_id = JobId("id1")
    job = Job(job_id, task, "submit_id")

    dispatcher = _SchedulerFactory._dispatcher

    assert dispatcher._can_execute()
    dispatcher._dispatch(job)
    assert dispatcher._can_execute()


def test_exception_in_user_function():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)
    _SchedulerFactory._build_dispatcher()

    task_id = TaskId("task_id1")
    job_id = JobId("id1")
    task = Task(config_id="name", input=[], function=_error, output=[], id=task_id)
    job = Job(job_id, task, "submit_id")

    dispatcher = _SchedulerFactory._dispatcher
    dispatcher._dispatch(job)
    assert job.is_failed()
    assert 'RuntimeError("Something bad has happened")' in str(job.stacktrace[0])


def test_exception_in_writing_data():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)
    _SchedulerFactory._build_dispatcher()

    task_id = TaskId("task_id1")
    job_id = JobId("id1")
    output = MagicMock()
    output.id = DataNodeId("output_id")
    output.config_id = "my_raising_datanode"
    output._is_in_cache = False
    output.write.side_effect = ValueError()
    task = Task(config_id="name", input=[], function=print, output=[output], id=task_id)
    job = Job(job_id, task, "submit_id")

    dispatcher = _SchedulerFactory._dispatcher

    with mock.patch("src.taipy.core.data._data_manager._DataManager._get") as get:
        get.return_value = output
        dispatcher._dispatch(job)
        assert job.is_failed()
        assert "node" in job.stacktrace[0]


def assert_true_after_120_second_max(assertion):
    start = datetime.now()
    while (datetime.now() - start).seconds < 120:
        sleep(0.1)  # Limit CPU usage
        if assertion():
            return
    assert assertion()
