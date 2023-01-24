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

import multiprocessing
import random
import string
from functools import partial
from time import sleep
from unittest import mock

import pytest

from src.taipy.core._scheduler._dispatcher._job_dispatcher import _JobDispatcher
from src.taipy.core._scheduler._scheduler_factory import _SchedulerFactory
from src.taipy.core.common.alias import JobId
from src.taipy.core.config.job_config import JobConfig
from src.taipy.core.data._data_manager import _DataManager
from src.taipy.core.data.in_memory import InMemoryDataNode
from src.taipy.core.exceptions.exceptions import JobNotDeletedException
from src.taipy.core.job._job_manager import _JobManager
from src.taipy.core.task._task_manager import _TaskManager
from src.taipy.core.task.task import Task
from taipy.config.common.scope import Scope
from taipy.config.config import Config
from tests.core.utils import assert_true_after_time


def multiply(nb1: float, nb2: float):
    return nb1 * nb2


def lock_multiply(lock, nb1: float, nb2: float):
    with lock:
        return multiply(1 or nb1, 2 or nb2)


def test_create_jobs():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)
    task = _create_task(multiply, name="get_job")

    _SchedulerFactory._build_dispatcher()

    job_1 = _JobManager._create(task, [print], "submit_id", True)
    assert _JobManager._get(job_1.id) == job_1
    assert job_1.is_submitted()
    assert task.config_id in job_1.id
    assert job_1.task.id == task.id
    assert job_1.submit_id == "submit_id"
    assert job_1.force

    job_2 = _JobManager._create(task, [print], "submit_id_1", False)
    assert _JobManager._get(job_2.id) == job_2
    assert job_2.is_submitted()
    assert task.config_id in job_2.id
    assert job_2.task.id == task.id
    assert job_2.submit_id == "submit_id_1"
    assert not job_2.force


def test_get_job():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)

    task = _create_task(multiply, name="get_job")

    _SchedulerFactory._build_dispatcher()

    job_1 = _SchedulerFactory._scheduler.submit_task(task, "submit_id_1")
    assert _JobManager._get(job_1.id) == job_1

    job_2 = _SchedulerFactory._scheduler.submit_task(task, "submit_id_2")
    assert job_1 != job_2
    assert _JobManager._get(job_1.id).id == job_1.id
    assert _JobManager._get(job_2.id).id == job_2.id


def test_get_latest_job():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)

    task = _create_task(multiply, name="get_latest_job")
    task_2 = _create_task(multiply, name="get_latest_job_2")

    _SchedulerFactory._build_dispatcher()

    job_1 = _SchedulerFactory._scheduler.submit_task(task, "submit_id_1")
    assert _JobManager._get_latest(task) == job_1
    assert _JobManager._get_latest(task_2) is None

    sleep(0.01)  # Comparison is based on time, precision on Windows is not enough important
    job_2 = _SchedulerFactory._scheduler.submit_task(task_2, "submit_id_2")
    assert _JobManager._get_latest(task).id == job_1.id
    assert _JobManager._get_latest(task_2).id == job_2.id

    sleep(0.01)  # Comparison is based on time, precision on Windows is not enough important
    job_1_bis = _SchedulerFactory._scheduler.submit_task(task, "submit_id_1_bis")
    assert _JobManager._get_latest(task).id == job_1_bis.id
    assert _JobManager._get_latest(task_2).id == job_2.id


def test_get_job_unknown():
    assert _JobManager._get(JobId("Unknown")) is None


def test_get_jobs():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)

    task = _create_task(multiply, name="get_all_jobs")

    _SchedulerFactory._build_dispatcher()

    job_1 = _SchedulerFactory._scheduler.submit_task(task, "submit_id_1")
    job_2 = _SchedulerFactory._scheduler.submit_task(task, "submit_id_2")

    assert {job.id for job in _JobManager._get_all()} == {job_1.id, job_2.id}


def test_delete_job():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)

    task = _create_task(multiply, name="delete_job")

    _SchedulerFactory._build_dispatcher()

    job_1 = _SchedulerFactory._scheduler.submit_task(task, "submit_id_1")
    job_2 = _SchedulerFactory._scheduler.submit_task(task, "submit_id_2")

    _JobManager._delete(job_1)

    assert [job.id for job in _JobManager._get_all()] == [job_2.id]
    assert _JobManager._get(job_1.id) is None


m = multiprocessing.Manager()
lock = m.Lock()


def inner_lock_multiply(nb1: float, nb2: float):
    with lock:
        return multiply(1 or nb1, 2 or nb2)


def test_raise_when_trying_to_delete_unfinished_job():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)
    m = multiprocessing.Manager()
    lock = m.Lock()
    dn_1 = InMemoryDataNode("dn_config_1", Scope.SCENARIO, properties={"default_data": 1})
    dn_2 = InMemoryDataNode("dn_config_2", Scope.SCENARIO, properties={"default_data": 2})
    dn_3 = InMemoryDataNode("dn_config_3", Scope.SCENARIO)
    task = Task(
        "task_config_1", {}, partial(lock_multiply, lock), [dn_1, dn_2], [dn_3], id="raise_when_delete_unfinished"
    )
    _SchedulerFactory._build_dispatcher()
    with lock:
        job = _SchedulerFactory._scheduler.submit_task(task, "submit_id")

        assert_true_after_time(lambda: len(_JobDispatcher._dispatched_processes) == 1)
        assert_true_after_time(job.is_running)
        with pytest.raises(JobNotDeletedException):
            _JobManager._delete(job)
        with pytest.raises(JobNotDeletedException):
            _JobManager._delete(job, force=False)
    assert_true_after_time(job.is_completed)
    _JobManager._delete(job)


def test_force_deleting_unfinished_job():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)
    m = multiprocessing.Manager()
    lock = m.Lock()
    dn_1 = InMemoryDataNode("dn_config_1", Scope.SCENARIO, properties={"default_data": 1})
    dn_2 = InMemoryDataNode("dn_config_2", Scope.SCENARIO, properties={"default_data": 2})
    dn_3 = InMemoryDataNode("dn_config_3", Scope.SCENARIO)
    task = Task(
        "task_config_1", {}, partial(lock_multiply, lock), [dn_1, dn_2], [dn_3], id="force_deleting_unfinished_job"
    )
    _SchedulerFactory._build_dispatcher()
    with lock:
        job = _SchedulerFactory._scheduler.submit_task(task, "submit_id")
        assert_true_after_time(job.is_running)
        with pytest.raises(JobNotDeletedException):
            _JobManager._delete(job, force=False)
        _JobManager._delete(job, force=True)
    assert _JobManager._get(job.id) is None


def test_cancel_single_job():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=1)

    task = _create_task(multiply, name="cancel_single_job")

    _SchedulerFactory._build_dispatcher()

    assert_true_after_time(_SchedulerFactory._dispatcher.is_running)
    _SchedulerFactory._dispatcher.stop()
    assert_true_after_time(lambda: not _SchedulerFactory._dispatcher.is_running())

    job = _SchedulerFactory._scheduler.submit_task(task, "submit_id")

    assert_true_after_time(job.is_pending)
    assert_true_after_time(lambda: len(_JobDispatcher._dispatched_processes) == 0)
    _JobManager._cancel(job.id)
    assert_true_after_time(job.is_canceled)
    assert_true_after_time(job.is_canceled)


@mock.patch(
    "src.taipy.core._scheduler._scheduler._Scheduler._schedule_job_to_run_or_block", return_value="schedule_job"
)
@mock.patch("src.taipy.core._scheduler._scheduler._Scheduler._cancel_jobs")
def test_cancel_canceled_abandoned_failed_jobs(cancel_jobs, schedule_job):
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=1)

    task = _create_task(multiply, name="test_cancel_canceled_abandoned_failed_jobs")

    _SchedulerFactory._build_dispatcher()

    assert_true_after_time(_SchedulerFactory._dispatcher.is_running)
    _SchedulerFactory._dispatcher.stop()
    assert_true_after_time(lambda: not _SchedulerFactory._dispatcher.is_running())

    job = _SchedulerFactory._scheduler.submit_task(task, "submit_id")
    job.canceled()
    assert job.is_canceled()
    _JobManager._cancel(job)
    cancel_jobs.assert_not_called()
    assert job.is_canceled()

    job = _SchedulerFactory._scheduler.submit_task(task, "submit_id")
    job.failed()
    assert job.is_failed()
    _JobManager._cancel(job)
    cancel_jobs.assert_not_called()
    assert job.is_failed()

    job = _SchedulerFactory._scheduler.submit_task(task, "submit_id")
    job.abandoned()
    assert job.is_abandoned()
    _JobManager._cancel(job)
    cancel_jobs.assert_not_called()
    assert job.is_abandoned()


@mock.patch(
    "src.taipy.core._scheduler._scheduler._Scheduler._schedule_job_to_run_or_block", return_value="schedule_job"
)
@mock.patch("src.taipy.core.job.job.Job.canceled")
def test_cancel_completed_skipped_jobs(cancel_jobs, schedule_job):
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=1)
    task = _create_task(multiply, name="cancel_single_job")

    _SchedulerFactory._build_dispatcher()

    assert_true_after_time(_SchedulerFactory._dispatcher.is_running)
    _SchedulerFactory._dispatcher.stop()
    assert_true_after_time(lambda: not _SchedulerFactory._dispatcher.is_running())

    job = _SchedulerFactory._scheduler.submit_task(task, "submit_id")
    job.completed()
    assert job.is_completed()
    cancel_jobs.assert_not_called()
    _JobManager._cancel(job)
    assert job.is_completed()

    job = _SchedulerFactory._scheduler.submit_task(task, "submit_id")
    job.failed()
    assert job.is_failed()
    cancel_jobs.assert_not_called()
    _JobManager._cancel(job)
    assert job.is_failed()

    job = _SchedulerFactory._scheduler.submit_task(task, "submit_id")
    job.skipped()
    assert job.is_skipped()
    cancel_jobs.assert_not_called()
    _JobManager._cancel(job)
    assert job.is_skipped()


def test_cancel_single_running_job():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)

    m = multiprocessing.Manager()
    lock = m.Lock()
    dn_1 = InMemoryDataNode("dn_config_1", Scope.SCENARIO, properties={"default_data": 1})
    dn_2 = InMemoryDataNode("dn_config_2", Scope.SCENARIO, properties={"default_data": 2})
    dn_3 = InMemoryDataNode("dn_config_3", Scope.SCENARIO)
    task = Task("task_config_1", {}, partial(lock_multiply, lock), [dn_1, dn_2], [dn_3], id="cancel_single_job")

    _SchedulerFactory._build_dispatcher()

    assert_true_after_time(_SchedulerFactory._dispatcher.is_running)
    assert_true_after_time(lambda: _SchedulerFactory._dispatcher._nb_available_workers == 2)

    with lock:
        job = _SchedulerFactory._scheduler.submit_task(task, "submit_id")

        assert_true_after_time(lambda: len(_JobDispatcher._dispatched_processes) == 1)
        assert_true_after_time(lambda: _SchedulerFactory._dispatcher._nb_available_workers == 1)
        assert_true_after_time(job.is_running)
        _JobManager._cancel(job)
        assert_true_after_time(job.is_running)
    assert_true_after_time(lambda: len(_JobDispatcher._dispatched_processes) == 0)
    assert_true_after_time(lambda: _SchedulerFactory._dispatcher._nb_available_workers == 2)
    assert_true_after_time(job.is_completed)


def test_cancel_subsequent_jobs():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=1)
    _SchedulerFactory._build_dispatcher()

    lock_0 = m.Lock()

    dn_1 = InMemoryDataNode("dn_config_1", Scope.PIPELINE, properties={"default_data": 1})
    dn_2 = InMemoryDataNode("dn_config_2", Scope.PIPELINE, properties={"default_data": 2})
    dn_3 = InMemoryDataNode("dn_config_3", Scope.PIPELINE, properties={"default_data": 3})
    dn_4 = InMemoryDataNode("dn_config_4", Scope.PIPELINE, properties={"default_data": 4})
    task_1 = Task("task_config_1", {}, partial(lock_multiply, lock_0), [dn_1, dn_2], [dn_3], id="task_1")
    task_2 = Task("task_config_2", {}, multiply, [dn_1, dn_3], [dn_4], id="task_2")
    task_3 = Task("task_config_3", {}, print, [dn_4], id="task_3")

    _DataManager._set(dn_1)
    _DataManager._set(dn_2)
    _DataManager._set(dn_3)
    _DataManager._set(dn_4)

    with lock_0:
        submit_id_1 = "submit_1"
        job_1 = _SchedulerFactory._scheduler.submit_task(task_1, submit_id=submit_id_1)
        job_2 = _SchedulerFactory._scheduler.submit_task(task_2, submit_id=submit_id_1)
        job_3 = _SchedulerFactory._scheduler.submit_task(task_3, submit_id=submit_id_1)

        assert_true_after_time(lambda: _SchedulerFactory._scheduler.jobs_to_run.qsize() == 0)
        assert_true_after_time(lambda: len(_SchedulerFactory._scheduler.blocked_jobs) == 2)
        assert_true_after_time(job_1.is_running)
        assert_true_after_time(job_2.is_blocked)
        assert_true_after_time(job_3.is_blocked)

        submit_id_2 = "submit_2"
        job_4 = _SchedulerFactory._scheduler.submit_task(task_1, submit_id=submit_id_2)
        job_5 = _SchedulerFactory._scheduler.submit_task(task_2, submit_id=submit_id_2)
        job_6 = _SchedulerFactory._scheduler.submit_task(task_3, submit_id=submit_id_2)

        assert_true_after_time(job_4.is_pending)
        assert_true_after_time(job_5.is_blocked)
        assert_true_after_time(job_6.is_blocked)
        assert _SchedulerFactory._scheduler.jobs_to_run.qsize() == 1
        assert len(_SchedulerFactory._scheduler.blocked_jobs) == 4

        _JobManager._cancel(job_4)
        assert_true_after_time(job_4.is_canceled)
        assert_true_after_time(job_5.is_abandoned)
        assert_true_after_time(job_6.is_abandoned)
        assert _SchedulerFactory._scheduler.jobs_to_run.qsize() == 0
        assert len(_SchedulerFactory._scheduler.blocked_jobs) == 2

        _JobManager._cancel(job_1)
        assert_true_after_time(job_1.is_running)
        assert_true_after_time(job_2.is_abandoned)
        assert_true_after_time(job_3.is_abandoned)

    assert_true_after_time(job_1.is_completed)
    assert_true_after_time(job_2.is_abandoned)
    assert_true_after_time(job_3.is_abandoned)
    assert_true_after_time(job_4.is_canceled)
    assert_true_after_time(job_5.is_abandoned)
    assert_true_after_time(job_6.is_abandoned)
    assert_true_after_time(
        lambda: all(
            not _SchedulerFactory._scheduler._is_blocked(job) for job in [job_1, job_2, job_3, job_4, job_5, job_6]
        )
    )
    assert_true_after_time(lambda: _SchedulerFactory._scheduler.jobs_to_run.qsize() == 0)


def _create_task(function, nb_outputs=1, name=None):
    input1_dn_config = Config.configure_data_node("input1", "pickle", Scope.PIPELINE, default_data=21)
    input2_dn_config = Config.configure_data_node("input2", "pickle", Scope.PIPELINE, default_data=2)
    output_dn_configs = [
        Config.configure_data_node(f"output{i}", "pickle", Scope.PIPELINE, default_data=0) for i in range(nb_outputs)
    ]
    _DataManager._bulk_get_or_create({cfg for cfg in output_dn_configs})
    name = name or "".join(random.choice(string.ascii_lowercase) for _ in range(10))
    task_config = Config.configure_task(
        id=name,
        function=function,
        input=[input1_dn_config, input2_dn_config],
        output=output_dn_configs,
    )
    return _TaskManager._bulk_get_or_create([task_config])[0]
