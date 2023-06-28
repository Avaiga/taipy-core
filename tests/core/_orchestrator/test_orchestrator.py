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
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from functools import partial
from time import sleep

import pytest

from src.taipy.core import taipy
from src.taipy.core._orchestrator._orchestrator import _Orchestrator
from src.taipy.core._orchestrator._orchestrator_factory import _OrchestratorFactory
from src.taipy.core.config.job_config import JobConfig
from src.taipy.core.data._data_manager import _DataManager
from src.taipy.core.data.in_memory import InMemoryDataNode
from src.taipy.core.pipeline._pipeline_manager import _PipelineManager
from src.taipy.core.pipeline.pipeline import Pipeline
from src.taipy.core.scenario._scenario_manager import _ScenarioManager
from src.taipy.core.scenario.scenario import Scenario
from src.taipy.core.task._task_manager import _TaskManager
from src.taipy.core.task.task import Task
from taipy.config import Config
from taipy.config.common.scope import Scope
from taipy.config.exceptions.exceptions import ConfigurationUpdateBlocked
from tests.core.utils import assert_true_after_time

# ################################  USER FUNCTIONS  ##################################


def multiply(nb1: float, nb2: float):
    sleep(0.1)
    return nb1 * nb2


def lock_multiply(lock, nb1: float, nb2: float):
    with lock:
        return multiply(nb1, nb2)


def mult_by_2(n):
    return n * 2


def nothing():
    return True


def concat(a, b):
    return a + b


def _error():
    raise Exception


# ################################  TEST METHODS    ##################################


def test_submit_task():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)

    before_creation = datetime.now()
    sleep(0.1)
    task = _create_task(multiply)
    output_dn_id = task.output[f"{task.config_id}_output0"].id

    _OrchestratorFactory._build_dispatcher()

    assert _DataManager._get(output_dn_id).last_edition_date > before_creation
    assert _DataManager._get(output_dn_id).job_ids == []
    assert _DataManager._get(output_dn_id).is_ready_for_reading

    before_submission_creation = datetime.now()
    sleep(0.1)
    job = _Orchestrator.submit_task(task, "submit_id")
    sleep(0.1)
    after_submission_creation = datetime.now()
    assert _DataManager._get(output_dn_id).read() == 42
    assert _DataManager._get(output_dn_id).last_edition_date > before_submission_creation
    assert _DataManager._get(output_dn_id).last_edition_date < after_submission_creation
    assert _DataManager._get(output_dn_id).job_ids == [job.id]
    assert _DataManager._get(output_dn_id).is_ready_for_reading
    assert job.is_completed()


def test_submit_pipeline_generate_unique_submit_id(pipeline, task):
    dn_1 = InMemoryDataNode("dn_config_id_1", Scope.SCENARIO)
    dn_2 = InMemoryDataNode("dn_config_id_2", Scope.SCENARIO)
    task_1 = Task("task_config_id_1", {}, print, [dn_1])
    task_2 = Task("task_config_id_2", {}, print, [dn_2])
    pipeline.tasks = [task_1, task_2]

    _DataManager._set(dn_1)
    _DataManager._set(dn_2)
    _TaskManager._set(task_1)
    _TaskManager._set(task_2)
    _PipelineManager._set(pipeline)

    jobs_1 = taipy.submit(pipeline)
    jobs_2 = taipy.submit(pipeline)
    assert len(jobs_1) == 2
    assert len(jobs_2) == 2
    submit_ids_1 = [job.submit_id for job in jobs_1]
    submit_ids_2 = [job.submit_id for job in jobs_2]
    assert len(set(submit_ids_1)) == 1
    assert len(set(submit_ids_2)) == 1
    assert set(submit_ids_1) != set(submit_ids_2)


def test_submit_scenario_generate_unique_submit_id():
    dn_1 = InMemoryDataNode("dn_config_id_1", Scope.SCENARIO)
    dn_2 = InMemoryDataNode("dn_config_id_2", Scope.SCENARIO)
    dn_3 = InMemoryDataNode("dn_config_id_3", Scope.SCENARIO)
    task_1 = Task("task_config_id_1", {}, print, [dn_1])
    task_2 = Task("task_config_id_2", {}, print, [dn_2])
    task_3 = Task("task_config_id_3", {}, print, [dn_3])
    pipeline_1 = Pipeline("pipeline_config_id_1", {}, [task_1, task_2])
    pipeline_2 = Pipeline("pipeline_config_id_2", {}, [task_3])
    scenario = Scenario("scenario_config_id", [pipeline_1, pipeline_2], {})

    _DataManager._set(dn_1)
    _DataManager._set(dn_2)
    _TaskManager._set(task_1)
    _TaskManager._set(task_2)
    _TaskManager._set(task_3)
    _PipelineManager._set(pipeline_1)
    _PipelineManager._set(pipeline_2)
    _ScenarioManager._set(scenario)

    jobs_1 = taipy.submit(scenario)
    jobs_2 = taipy.submit(scenario)

    assert len(jobs_1) == 3
    assert len(jobs_2) == 3


def test_submit_task_that_return_multiple_outputs():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)

    def return_2tuple(nb1, nb2):
        return multiply(nb1, nb2), multiply(nb1, nb2) / 2

    def return_list(nb1, nb2):
        return [multiply(nb1, nb2), multiply(nb1, nb2) / 2]

    with_tuple = _create_task(return_2tuple, 2)
    with_list = _create_task(return_list, 2)

    _OrchestratorFactory._build_dispatcher()

    _Orchestrator.submit_task(with_tuple, "submit_id_1")
    _Orchestrator.submit_task(with_list, "submit_id_2")

    assert (
        with_tuple.output[f"{with_tuple.config_id}_output0"].read()
        == with_list.output[f"{with_list.config_id}_output0"].read()
        == 42
    )
    assert (
        with_tuple.output[f"{with_tuple.config_id}_output1"].read()
        == with_list.output[f"{with_list.config_id}_output1"].read()
        == 21
    )


def test_submit_task_returns_single_iterable_output():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)

    def return_2tuple(nb1, nb2):
        return multiply(nb1, nb2), multiply(nb1, nb2) / 2

    def return_list(nb1, nb2):
        return [multiply(nb1, nb2), multiply(nb1, nb2) / 2]

    task_with_tuple = _create_task(return_2tuple, 1)
    task_with_list = _create_task(return_list, 1)

    _OrchestratorFactory._build_dispatcher()

    _Orchestrator.submit_task(task_with_tuple, "submit_id_1")
    assert task_with_tuple.output[f"{task_with_tuple.config_id}_output0"].read() == (42, 21)
    assert len(_OrchestratorFactory._dispatcher._dispatched_processes) == 0
    _Orchestrator.submit_task(task_with_list, "submit_id_2")
    assert task_with_list.output[f"{task_with_list.config_id}_output0"].read() == [42, 21]
    assert len(_OrchestratorFactory._dispatcher._dispatched_processes) == 0


def test_data_node_not_written_due_to_wrong_result_nb():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)

    def return_2tuple():
        return lambda nb1, nb2: (multiply(nb1, nb2), multiply(nb1, nb2) / 2)

    task = _create_task(return_2tuple(), 3)

    _OrchestratorFactory._build_dispatcher()

    job = _Orchestrator.submit_task(task, "submit_id")
    assert task.output[f"{task.config_id}_output0"].read() == 0
    assert job.is_failed()
    assert len(_OrchestratorFactory._dispatcher._dispatched_processes) == 0


def test_scenario_only_submit_same_task_once():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)
    _OrchestratorFactory._build_dispatcher()

    dn_0 = InMemoryDataNode("dn_config_0", Scope.SCENARIO, properties={"default_data": 0})
    dn_1 = InMemoryDataNode("dn_config_1", Scope.SCENARIO, properties={"default_data": 1})
    dn_2 = InMemoryDataNode("dn_config_2", Scope.SCENARIO, properties={"default_data": 2})
    task_1 = Task("task_config_1", {}, print, input=[dn_0], output=[dn_1], id="task_1")
    task_2 = Task("task_config_2", {}, print, input=[dn_1], id="task_2")
    task_3 = Task("task_config_3", {}, print, input=[dn_2], id="task_3")
    pipeline_1 = Pipeline("pipeline_config_1", {}, [task_1, task_2], pipeline_id="pipeline_1")
    pipeline_2 = Pipeline("pipeline_config_2", {}, [task_1, task_3], pipeline_id="pipeline_2")
    scenario_1 = Scenario("scenario_config_1", [pipeline_1, pipeline_2], {}, "scenario_1")

    jobs = _Orchestrator.submit(scenario_1)
    assert len(jobs) == 3
    assert all([job.is_completed() for job in jobs])
    assert all(not _Orchestrator._is_blocked(job) for job in jobs)

    jobs = _Orchestrator.submit(pipeline_1)
    assert len(jobs) == 2
    assert all([job.is_completed() for job in jobs])
    assert all(not _Orchestrator._is_blocked(job) for job in jobs)

    jobs = _Orchestrator.submit(pipeline_2)
    assert len(jobs) == 2
    assert all([job.is_completed() for job in jobs])
    assert all(not _Orchestrator._is_blocked(job) for job in jobs)


def test_update_status_fail_job():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)
    _OrchestratorFactory._build_dispatcher()

    dn_0 = InMemoryDataNode("dn_config_0", Scope.SCENARIO, properties={"default_data": 0})
    dn_1 = InMemoryDataNode("dn_config_1", Scope.SCENARIO, properties={"default_data": 1})
    dn_2 = InMemoryDataNode("dn_config_2", Scope.SCENARIO, properties={"default_data": 2})
    task_0 = Task("task_config_0", {}, _error, output=[dn_0], id="task_0")
    task_1 = Task("task_config_1", {}, print, input=[dn_0], output=[dn_1], id="task_1")
    task_2 = Task("task_config_2", {}, print, input=[dn_1], id="task_2")
    task_3 = Task("task_config_3", {}, print, input=[dn_2], id="task_3")
    pipeline_1 = Pipeline("pipeline_config_1", {}, [task_0, task_1, task_2, task_3], pipeline_id="pipeline_1")
    pipeline_2 = Pipeline("pipeline_config_2", {}, [task_0, task_1, task_2], pipeline_id="pipeline_2")
    pipeline_3 = Pipeline("pipeline_config_3", {}, [task_3], pipeline_id="pipeline_3")
    scenario_1 = Scenario("scenario_config_1", [pipeline_1], {}, "scenario_1")
    scenario_2 = Scenario("scenario_config_2", [pipeline_2, pipeline_3], {}, "scenario_2")

    _DataManager._set(dn_0)
    _DataManager._set(dn_1)
    _DataManager._set(dn_2)
    _TaskManager._set(task_0)
    _TaskManager._set(task_1)
    _TaskManager._set(task_2)
    _TaskManager._set(task_3)
    _PipelineManager._set(pipeline_1)
    _PipelineManager._set(pipeline_2)
    _PipelineManager._set(pipeline_3)
    _ScenarioManager._set(scenario_1)
    _ScenarioManager._set(scenario_2)

    job = _Orchestrator.submit_task(task_0, "submit_id")
    assert job.is_failed()

    jobs = _Orchestrator.submit(pipeline_1)
    tasks_jobs = {job._task.id: job for job in jobs}
    assert tasks_jobs["task_0"].is_failed()
    assert all([job.is_abandoned() for job in [tasks_jobs["task_1"], tasks_jobs["task_2"]]])
    assert tasks_jobs["task_3"].is_completed()
    assert all(not _Orchestrator._is_blocked(job) for job in jobs)

    jobs = _Orchestrator.submit(scenario_1)
    tasks_jobs = {job._task.id: job for job in jobs}
    assert tasks_jobs["task_0"].is_failed()
    assert all([job.is_abandoned() for job in [tasks_jobs["task_1"], tasks_jobs["task_2"]]])
    assert tasks_jobs["task_3"].is_completed()
    assert all(not _Orchestrator._is_blocked(job) for job in jobs)

    jobs = _Orchestrator.submit(scenario_2)
    tasks_jobs = {job._task.id: job for job in jobs}
    assert tasks_jobs["task_0"].is_failed()
    assert all([job.is_abandoned() for job in [tasks_jobs["task_1"], tasks_jobs["task_2"]]])
    assert tasks_jobs["task_3"].is_completed()
    assert all(not _Orchestrator._is_blocked(job) for job in jobs)


def test_update_status_fail_job_in_parallel():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)
    _OrchestratorFactory._build_dispatcher()

    dn_0 = InMemoryDataNode("dn_config_0", Scope.SCENARIO, properties={"default_data": 0})
    dn_1 = InMemoryDataNode("dn_config_1", Scope.SCENARIO, properties={"default_data": 1})
    dn_2 = InMemoryDataNode("dn_config_2", Scope.SCENARIO, properties={"default_data": 2})
    task_0 = Task("task_config_0", {}, _error, output=[dn_0], id="task_0")
    task_1 = Task("task_config_1", {}, print, input=[dn_0], output=[dn_1], id="task_1")
    task_2 = Task("task_config_2", {}, print, input=[dn_1], id="task_2")
    task_3 = Task("task_config_3", {}, print, input=[dn_2], id="task_3")
    pipeline_1 = Pipeline("pipeline_config_1", {}, [task_0, task_1, task_2, task_3], pipeline_id="pipeline_1")
    pipeline_2 = Pipeline("pipeline_config_2", {}, [task_0, task_1, task_2], pipeline_id="pipeline_2")
    pipeline_3 = Pipeline("pipeline_config_3", {}, [task_3], pipeline_id="pipeline_3")
    scenario_1 = Scenario("scenario_config_1", [pipeline_1], {}, "scenario_1")
    scenario_2 = Scenario("scenario_config_2", [pipeline_2, pipeline_3], {}, "scenario_2")

    _DataManager._set(dn_0)
    _DataManager._set(dn_1)
    _DataManager._set(dn_2)
    _TaskManager._set(task_0)
    _TaskManager._set(task_1)
    _TaskManager._set(task_2)
    _TaskManager._set(task_3)
    _PipelineManager._set(pipeline_1)
    _PipelineManager._set(pipeline_2)
    _PipelineManager._set(pipeline_3)
    _ScenarioManager._set(scenario_1)
    _ScenarioManager._set(scenario_2)

    job = _Orchestrator.submit_task(task_0, "submit_id")
    assert_true_after_time(job.is_failed)

    jobs = _Orchestrator.submit(pipeline_1)
    tasks_jobs = {job._task.id: job for job in jobs}
    assert_true_after_time(tasks_jobs["task_0"].is_failed)
    assert_true_after_time(tasks_jobs["task_3"].is_completed)
    assert_true_after_time(lambda: all([job.is_abandoned() for job in [tasks_jobs["task_1"], tasks_jobs["task_2"]]]))
    assert_true_after_time(lambda: all(not _Orchestrator._is_blocked(job) for job in jobs))

    jobs = _Orchestrator.submit(scenario_1)
    tasks_jobs = {job._task.id: job for job in jobs}
    assert_true_after_time(tasks_jobs["task_0"].is_failed)
    assert_true_after_time(tasks_jobs["task_3"].is_completed)
    assert_true_after_time(lambda: all([job.is_abandoned() for job in [tasks_jobs["task_1"], tasks_jobs["task_2"]]]))
    assert_true_after_time(lambda: all(not _Orchestrator._is_blocked(job) for job in jobs))

    jobs = _Orchestrator.submit(scenario_2)
    tasks_jobs = {job._task.id: job for job in jobs}
    assert_true_after_time(tasks_jobs["task_0"].is_failed)
    assert_true_after_time(tasks_jobs["task_3"].is_completed)
    assert_true_after_time(lambda: all([job.is_abandoned() for job in [tasks_jobs["task_1"], tasks_jobs["task_2"]]]))
    assert_true_after_time(lambda: all(not _Orchestrator._is_blocked(job) for job in jobs))


def test_submit_task_in_parallel():
    m = multiprocessing.Manager()
    lock = m.Lock()

    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)

    task = _create_task(partial(lock_multiply, lock))

    _OrchestratorFactory._build_dispatcher()

    with lock:
        assert task.output[f"{task.config_id}_output0"].read() == 0
        job = _Orchestrator.submit_task(task, "submit_id")
        assert_true_after_time(job.is_running)
        assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 1)

    assert_true_after_time(lambda: task.output[f"{task.config_id}_output0"].read() == 42)
    assert_true_after_time(job.is_completed)
    assert len(_OrchestratorFactory._dispatcher._dispatched_processes) == 0


def test_submit_pipeline_in_parallel():
    m = multiprocessing.Manager()
    lock = m.Lock()

    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)

    task = _create_task(partial(lock_multiply, lock))
    pipeline = Pipeline("pipeline_config", {}, [task], "pipeline_id")

    _OrchestratorFactory._build_dispatcher()

    with lock:
        assert task.output[f"{task.config_id}_output0"].read() == 0
        job = _Orchestrator.submit(pipeline)[0]
        assert_true_after_time(job.is_running)
        assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 1)

    assert_true_after_time(lambda: task.output[f"{task.config_id}_output0"].read() == 42)
    assert_true_after_time(job.is_completed)
    assert len(_OrchestratorFactory._dispatcher._dispatched_processes) == 0


def test_submit_scenario_in_parallel():
    m = multiprocessing.Manager()
    lock = m.Lock()

    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)

    task = _create_task(partial(lock_multiply, lock))
    pipeline = Pipeline("pipeline_config", {}, [task], "pipeline_id")
    scenario = Scenario("scenario_config", [pipeline], {}, "scenario_id")

    _OrchestratorFactory._build_dispatcher()

    with lock:
        assert task.output[f"{task.config_id}_output0"].read() == 0
        job = _Orchestrator.submit(scenario)[0]
        assert_true_after_time(job.is_running)
        assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 1)

    assert_true_after_time(lambda: task.output[f"{task.config_id}_output0"].read() == 42)
    assert_true_after_time(job.is_completed)
    assert len(_OrchestratorFactory._dispatcher._dispatched_processes) == 0


def sleep_fct(seconds):
    sleep(seconds)


def sleep_and_raise_error_fct(seconds):
    sleep(seconds)
    raise Exception


def test_submit_task_synchronously_in_parallel():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)
    _OrchestratorFactory._build_dispatcher()

    sleep_period = 1
    start_time = datetime.now()
    task = Task("sleep_task", {}, function=partial(sleep, sleep_period))
    job = _Orchestrator.submit_task(task, "submit_id", wait=True)
    assert (datetime.now() - start_time).seconds >= sleep_period
    assert_true_after_time(job.is_completed)


def test_submit_pipeline_synchronously_in_parallel():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)
    _OrchestratorFactory._build_dispatcher()

    sleep_period = 1
    start_time = datetime.now()
    task = Task("sleep_task", {}, function=partial(sleep, sleep_period))
    pipeline = Pipeline("pipeline_config", {}, [task])

    job = _Orchestrator.submit(pipeline, wait=True)[0]
    assert (datetime.now() - start_time).seconds >= sleep_period
    assert_true_after_time(job.is_completed)


def test_submit_scenario_synchronously_in_parallel():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)
    _OrchestratorFactory._build_dispatcher()

    sleep_period = 1
    start_time = datetime.now()
    task = Task("sleep_task", {}, function=partial(sleep, sleep_period))
    pipeline = Pipeline("pipeline_config", {}, [task])
    scenario = Scenario("scenario_config", [pipeline], {})

    job = _Orchestrator.submit(scenario, wait=True)[0]
    assert (datetime.now() - start_time).seconds >= sleep_period
    assert_true_after_time(job.is_completed)


def test_submit_fail_task_synchronously_in_parallel():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)
    _OrchestratorFactory._build_dispatcher()

    sleep_period = 1.0
    start_time = datetime.now()
    task = Task("sleep_task", {}, function=partial(sleep_and_raise_error_fct, sleep_period))
    job = _Orchestrator.submit_task(task, "submit_id", wait=True)
    assert (datetime.now() - start_time).seconds >= sleep_period
    assert_true_after_time(job.is_failed)


def test_submit_fail_pipeline_synchronously_in_parallel():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)
    _OrchestratorFactory._build_dispatcher()

    sleep_period = 1.0
    start_time = datetime.now()
    task = Task("sleep_task", {}, function=partial(sleep_and_raise_error_fct, sleep_period))
    pipeline = Pipeline("pipeline_config", {}, [task], "pipeline_id")

    job = _Orchestrator.submit(pipeline, wait=True)[0]
    assert (datetime.now() - start_time).seconds >= sleep_period
    assert_true_after_time(job.is_failed)


def test_submit_fail_scenario_synchronously_in_parallel():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)
    _OrchestratorFactory._build_dispatcher()

    sleep_period = 1.0
    start_time = datetime.now()
    task = Task("sleep_task", {}, function=partial(sleep_and_raise_error_fct, sleep_period))
    pipeline = Pipeline("pipeline_config", {}, [task], "pipeline_id")
    scenario = Scenario("scenario_config", [pipeline], {})

    job = _Orchestrator.submit(scenario, wait=True)[0]
    assert (datetime.now() - start_time).seconds >= sleep_period
    assert_true_after_time(job.is_failed)


def test_submit_task_synchronously_in_parallel_with_timeout():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)
    _OrchestratorFactory._build_dispatcher()

    task_duration = 2
    timeout_duration = task_duration - 1
    task = Task("sleep_task", {}, function=partial(sleep, task_duration))

    start_time = datetime.now()
    job = _Orchestrator.submit_task(task, "submit_id", wait=True, timeout=timeout_duration)
    end_time = datetime.now()

    assert timeout_duration <= (end_time - start_time).seconds
    assert_true_after_time(job.is_completed)


def test_submit_task_multithreading_multiple_task():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)

    m = multiprocessing.Manager()
    lock_1 = m.Lock()
    lock_2 = m.Lock()

    task_1 = _create_task(partial(lock_multiply, lock_1))
    task_2 = _create_task(partial(lock_multiply, lock_2))

    _OrchestratorFactory._build_dispatcher()

    with lock_1:
        with lock_2:
            job_1 = _Orchestrator.submit_task(task_1, "submit_id_1")
            job_2 = _Orchestrator.submit_task(task_2, "submit_id_2")

            assert task_1.output[f"{task_1.config_id}_output0"].read() == 0
            assert task_2.output[f"{task_2.config_id}_output0"].read() == 0
            assert_true_after_time(job_1.is_running)
            assert_true_after_time(job_2.is_running)
            assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 2)

        assert_true_after_time(lambda: task_2.output[f"{task_2.config_id}_output0"].read() == 42)
        assert task_1.output[f"{task_1.config_id}_output0"].read() == 0
        assert_true_after_time(job_2.is_completed)
        assert_true_after_time(job_1.is_running)
        assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 1)

    assert_true_after_time(lambda: task_1.output[f"{task_1.config_id}_output0"].read() == 42)
    assert_true_after_time(job_1.is_completed)
    assert_true_after_time(job_2.is_completed)
    assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 0)


def test_submit_pipeline_multithreading_multiple_task():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)

    m = multiprocessing.Manager()
    lock_1 = m.Lock()
    lock_2 = m.Lock()

    task_1 = _create_task(partial(lock_multiply, lock_1))
    task_2 = _create_task(partial(lock_multiply, lock_2))

    pipeline = Pipeline("pipeline_config", {}, [task_1, task_2])

    _OrchestratorFactory._build_dispatcher()

    with lock_1:
        with lock_2:
            tasks_jobs = {job._task.id: job for job in _Orchestrator.submit(pipeline)}
            job_1 = tasks_jobs[task_1.id]
            job_2 = tasks_jobs[task_2.id]

            assert task_1.output[f"{task_1.config_id}_output0"].read() == 0
            assert task_2.output[f"{task_2.config_id}_output0"].read() == 0
            assert_true_after_time(job_1.is_running)
            assert_true_after_time(job_2.is_running)
            assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 2)

        assert_true_after_time(lambda: task_2.output[f"{task_2.config_id}_output0"].read() == 42)
        assert task_1.output[f"{task_1.config_id}_output0"].read() == 0
        assert_true_after_time(job_2.is_completed)
        assert_true_after_time(job_1.is_running)
        assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 1)

    assert_true_after_time(lambda: task_1.output[f"{task_1.config_id}_output0"].read() == 42)
    assert_true_after_time(job_1.is_completed)
    assert_true_after_time(job_2.is_completed)
    assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 0)


def test_submit_scenario_multithreading_multiple_task():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)

    m = multiprocessing.Manager()
    lock_1 = m.Lock()
    lock_2 = m.Lock()

    task_1 = _create_task(partial(lock_multiply, lock_1))
    task_2 = _create_task(partial(lock_multiply, lock_2))

    pipeline = Pipeline("pipeline_config", {}, [task_1, task_2])
    scenario = Scenario("scenario_config", [pipeline], {})

    _OrchestratorFactory._build_dispatcher()

    with lock_1:
        with lock_2:
            tasks_jobs = {job._task.id: job for job in _Orchestrator.submit(scenario)}
            job_1 = tasks_jobs[task_1.id]
            job_2 = tasks_jobs[task_2.id]

            assert task_1.output[f"{task_1.config_id}_output0"].read() == 0
            assert task_2.output[f"{task_2.config_id}_output0"].read() == 0
            assert_true_after_time(job_1.is_running)
            assert_true_after_time(job_2.is_running)
            assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 2)

        assert_true_after_time(lambda: task_2.output[f"{task_2.config_id}_output0"].read() == 42)
        assert task_1.output[f"{task_1.config_id}_output0"].read() == 0
        assert_true_after_time(job_2.is_completed)
        assert_true_after_time(job_1.is_running)
        assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 1)

    assert_true_after_time(lambda: task_1.output[f"{task_1.config_id}_output0"].read() == 42)
    assert_true_after_time(job_1.is_completed)
    assert_true_after_time(job_2.is_completed)
    assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 0)


def test_submit_task_multithreading_multiple_task_in_sync_way_to_check_job_status():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)

    m = multiprocessing.Manager()
    lock_0 = m.Lock()
    lock_1 = m.Lock()
    lock_2 = m.Lock()

    task_0 = _create_task(partial(lock_multiply, lock_0))
    task_1 = _create_task(partial(lock_multiply, lock_1))
    task_2 = _create_task(partial(lock_multiply, lock_2))

    _OrchestratorFactory._build_dispatcher()

    with lock_0:
        job_0 = _Orchestrator.submit_task(task_0, "submit_id_0")
        assert_true_after_time(job_0.is_running)
        assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 1)
        with lock_1:
            with lock_2:
                assert task_1.output[f"{task_1.config_id}_output0"].read() == 0
                assert task_2.output[f"{task_2.config_id}_output0"].read() == 0
                job_2 = _Orchestrator.submit_task(task_2, "submit_id_2")
                job_1 = _Orchestrator.submit_task(task_1, "submit_id_1")
                assert_true_after_time(job_0.is_running)
                assert_true_after_time(job_1.is_pending)
                assert_true_after_time(job_2.is_running)
                assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 2)

            assert_true_after_time(lambda: task_2.output[f"{task_2.config_id}_output0"].read() == 42)
            assert task_1.output[f"{task_1.config_id}_output0"].read() == 0
            assert_true_after_time(job_0.is_running)
            assert_true_after_time(job_1.is_running)
            assert_true_after_time(job_2.is_completed)
            assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 2)

        assert_true_after_time(lambda: task_1.output[f"{task_1.config_id}_output0"].read() == 42)
        assert task_0.output[f"{task_0.config_id}_output0"].read() == 0
        assert_true_after_time(job_0.is_running)
        assert_true_after_time(job_1.is_completed)
        assert job_2.is_completed()
        assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 1)

    assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 0)
    assert task_0.output[f"{task_0.config_id}_output0"].read() == 42
    assert job_0.is_completed()
    assert job_1.is_completed()
    assert job_2.is_completed()


def test_blocked_task():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)

    m = multiprocessing.Manager()
    lock_1 = m.Lock()
    lock_2 = m.Lock()

    foo_cfg = Config.configure_data_node("foo", default_data=1)
    bar_cfg = Config.configure_data_node("bar")
    baz_cfg = Config.configure_data_node("baz")

    _OrchestratorFactory._build_dispatcher()

    dns = _DataManager._bulk_get_or_create([foo_cfg, bar_cfg, baz_cfg])
    foo = dns[foo_cfg]
    bar = dns[bar_cfg]
    baz = dns[baz_cfg]
    task_1 = Task("by_2", {}, partial(lock_multiply, lock_1, 2), [foo], [bar])
    task_2 = Task("by_3", {}, partial(lock_multiply, lock_2, 3), [bar], [baz])

    assert task_1.foo.is_ready_for_reading  # foo is ready
    assert not task_1.bar.is_ready_for_reading  # But bar is not ready
    assert not task_2.baz.is_ready_for_reading  # neither does baz

    assert len(_Orchestrator.blocked_jobs) == 0
    job_2 = _Orchestrator.submit_task(task_2, "submit_id_2")  # job 2 is submitted first
    assert job_2.is_blocked()  # since bar is not up_to_date the job 2 is blocked
    assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 0)
    assert len(_Orchestrator.blocked_jobs) == 1
    with lock_2:
        with lock_1:
            job_1 = _Orchestrator.submit_task(task_1, "submit_id_1")  # job 1 is submitted and locked
            assert_true_after_time(job_1.is_running)  # so it is still running
            assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 1)
            assert not _DataManager._get(task_1.bar.id).is_ready_for_reading  # And bar still not ready
            assert_true_after_time(job_2.is_blocked)  # the job_2 remains blocked
        assert_true_after_time(job_1.is_completed)  # job1 unlocked and can complete
        assert _DataManager._get(task_1.bar.id).is_ready_for_reading  # bar becomes ready
        assert _DataManager._get(task_1.bar.id).read() == 2  # the data is computed and written
        assert_true_after_time(job_2.is_running)  # And job 2 can start running
        assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 1)
        assert len(_Orchestrator.blocked_jobs) == 0
    assert_true_after_time(job_2.is_completed)  # job 2 unlocked so it can complete
    assert _DataManager._get(task_2.baz.id).is_ready_for_reading  # baz becomes ready
    assert _DataManager._get(task_2.baz.id).read() == 6  # the data is computed and written
    assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 0)


def test_blocked_pipeline():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)

    m = multiprocessing.Manager()
    lock_1 = m.Lock()
    lock_2 = m.Lock()

    foo_cfg = Config.configure_data_node("foo", default_data=1)
    bar_cfg = Config.configure_data_node("bar")
    baz_cfg = Config.configure_data_node("baz")

    _OrchestratorFactory._build_dispatcher()

    dns = _DataManager._bulk_get_or_create([foo_cfg, bar_cfg, baz_cfg])
    foo = dns[foo_cfg]
    bar = dns[bar_cfg]
    baz = dns[baz_cfg]
    task_1 = Task("by_2", {}, partial(lock_multiply, lock_1, 2), [foo], [bar])
    task_2 = Task("by_3", {}, partial(lock_multiply, lock_2, 3), [bar], [baz])
    pipeline = Pipeline("pipeline_config", {}, [task_1, task_2])

    assert task_1.foo.is_ready_for_reading  # foo is ready
    assert not task_1.bar.is_ready_for_reading  # But bar is not ready
    assert not task_2.baz.is_ready_for_reading  # neither does baz

    assert len(_Orchestrator.blocked_jobs) == 0
    with lock_2:
        with lock_1:
            jobs = _Orchestrator.submit(pipeline)  # pipeline is submitted
            tasks_jobs = {job._task.id: job for job in jobs}
            job_1, job_2 = tasks_jobs[task_1.id], tasks_jobs[task_2.id]
            assert_true_after_time(job_1.is_running)  # job 1 is submitted and locked so it is still running
            assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 1)
            assert not _DataManager._get(task_1.bar.id).is_ready_for_reading  # And bar still not ready
            assert_true_after_time(job_2.is_blocked)  # the job_2 remains blocked
        assert_true_after_time(job_1.is_completed)  # job1 unlocked and can complete
        assert _DataManager._get(task_1.bar.id).is_ready_for_reading  # bar becomes ready
        assert _DataManager._get(task_1.bar.id).read() == 2  # the data is computed and written
        assert_true_after_time(job_2.is_running)  # And job 2 can start running
        assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 1)
        assert len(_Orchestrator.blocked_jobs) == 0
    assert_true_after_time(job_2.is_completed)  # job 2 unlocked so it can complete
    assert _DataManager._get(task_2.baz.id).is_ready_for_reading  # baz becomes ready
    assert _DataManager._get(task_2.baz.id).read() == 6  # the data is computed and written
    assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 0)


def test_blocked_scenario():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)

    m = multiprocessing.Manager()
    lock_1 = m.Lock()
    lock_2 = m.Lock()

    foo_cfg = Config.configure_data_node("foo", default_data=1)
    bar_cfg = Config.configure_data_node("bar")
    baz_cfg = Config.configure_data_node("baz")

    _OrchestratorFactory._build_dispatcher()

    dns = _DataManager._bulk_get_or_create([foo_cfg, bar_cfg, baz_cfg])
    foo = dns[foo_cfg]
    bar = dns[bar_cfg]
    baz = dns[baz_cfg]
    task_1 = Task("by_2", {}, partial(lock_multiply, lock_1, 2), [foo], [bar])
    task_2 = Task("by_3", {}, partial(lock_multiply, lock_2, 3), [bar], [baz])
    pipeline = Pipeline("pipeline_config", {}, [task_1, task_2])
    scenario = Scenario("scenario_config", [pipeline], {})

    assert task_1.foo.is_ready_for_reading  # foo is ready
    assert not task_1.bar.is_ready_for_reading  # But bar is not ready
    assert not task_2.baz.is_ready_for_reading  # neither does baz

    assert len(_Orchestrator.blocked_jobs) == 0
    with lock_2:
        with lock_1:
            jobs = _Orchestrator.submit(scenario)  # scenario is submitted
            tasks_jobs = {job._task.id: job for job in jobs}
            job_1, job_2 = tasks_jobs[task_1.id], tasks_jobs[task_2.id]
            assert_true_after_time(job_1.is_running)  # job 1 is submitted and locked so it is still running
            assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 1)
            assert not _DataManager._get(task_1.bar.id).is_ready_for_reading  # And bar still not ready
            assert_true_after_time(job_2.is_blocked)  # the job_2 remains blocked
        assert_true_after_time(job_1.is_completed)  # job1 unlocked and can complete
        assert _DataManager._get(task_1.bar.id).is_ready_for_reading  # bar becomes ready
        assert _DataManager._get(task_1.bar.id).read() == 2  # the data is computed and written
        assert_true_after_time(job_2.is_running)  # And job 2 can start running
        assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 1)
        assert len(_Orchestrator.blocked_jobs) == 0
    assert_true_after_time(job_2.is_completed)  # job 2 unlocked so it can complete
    assert _DataManager._get(task_2.baz.id).is_ready_for_reading  # baz becomes ready
    assert _DataManager._get(task_2.baz.id).read() == 6  # the data is computed and written
    assert_true_after_time(lambda: len(_OrchestratorFactory._dispatcher._dispatched_processes) == 0)


def test_task_orchestrator_create_synchronous_dispatcher():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)
    _OrchestratorFactory._build_dispatcher()

    assert _OrchestratorFactory._dispatcher._nb_available_workers == 1


def test_task_orchestrator_create_standalone_dispatcher():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=3)
    _OrchestratorFactory._build_dispatcher()
    assert isinstance(_OrchestratorFactory._dispatcher._executor, ProcessPoolExecutor)
    assert _OrchestratorFactory._dispatcher._nb_available_workers == 3


def modified_config_task(n):
    from taipy.config import Config

    assert_true_after_time(lambda: Config.global_config.storage_folder == ".my_data/")
    assert_true_after_time(lambda: Config.global_config.custom_property == "custom_property")
    return n * 2


def test_can_exec_task_with_modified_config():
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)
    Config.configure_global_app(
        storage_folder=".my_data/", clean_entities_enabled=True, custom_property="custom_property"
    )

    dn_input_config = Config.configure_data_node("input", "pickle", scope=Scope.SCENARIO, default_data=1)
    dn_output_config = Config.configure_data_node("output", "pickle")
    task_config = Config.configure_task("task_config", modified_config_task, dn_input_config, dn_output_config)
    pipeline_config = Config.configure_pipeline("pipeline_config", [task_config])

    _OrchestratorFactory._build_dispatcher()

    pipeline = _PipelineManager._get_or_create(pipeline_config)

    jobs = pipeline.submit()
    assert_true_after_time(jobs[0].is_finished, time=120)
    assert_true_after_time(
        jobs[0].is_completed
    )  # If the job is completed, that means the asserts in the task are successful
    taipy.clean_all_entities()


def update_config_task(n):
    from taipy.config import Config

    # The exception will be saved to logger, and there is no way to check for it
    # so it will be checked here
    with pytest.raises(ConfigurationUpdateBlocked):
        Config.global_config.storage_folder = ".new_storage_folder/"
    with pytest.raises(ConfigurationUpdateBlocked):
        Config.global_config.properties = {"custom_property": "new_custom_property"}

    Config.global_config.storage_folder = ".new_storage_folder/"
    Config.global_config.properties = {"custom_property": "new_custom_property"}

    return n * 2


def test_cannot_exec_task_that_update_config():
    """
    _ConfigBlocker singleton is not passed to the subprocesses. That means in each subprocess,
    the config update will not be blocked.

    After rebuilding a new Config in each subprocess, the Config should be blocked.
    """
    Config.configure_job_executions(mode=JobConfig._STANDALONE_MODE, max_nb_of_workers=2)

    dn_input_config = Config.configure_data_node("input", "pickle", scope=Scope.SCENARIO, default_data=1)
    dn_output_config = Config.configure_data_node("output", "pickle")
    task_config = Config.configure_task("task_config", update_config_task, dn_input_config, dn_output_config)
    pipeline_config = Config.configure_pipeline("pipeline_config", [task_config])

    _OrchestratorFactory._build_dispatcher()

    pipeline = _PipelineManager._get_or_create(pipeline_config)

    jobs = pipeline.submit()

    # The job should fail due to an exception is raised
    assert_true_after_time(jobs[0].is_failed)


def test_can_execute_task_with_development_mode():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)

    dn_input_config = Config.configure_data_node("input", "pickle", scope=Scope.SCENARIO, default_data=1)
    dn_output_config = Config.configure_data_node("output", "pickle")
    task_config = Config.configure_task("task_config", mult_by_2, dn_input_config, dn_output_config)
    pipeline_config = Config.configure_pipeline("pipeline_config", [task_config])

    _OrchestratorFactory._build_dispatcher()

    pipeline = _PipelineManager._get_or_create(pipeline_config)
    pipeline.submit()
    while pipeline.output.edit_in_progress:
        sleep(1)
    assert 2 == pipeline.output.read()


def test_need_to_run_no_output():
    hello_cfg = Config.configure_data_node("hello", default_data="Hello ")
    world_cfg = Config.configure_data_node("world", default_data="world !")
    task_cfg = Config.configure_task("name", input=[hello_cfg, world_cfg], function=concat, output=[])
    task = _create_task_from_config(task_cfg)

    assert _OrchestratorFactory._dispatcher._needs_to_run(task)


def test_need_to_run_task_not_skippable():
    hello_cfg = Config.configure_data_node("hello", default_data="Hello ")
    world_cfg = Config.configure_data_node("world", default_data="world !")
    hello_world_cfg = Config.configure_data_node("hello_world")
    task_cfg = Config.configure_task(
        "name", input=[hello_cfg, world_cfg], function=concat, output=[hello_world_cfg], skippable=False
    )
    task = _create_task_from_config(task_cfg)

    assert _OrchestratorFactory._dispatcher._needs_to_run(task)


def test_need_to_run_skippable_task_no_input():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)

    hello_world_cfg = Config.configure_data_node("hello_world")
    task_cfg = Config.configure_task("name", input=[], function=nothing, output=[hello_world_cfg], skippable=True)

    _OrchestratorFactory._build_dispatcher()

    task = _create_task_from_config(task_cfg)
    assert _OrchestratorFactory._dispatcher._needs_to_run(task)
    _Orchestrator.submit_task(task, "submit_id")

    assert not _OrchestratorFactory._dispatcher._needs_to_run(task)


def test_need_to_run_skippable_task_no_validity_period_on_output():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)

    hello_cfg = Config.configure_data_node("hello", default_data="Hello ")
    world_cfg = Config.configure_data_node("world", default_data="world !")
    hello_world_cfg = Config.configure_data_node("hello_world")
    task_cfg = Config.configure_task(
        "name", input=[hello_cfg, world_cfg], function=concat, output=[hello_world_cfg], skippable=True
    )

    _OrchestratorFactory._build_dispatcher()

    task = _create_task_from_config(task_cfg)
    assert _OrchestratorFactory._dispatcher._needs_to_run(task)
    _Orchestrator.submit_task(task, "submit_id")

    assert not _OrchestratorFactory._dispatcher._needs_to_run(task)


def test_need_to_run_skippable_task_with_validity_period_up_to_date_on_output():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)

    hello_cfg = Config.configure_data_node("hello", default_data="Hello ")
    world_cfg = Config.configure_data_node("world", default_data="world !")
    hello_world_cfg = Config.configure_data_node("hello_world", validity_days=1)
    task_cfg = Config.configure_task(
        "name", input=[hello_cfg, world_cfg], function=concat, output=[hello_world_cfg], skippable=True
    )
    _OrchestratorFactory._build_dispatcher()

    task = _create_task_from_config(task_cfg)

    assert _OrchestratorFactory._dispatcher._needs_to_run(task)
    job = _Orchestrator.submit_task(task, "submit_id_1")

    assert not _OrchestratorFactory._dispatcher._needs_to_run(task)
    job_skipped = _Orchestrator.submit_task(task, "submit_id_2")

    assert job.is_completed()
    assert job.is_finished()
    assert job_skipped.is_skipped()
    assert job_skipped.is_finished()


def test_need_to_run_skippable_task_with_validity_period_obsolete_on_output():
    hello_cfg = Config.configure_data_node("hello", default_data="Hello ")
    world_cfg = Config.configure_data_node("world", default_data="world !")
    hello_world_cfg = Config.configure_data_node("hello_world", validity_days=1)
    task_cfg = Config.configure_task(
        "name", input=[hello_cfg, world_cfg], function=concat, output=[hello_world_cfg], skippable=True
    )
    task = _create_task_from_config(task_cfg)

    assert _OrchestratorFactory._dispatcher._needs_to_run(task)
    _Orchestrator.submit_task(task, "submit_id")

    output = task.hello_world
    output._last_edit_date = datetime.now() - timedelta(days=1, minutes=30)
    _DataManager()._set(output)
    assert _OrchestratorFactory._dispatcher._needs_to_run(task)


# ################################  UTIL METHODS    ##################################


def _create_task(function, nb_outputs=1):
    output_dn_config_id = "".join(random.choice(string.ascii_lowercase) for _ in range(10))
    dn_input_configs = [
        Config.configure_data_node("input1", "pickle", Scope.SCENARIO, default_data=21),
        Config.configure_data_node("input2", "pickle", Scope.SCENARIO, default_data=2),
    ]
    dn_output_configs = [
        Config.configure_data_node(f"{output_dn_config_id}_output{i}", "pickle", Scope.SCENARIO, default_data=0)
        for i in range(nb_outputs)
    ]
    input_dn = _DataManager._bulk_get_or_create(dn_input_configs).values()
    output_dn = _DataManager._bulk_get_or_create(dn_output_configs).values()
    return Task(
        output_dn_config_id,
        {},
        function=function,
        input=input_dn,
        output=output_dn,
    )


def _create_task_from_config(task_cfg):
    return _TaskManager()._bulk_get_or_create([task_cfg])[0]
