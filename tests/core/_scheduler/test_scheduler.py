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
import random
import string
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from functools import partial
from time import sleep

import pytest

from src.taipy.core import taipy
from src.taipy.core._scheduler._executor._synchronous import _Synchronous
from src.taipy.core._scheduler._scheduler import _Scheduler
from src.taipy.core.data._data_manager import _DataManager
from src.taipy.core.data.in_memory import InMemoryDataNode
from src.taipy.core.pipeline._pipeline_manager import _PipelineManager
from src.taipy.core.task._task_manager import _TaskManager
from src.taipy.core.task.task import Task
from taipy.config import Config, JobConfig
from taipy.config._config import _Config
from taipy.config.data_node.scope import Scope
from tests.core.utils import assert_true_after_1_minute_max


@pytest.fixture(scope="function", autouse=True)
def reset_configuration_singleton():
    yield
    Config._python_config = _Config()
    Config._file_config = None
    Config._env_file_config = None
    Config._applied_config = _Config._default_config()

    for f in glob.glob("*.p"):
        print(f"deleting file {f}")
        os.remove(f)


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


# ################################  TEST METHODS    ##################################


def test_submit_task():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)
    _Scheduler._update_job_config()

    before_creation = datetime.now()
    sleep(0.1)
    task = _create_task(multiply)
    output_dn_id = task.output[f"{task.config_id}_output0"].id

    assert _DataManager._get(output_dn_id).last_edition_date > before_creation
    assert _DataManager._get(output_dn_id).job_ids == []
    assert _DataManager._get(output_dn_id).is_ready_for_reading

    before_submission_creation = datetime.now()
    sleep(0.1)
    job = _Scheduler.submit_task(task, "submit_id")
    sleep(0.1)
    after_submission_creation = datetime.now()
    assert _DataManager._get(output_dn_id).read() == 42
    assert _DataManager._get(output_dn_id).last_edition_date > before_submission_creation
    assert _DataManager._get(output_dn_id).last_edition_date < after_submission_creation
    assert _DataManager._get(output_dn_id).job_ids == [job.id]
    assert _DataManager._get(output_dn_id).is_ready_for_reading
    assert job.is_completed()


def test_submit_pipeline_generate_unique_submit_id(pipeline, task):
    dn_1 = InMemoryDataNode("dn_config_id_1", Scope.PIPELINE)
    dn_2 = InMemoryDataNode("dn_config_id_2", Scope.PIPELINE)
    task_1 = Task("task_config_id_1", print, [dn_1])
    task_2 = Task("task_config_id_2", print, [dn_2])
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


def test_submit_task_that_return_multiple_outputs():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)
    _Scheduler._update_job_config()

    def return_2tuple(nb1, nb2):
        return multiply(nb1, nb2), multiply(nb1, nb2) / 2

    def return_list(nb1, nb2):
        return [multiply(nb1, nb2), multiply(nb1, nb2) / 2]

    with_tuple = _create_task(return_2tuple, 2)
    with_list = _create_task(return_list, 2)

    _Scheduler.submit_task(with_tuple, "submit_id_1")
    _Scheduler.submit_task(with_list, "submit_id_2")

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
    _Scheduler._update_job_config()

    def return_2tuple(nb1, nb2):
        return multiply(nb1, nb2), multiply(nb1, nb2) / 2

    def return_list(nb1, nb2):
        return [multiply(nb1, nb2), multiply(nb1, nb2) / 2]

    task_with_tuple = _create_task(return_2tuple, 1)
    task_with_list = _create_task(return_list, 1)

    _Scheduler.submit_task(task_with_tuple, "submit_id_1")
    assert task_with_tuple.output[f"{task_with_tuple.config_id}_output0"].read() == (42, 21)
    assert len(_Scheduler._processes) == 0
    _Scheduler.submit_task(task_with_list, "submit_id_2")
    assert task_with_list.output[f"{task_with_list.config_id}_output0"].read() == [42, 21]
    assert len(_Scheduler._processes) == 0


def test_data_node_not_written_due_to_wrong_result_nb():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)
    _Scheduler._update_job_config()

    def return_2tuple():
        return lambda nb1, nb2: (multiply(nb1, nb2), multiply(nb1, nb2) / 2)

    task = _create_task(return_2tuple(), 3)

    job = _Scheduler.submit_task(task, "submit_id")
    assert task.output[f"{task.config_id}_output0"].read() == 0
    assert job.is_failed()
    assert len(_Scheduler._processes) == 0


def test_submit_task_in_parallel():
    m = multiprocessing.Manager()
    lock = m.Lock()

    Config.configure_job_executions(mode="standalone", nb_of_workers=2)
    _Scheduler._update_job_config()
    task = _create_task(partial(lock_multiply, lock))

    with lock:
        job = _Scheduler.submit_task(task, "submit_id")
        assert task.output[f"{task.config_id}_output0"].read() == 0
        assert job.is_running()
        assert len(_Scheduler._processes) == 1

    assert_true_after_1_minute_max(job.is_completed)
    assert len(_Scheduler._processes) == 0


def test_submit_task_multithreading_multiple_task():
    Config.configure_job_executions(mode="standalone", nb_of_workers=2)
    _Scheduler._update_job_config()

    m = multiprocessing.Manager()
    lock_1 = m.Lock()
    lock_2 = m.Lock()

    task_1 = _create_task(partial(lock_multiply, lock_1))
    task_2 = _create_task(partial(lock_multiply, lock_2))

    with lock_1:
        with lock_2:
            job_1 = _Scheduler.submit_task(task_1, "submit_id_1")
            job_2 = _Scheduler.submit_task(task_2, "submit_id_2")

            assert len(_Scheduler._processes) == 2
            assert task_1.output[f"{task_1.config_id}_output0"].read() == 0
            assert task_2.output[f"{task_2.config_id}_output0"].read() == 0
            assert job_1.is_running()
            assert job_2.is_running()

        assert_true_after_1_minute_max(lambda: task_2.output[f"{task_2.config_id}_output0"].read() == 42)
        assert task_1.output[f"{task_1.config_id}_output0"].read() == 0
        assert_true_after_1_minute_max(job_2.is_completed)
        assert job_1.is_running()
        assert job_2.is_completed()
        assert len(_Scheduler._processes) == 1

    assert_true_after_1_minute_max(lambda: task_1.output[f"{task_1.config_id}_output0"].read() == 42)
    assert task_2.output[f"{task_2.config_id}_output0"].read() == 42
    assert_true_after_1_minute_max(job_1.is_completed)
    assert job_2.is_completed()
    assert len(_Scheduler._processes) == 0


def test_submit_task_multithreading_multiple_task_in_sync_way_to_check_job_status():
    Config.configure_job_executions(mode="standalone", nb_of_workers=2)
    _Scheduler._update_job_config()

    m = multiprocessing.Manager()
    lock_0 = m.Lock()
    lock_1 = m.Lock()
    lock_2 = m.Lock()

    task_0 = _create_task(partial(lock_multiply, lock_0))
    task_1 = _create_task(partial(lock_multiply, lock_1))
    task_2 = _create_task(partial(lock_multiply, lock_2))

    with lock_0:
        job_0 = _Scheduler.submit_task(task_0, "submit_id_0")
        assert job_0.is_running()
        assert len(_Scheduler._processes) == 1
        with lock_1:
            with lock_2:
                job_1 = _Scheduler.submit_task(task_2, "submit_id_2")
                job_2 = _Scheduler.submit_task(task_1, "submit_id_1")

                assert len(_Scheduler._processes) == 2
                assert task_1.output[f"{task_1.config_id}_output0"].read() == 0
                assert task_2.output[f"{task_2.config_id}_output0"].read() == 0
                assert job_1.is_running()
                assert job_2.is_pending()

            assert_true_after_1_minute_max(lambda: task_2.output[f"{task_2.config_id}_output0"].read() == 42)
            assert len(_Scheduler._processes) == 2
            assert task_1.output[f"{task_1.config_id}_output0"].read() == 0
            assert_true_after_1_minute_max(job_1.is_completed)
            assert_true_after_1_minute_max(job_2.is_running)

    assert_true_after_1_minute_max(lambda: task_1.output[f"{task_1.config_id}_output0"].read() == 42)
    assert task_2.output[f"{task_2.config_id}_output0"].read() == 42
    assert job_1.is_completed()
    assert_true_after_1_minute_max(job_2.is_completed)


def test_blocked_task():
    Config.configure_job_executions(mode="standalone", nb_of_workers=2)
    _Scheduler._update_job_config()

    m = multiprocessing.Manager()
    lock_1 = m.Lock()
    lock_2 = m.Lock()

    foo_cfg = Config.configure_data_node("foo", default_data=1)
    bar_cfg = Config.configure_data_node("bar")
    baz_cfg = Config.configure_data_node("baz")
    dns = _DataManager._bulk_get_or_create([foo_cfg, bar_cfg, baz_cfg])
    foo = dns[foo_cfg]
    bar = dns[bar_cfg]
    baz = dns[baz_cfg]
    task_1 = Task("by_2", partial(lock_multiply, lock_1, 2), [foo], [bar])
    task_2 = Task("by_3", partial(lock_multiply, lock_2, 3), [bar], [baz])

    assert task_1.foo.is_ready_for_reading  # foo is ready
    assert not task_1.bar.is_ready_for_reading  # But bar is not ready
    assert not task_2.baz.is_ready_for_reading  # neither does baz

    assert len(_Scheduler.blocked_jobs) == 0
    job_2 = _Scheduler.submit_task(task_2, "submit_id_2")  # job 2 is submitted first
    assert job_2.is_blocked()  # since bar is not up_to_date the job 2 is blocked
    assert len(_Scheduler._processes) == 0
    assert len(_Scheduler.blocked_jobs) == 1
    with lock_2:
        with lock_1:
            job_1 = _Scheduler.submit_task(task_1, "submit_id_1")  # job 1 is submitted and locked
            assert job_1.is_running()  # so it is still running
            assert len(_Scheduler._processes) == 1
            assert not _DataManager._get(task_1.bar.id).is_ready_for_reading  # And bar still not ready
            assert job_2.is_blocked()  # the job_2 remains blocked
        assert_true_after_1_minute_max(job_1.is_completed)  # job1 unlocked and can complete
        assert _DataManager._get(task_1.bar.id).is_ready_for_reading  # bar becomes ready
        assert _DataManager._get(task_1.bar.id).read() == 2  # the data is computed and written
        assert_true_after_1_minute_max(job_2.is_running)  # And job 2 can start running
        assert len(_Scheduler._processes) == 1
        assert len(_Scheduler.blocked_jobs) == 0
    assert_true_after_1_minute_max(job_2.is_completed)  # job 2 unlocked so it can complete
    assert _DataManager._get(task_2.baz.id).is_ready_for_reading  # baz becomes ready
    assert _DataManager._get(task_2.baz.id).read() == 6  # the data is computed and written
    assert len(_Scheduler._processes) == 0


def test_task_scheduler_create_synchronous_dispatcher():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)
    _Scheduler._update_job_config()

    assert isinstance(_Scheduler._dispatcher._executor, _Synchronous)
    assert _Scheduler._dispatcher._nb_available_workers == 1


def test_task_scheduler_create_standalone_dispatcher():
    Config.configure_job_executions(mode="standalone", nb_of_workers=42)
    _Scheduler._update_job_config()
    assert isinstance(_Scheduler._dispatcher._executor, ProcessPoolExecutor)
    assert _Scheduler._dispatcher._nb_available_workers == 42


def test_can_exec_task_with_modified_config():
    assert Config.global_config.storage_folder == ".data/"
    Config.configure_global_app(storage_folder=".my_data/", clean_entities_enabled=True)
    assert Config.global_config.storage_folder == ".my_data/"

    dn_input_config = Config.configure_data_node("input", "pickle", scope=Scope.PIPELINE, default_data=1)
    dn_output_config = Config.configure_data_node("output", "pickle")
    task_config = Config.configure_task("task_config", mult_by_2, dn_input_config, dn_output_config)
    pipeline_config = Config.configure_pipeline("pipeline_config", [task_config])
    pipeline = _PipelineManager._get_or_create(pipeline_config)

    pipeline.submit()
    while pipeline.output.edit_in_progress:
        sleep(1)
    assert 2 == pipeline.output.read()
    taipy.clean_all_entities()


def test_can_execute_task_with_development_mode():
    assert Config.job_config.mode == JobConfig._DEVELOPMENT_MODE

    dn_input_config = Config.configure_data_node("input", "pickle", scope=Scope.PIPELINE, default_data=1)
    dn_output_config = Config.configure_data_node("output", "pickle")
    task_config = Config.configure_task("task_config", mult_by_2, dn_input_config, dn_output_config)
    pipeline_config = Config.configure_pipeline("pipeline_config", [task_config])
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

    assert _Scheduler()._needs_to_run(task)


def test_need_to_run_output_not_cacheable():
    hello_cfg = Config.configure_data_node("hello", default_data="Hello ")
    world_cfg = Config.configure_data_node("world", default_data="world !")
    hello_world_cfg = Config.configure_data_node("hello_world", cacheable=False)
    task_cfg = Config.configure_task("name", input=[hello_cfg, world_cfg], function=concat, output=[hello_world_cfg])
    task = _create_task_from_config(task_cfg)

    assert _Scheduler()._needs_to_run(task)


def test_need_to_run_output_cacheable_no_input():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)
    _Scheduler._update_job_config()

    hello_world_cfg = Config.configure_data_node("hello_world", cacheable=True)
    task_cfg = Config.configure_task("name", input=[], function=nothing, output=[hello_world_cfg])
    task = _create_task_from_config(task_cfg)

    assert _Scheduler._needs_to_run(task)
    _Scheduler.submit_task(task, "submit_id")

    assert not _Scheduler._needs_to_run(task)


def test_need_to_run_output_cacheable_no_validity_period():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)
    _Scheduler._update_job_config()

    hello_cfg = Config.configure_data_node("hello", default_data="Hello ")
    world_cfg = Config.configure_data_node("world", default_data="world !")
    hello_world_cfg = Config.configure_data_node("hello_world", cacheable=True)
    task_cfg = Config.configure_task("name", input=[hello_cfg, world_cfg], function=concat, output=[hello_world_cfg])
    task = _create_task_from_config(task_cfg)

    assert _Scheduler._needs_to_run(task)
    _Scheduler.submit_task(task, "submit_id")

    assert not _Scheduler._needs_to_run(task)


def test_need_to_run_output_cacheable_with_validity_period_up_to_date():
    Config.configure_job_executions(mode=JobConfig._DEVELOPMENT_MODE)
    _Scheduler._update_job_config()

    hello_cfg = Config.configure_data_node("hello", default_data="Hello ")
    world_cfg = Config.configure_data_node("world", default_data="world !")
    hello_world_cfg = Config.configure_data_node("hello_world", cacheable=True, validity_days=1)
    task_cfg = Config.configure_task("name", input=[hello_cfg, world_cfg], function=concat, output=[hello_world_cfg])
    task = _create_task_from_config(task_cfg)

    assert _Scheduler._needs_to_run(task)
    job = _Scheduler.submit_task(task, "submit_id_1")

    assert not _Scheduler._needs_to_run(task)
    job_skipped = _Scheduler.submit_task(task, "submit_id_2")

    assert job.is_completed()
    assert job.is_finished()
    assert job_skipped.is_skipped()
    assert job_skipped.is_finished()


def test_need_to_run_output_cacheable_with_validity_period_obsolete():
    hello_cfg = Config.configure_data_node("hello", default_data="Hello ")
    world_cfg = Config.configure_data_node("world", default_data="world !")
    hello_world_cfg = Config.configure_data_node("hello_world", cacheable=True, validity_days=1)
    task_cfg = Config.configure_task("name", input=[hello_cfg, world_cfg], function=concat, output=[hello_world_cfg])
    task = _create_task_from_config(task_cfg)

    assert _Scheduler._needs_to_run(task)
    _Scheduler.submit_task(task, "submit_id")

    output = task.hello_world
    output._last_edit_date = datetime.now() - timedelta(days=1, minutes=30)
    _DataManager()._set(output)
    assert _Scheduler._needs_to_run(task)


# ################################  UTIL METHODS    ##################################


def _create_task(function, nb_outputs=1):
    output_dn_config_id = "".join(random.choice(string.ascii_lowercase) for _ in range(10))
    dn_input_configs = [
        Config.configure_data_node("input1", "pickle", Scope.PIPELINE, default_data=21),
        Config.configure_data_node("input2", "pickle", Scope.PIPELINE, default_data=2),
    ]
    dn_output_configs = [
        Config.configure_data_node(f"{output_dn_config_id}_output{i}", "pickle", Scope.PIPELINE, default_data=0)
        for i in range(nb_outputs)
    ]
    input_dn = _DataManager._bulk_get_or_create(dn_input_configs).values()
    output_dn = _DataManager._bulk_get_or_create(dn_output_configs).values()

    return Task(
        output_dn_config_id,
        function=function,
        input=input_dn,
        output=output_dn,
    )


def _create_task_from_config(task_cfg):
    return _TaskManager()._bulk_get_or_create([task_cfg])[0]
