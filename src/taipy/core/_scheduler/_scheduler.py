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
import uuid
from multiprocessing import Lock
from queue import Queue
from typing import Callable, Dict, Iterable, List, Optional, Set, Union

from taipy.config.config import Config
from taipy.logger._taipy_logger import _TaipyLogger

from ..data._data_manager_factory import _DataManagerFactory
from ..job._job_manager_factory import _JobManagerFactory
from ..job.job import Job
from ..pipeline.pipeline import Pipeline
from ..task.task import Task
from ._abstract_scheduler import _AbstractScheduler
from ._dispatcher._development_job_dispatcher import _DevelopmentJobDispatcher
from ._dispatcher._standalone_job_dispatcher import _StandaloneJobDispatcher


class _Scheduler(_AbstractScheduler):
    """
    Handles the functional scheduling.

    Attributes:

    """

    jobs_to_run: Queue = Queue()
    blocked_jobs: List = []
    _dispatcher = None  # type: ignore
    lock = Lock()
    __logger = _TaipyLogger._get_logger()
    _processes: Dict = {}

    @classmethod
    def initialize(cls):
        pass

    @classmethod
    def is_running(cls) -> bool:
        """Returns False since the default scheduler is not runnable."""
        return False

    @classmethod
    def start(cls):
        RuntimeError("The default scheduler cannot be started.")

    @classmethod
    def stop(cls):
        RuntimeError("The default scheduler cannot be started nor stopped.")

    @classmethod
    def submit(
        cls, pipeline: Pipeline, callbacks: Optional[Iterable[Callable]] = None, force: bool = False
    ) -> List[Job]:
        """Submit the given `Pipeline^` for an execution.

        Parameters:
             pipeline (Pipeline^): The pipeline to submit for execution.
             callbacks: The optional list of functions that should be executed on jobs status change.
             force (bool) : Enforce execution of the pipeline's tasks even if their output data
                nodes are cached.

        Returns:
            The created Jobs.
        """
        submit_id = cls.__generate_submit_id()
        res = []
        tasks = pipeline._get_sorted_tasks()
        for ts in tasks:
            for task in ts:
                res.append(cls.submit_task(task, submit_id, callbacks=callbacks, force=force))
        return res

    @classmethod
    def submit_task(
        cls, task: Task, submit_id: str = None, callbacks: Optional[Iterable[Callable]] = None, force: bool = False
    ) -> Job:
        """Submit the given `Task^` for an execution.

        Parameters:
             task (Task^): The task to submit for execution.
             submit_id (str): The optional id to differentiate each submission.
             callbacks: The optional list of functions that should be executed on job status change.
             force (bool): Enforce execution of the task even if its output data nodes are cached.

        Returns:
            The created `Job^`.
        """
        submit_id = submit_id if submit_id else cls.__generate_submit_id()

        for dn in task.output.values():
            dn.lock_edit()
        job = _JobManagerFactory._build_manager()._create(
            task, itertools.chain([cls._on_status_change], callbacks or []), submit_id
        )
        cls._check_block_and_run_job(job)
        return job

    @staticmethod
    def __generate_submit_id():
        return f"SUBMISSION_{str(uuid.uuid4())}"

    @classmethod
    def _check_block_and_run_job(cls, job: Job):
        if cls._is_blocked(job):
            job.blocked()
            with cls.lock:
                cls.blocked_jobs.append(job)
        else:
            job.pending()
            with cls.lock:
                cls.jobs_to_run.put(job)
            cls.__execute_jobs()

    @staticmethod
    def _is_blocked(obj: Union[Task, Job]) -> bool:
        """Returns True if the execution of the `Job^` or the `Task^` is blocked by the execution of another `Job^`.

        Parameters:
             obj (Union[Task^, Job^]): The job or task entity to run.

        Returns:
             True if one of its input data nodes is blocked.
        """
        data_nodes = obj.task.input.values() if isinstance(obj, Job) else obj.input.values()
        data_manager = _DataManagerFactory._build_manager()
        return any(not data_manager._get(dn.id).is_ready_for_reading for dn in data_nodes)

    @classmethod
    def __execute_jobs(cls):
        if not cls._dispatcher:
            cls._update_job_config()
        while not cls.jobs_to_run.empty() and cls._dispatcher._can_execute():
            with cls.lock:
                try:
                    job = cls.jobs_to_run.get()
                except:  # In case the last job of the queue has been removed.
                    cls.__logger.warning(f"{job.id} is no longer in the list of jobs to run.")
            if job.force or cls._needs_to_run(job.task):
                if job.force:
                    cls.__logger.info(f"job {job.id} is forced to be executed.")
                job.running()
                _JobManagerFactory._build_manager()._set(job)
                cls._dispatcher._dispatch(job)
            else:
                cls.__unlock_edit_on_outputs(job)
                job.skipped()
                _JobManagerFactory._build_manager()._set(job)
                cls.__logger.info(f"job {job.id} is skipped.")

    @staticmethod
    def _needs_to_run(task: Task) -> bool:
        """
        Returns True if the task has no output or if at least one input was modified since the latest run.

        Parameters:
             task (Task^): The task to run.
        Returns:
             True if the task needs to run. False otherwise.
        """
        data_manager = _DataManagerFactory._build_manager()
        if len(task.output) == 0:
            return True
        are_outputs_in_cache = all(data_manager._get(dn.id)._is_in_cache for dn in task.output.values())
        if not are_outputs_in_cache:
            return True
        if len(task.input) == 0:
            return False
        input_last_edit = max(data_manager._get(dn.id).last_edit_date for dn in task.input.values())
        output_last_edit = min(data_manager._get(dn.id).last_edit_date for dn in task.output.values())
        return input_last_edit > output_last_edit

    @staticmethod
    def __unlock_edit_on_outputs(jobs: Union[Job, List[Job], Set[Job]]):
        jobs = [jobs] if isinstance(jobs, Job) else jobs
        for job in jobs:
            for dn in job.task.output.values():
                dn.unlock_edit(at=dn.last_edit_date)

    @classmethod
    def _on_status_change(cls, job: Job):
        if job.is_completed():
            cls.__unblock_jobs()
            cls.__execute_jobs()
        elif job.is_cancelled():
            cls.__execute_jobs()

    @classmethod
    def __unblock_jobs(cls):
        for job in cls.blocked_jobs:
            if not cls._is_blocked(job):
                with cls.lock:
                    job.pending()
                    cls.__remove_blocked_job(job)
                    cls.jobs_to_run.put(job)

    @classmethod
    def __remove_blocked_job(cls, job):
        try:  # In case the job has been removed from the list of blocked_jobs.
            cls.blocked_jobs.remove(job)
        except:
            cls.__logger.warning(f"{job.id} is not in the blocked list anymore.")

    @classmethod
    def _update_job_config(cls):
        if Config.job_config.is_standalone:  # type: ignore
            cls._dispatcher = _StandaloneJobDispatcher()
        else:
            cls._dispatcher = _DevelopmentJobDispatcher()

    @classmethod
    def cancel_job(cls, job: Job):
        to_cancel_jobs = set([job])
        to_cancel_jobs.update(cls.__find_subsequent_jobs(job.submit_id, set(job.task.output.keys())))
        with cls.lock:
            cls.__remove_blocked_jobs(to_cancel_jobs)
            cls.__remove_jobs_to_run(to_cancel_jobs)
            cls.__cancel_jobs_and_processes(job.id, to_cancel_jobs)
            cls.__unlock_edit_on_outputs(to_cancel_jobs)

    @classmethod
    def __find_subsequent_jobs(cls, submit_id, output_dn_config_ids: Set) -> Set[Job]:
        next_output_dn_config_ids = set()
        subsequent_jobs = set()
        for job in cls.blocked_jobs:
            job_input_dn_config_ids = job.task.input.keys()
            if job.submit_id == submit_id and len(output_dn_config_ids.intersection(job_input_dn_config_ids)) > 0:
                next_output_dn_config_ids.update(job.task.output.keys())
                subsequent_jobs.update([job])

        if len(next_output_dn_config_ids) > 0:
            subsequent_jobs.update(
                cls.__find_subsequent_jobs(submit_id, output_dn_config_ids=next_output_dn_config_ids)
            )

        return subsequent_jobs

    @classmethod
    def __remove_blocked_jobs(cls, jobs):
        for job in jobs:
            cls.__remove_blocked_job(job)

    @classmethod
    def __remove_jobs_to_run(cls, jobs):
        new_jobs_to_run: Queue = Queue()
        while not cls.jobs_to_run.empty():
            current_job = cls.jobs_to_run.get()
            if current_job not in jobs:
                new_jobs_to_run.put(current_job)
        cls.jobs_to_run = new_jobs_to_run

    @classmethod
    def __cancel_jobs_and_processes(cls, job_id_to_cancel, jobs):
        for job in jobs:
            if process := cls._pop_process_in_scheduler(job.id):
                process.cancel()  # TODO: this doesn't terminate the running process
            if job_id_to_cancel == job.id:
                job.cancelled()
            else:
                job.abandoned()

    @classmethod
    def _set_process_in_scheduler(cls, job_id, process):
        cls._processes[job_id] = process

    @classmethod
    def _pop_process_in_scheduler(cls, job_id, default=None):
        return cls._processes.pop(job_id, default)  # type: ignore
