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

import calendar
from datetime import datetime, time, timedelta
from typing import Optional

from taipy.config.common.frequency import Frequency

from .._manager._manager import _Manager
from ..common._entity_ids import _EntityIds
from ..common.alias import CycleId
from ..job._job_manager_factory import _JobManagerFactory
from ._cycle_repository_factory import _CycleRepositoryFactory
from .cycle import Cycle


class _CycleManager(_Manager[Cycle]):
    _repository = _CycleRepositoryFactory._build_repository()  # type: ignore
    _ENTITY_NAME = Cycle.__name__

    @classmethod
    def _create(
        cls, frequency: Frequency, name: str = None, creation_date: datetime = None, display_name=None, **properties
    ):
        creation_date = creation_date if creation_date else datetime.now()
        start_date = _CycleManager._get_start_date_of_cycle(frequency, creation_date)
        end_date = _CycleManager._get_end_date_of_cycle(frequency, start_date)
        properties["display_name"] = display_name if display_name else start_date.isoformat()
        cycle = Cycle(
            frequency, properties, creation_date=creation_date, start_date=start_date, end_date=end_date, name=name
        )
        cls._set(cycle)
        return cycle

    @classmethod
    def _get_or_create(
        cls, frequency: Frequency, creation_date: Optional[datetime] = None, display_name: Optional[str] = None
    ) -> Cycle:
        creation_date = creation_date if creation_date else datetime.now()
        start_date = _CycleManager._get_start_date_of_cycle(frequency, creation_date)
        cycles = cls._repository.get_cycles_by_frequency_and_start_date(frequency=frequency, start_date=start_date)  # type: ignore
        if len(cycles) > 0:
            return cycles[0]
        else:
            return cls._create(frequency=frequency, creation_date=creation_date, display_name=display_name)

    @staticmethod
    def _get_start_date_of_cycle(frequency: Frequency, creation_date: datetime):
        start_date = creation_date.date()
        start_time = time()
        if frequency == Frequency.DAILY:
            start_date = start_date
        if frequency == Frequency.WEEKLY:
            start_date = start_date - timedelta(days=start_date.weekday())
        if frequency == Frequency.MONTHLY:
            start_date = start_date.replace(day=1)
        if frequency == Frequency.YEARLY:
            start_date = start_date.replace(day=1, month=1)
        return datetime.combine(start_date, start_time)

    @staticmethod
    def _get_end_date_of_cycle(frequency: Frequency, start_date: datetime):
        end_date = start_date
        if frequency == Frequency.DAILY:
            end_date = end_date + timedelta(days=1)
        if frequency == Frequency.WEEKLY:
            end_date = end_date + timedelta(7 - end_date.weekday())
        if frequency == Frequency.MONTHLY:
            last_day_of_month = calendar.monthrange(start_date.year, start_date.month)[1]
            end_date = end_date.replace(day=last_day_of_month) + timedelta(days=1)
        if frequency == Frequency.YEARLY:
            end_date = end_date.replace(month=12, day=31) + timedelta(days=1)
        return end_date - timedelta(microseconds=1)

    @classmethod
    def _hard_delete(cls, cycle_id: CycleId):
        cycle = cls._get(cycle_id)
        entity_ids_to_delete = cls._get_owned_entities(cycle)
        entity_ids_to_delete.cycle_ids.add(cycle.id)
        cls._delete_entities_of_multiple_types(entity_ids_to_delete)

    @classmethod
    def _get_owned_entities(cls, cycle: Cycle) -> _EntityIds:
        from ..scenario._scenario_manager_factory import _ScenarioManagerFactory

        entity_ids = _EntityIds()

        scenarios = _ScenarioManagerFactory._build_manager()._get_all_by_cycle(cycle)

        for scenario in scenarios:
            entity_ids.scenario_ids.add(scenario.id)
            for pipeline in scenario.pipelines.values():
                parent_ids = {pipeline.id, scenario.id, cycle.id}
                if pipeline.parent_id in parent_ids:
                    entity_ids.pipeline_ids.add(pipeline.id)
                for task in pipeline.tasks.values():
                    if task.parent_id in parent_ids:
                        entity_ids.task_ids.add(task.id)
                    for data_node in task.data_nodes.values():
                        if data_node.parent_id in parent_ids:
                            entity_ids.data_node_ids.add(data_node.id)

        jobs = _JobManagerFactory._build_manager()._get_all()
        for job in jobs:
            if job.task.id in entity_ids.task_ids:
                entity_ids.job_ids.add(job.id)

        return entity_ids
