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
from datetime import datetime
from typing import List, Optional

from taipy.core.common.alias import CycleId, PipelineId
from taipy.core.cycle._cycle_manager_factory import _CycleManagerFactory
from taipy.core.pipeline._pipeline_manager_factory import _PipelineManagerFactory
from taipy.core.scenario._scenario_model import _ScenarioModel

from taipy.core._repository import _FileSystemRepository
from taipy.core.common import _utils
from taipy.core.config.config import Config
from taipy.core.cycle.cycle import Cycle
from taipy.core.exceptions.exceptions import NonExistingPipeline
from taipy.core.pipeline.pipeline import Pipeline
from taipy.core.scenario.scenario import Scenario


class _ScenarioRepository(_FileSystemRepository[_ScenarioModel, Scenario]):
    def __init__(self):
        super().__init__(model=_ScenarioModel, dir_name="scenarios")

    def _to_model(self, scenario: Scenario):
        return _ScenarioModel(
            id=scenario.id,
            config_id=scenario.config_id,
            pipelines=self.__to_pipeline_ids(scenario._pipelines.values()),
            properties=scenario._properties.data,
            creation_date=scenario._creation_date.isoformat(),
            primary_scenario=scenario._primary_scenario,
            subscribers=_utils._fcts_to_dict(scenario._subscribers),
            tags=list(scenario._tags),
            cycle=self.__to_cycle_id(scenario._cycle),
        )

    def _from_model(self, model: _ScenarioModel, org_entity: Scenario = None, eager_loading: bool = False) -> Scenario:
        scenario = Scenario(
            scenario_id=model.id,
            config_id=model.config_id,
            pipelines=self.__to_pipelines(
                model.pipelines, list(org_entity._pipelines.values()) if org_entity else None, eager_loading
            ),
            properties=model.properties,
            creation_date=datetime.fromisoformat(model.creation_date),
            is_primary=model.primary_scenario,
            tags=set(model.tags),
            cycle=self.__to_cycle(
                model.cycle, org_entity._cycle if org_entity and org_entity._cycle else None, eager_loading
            ),  # TODO: better implementation
            subscribers={
                _utils._load_fct(it["fct_module"], it["fct_name"]) for it in model.subscribers
            },  # type: ignore
        )
        return scenario

    @property
    def _storage_folder(self) -> pathlib.Path:
        return pathlib.Path(Config.global_config.storage_folder)  # type: ignore

    @staticmethod
    def __to_pipeline_ids(pipelines) -> List[PipelineId]:
        return [pipeline.id for pipeline in pipelines]

    @staticmethod
    def __to_pipelines(
        pipeline_ids: List[PipelineId], org_pipelines: List[Pipeline] = None, eager_loading: bool = False
    ) -> List[Pipeline]:
        pipelines = []
        pipeline_manager = _PipelineManagerFactory._build_manager()

        if eager_loading or org_pipelines is None:
            for _id in pipeline_ids:
                if pipeline := pipeline_manager._get(_id):
                    pipelines.append(pipeline)
                else:
                    raise NonExistingPipeline(_id)
        else:
            org_pipelines_dict = {p.id: p for p in org_pipelines}

            for _id in pipeline_ids:
                if _id in org_pipelines_dict.keys():
                    pipeline = org_pipelines_dict[_id]
                else:
                    pipeline = pipeline_manager._get(_id)

                if pipeline:
                    pipelines.append(pipeline)
                else:
                    raise NonExistingPipeline(_id)
        return pipelines

    @staticmethod
    def __to_cycle(cycle_id: CycleId = None, org_cycle: Cycle = None, eager_loading: bool = False) -> Optional[Cycle]:
        if eager_loading or org_cycle is None or cycle_id != org_cycle.id:
            return _CycleManagerFactory._build_manager()._get(cycle_id) if cycle_id else None
        else:
            return org_cycle

    @staticmethod
    def __to_cycle_id(cycle: Cycle = None) -> Optional[CycleId]:
        return cycle.id if cycle else None
