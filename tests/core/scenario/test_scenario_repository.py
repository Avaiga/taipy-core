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

from taipy.core.cycle._cycle_repository import _CycleRepository
from taipy.core.pipeline._pipeline_repository import _PipelineRepository
from taipy.core.scenario._scenario_repository import _ScenarioRepository

from taipy.core.pipeline.pipeline import Pipeline
from taipy.core.scenario.scenario import Scenario


def test_save_and_load(tmpdir, scenario):
    repository = _ScenarioRepository()
    repository.base_path = tmpdir
    repository._save(scenario)
    sc = repository.load(scenario.id)

    assert isinstance(sc, Scenario)
    assert scenario.id == sc.id


def test_from_and_to_model(scenario, scenario_model):
    repository = _ScenarioRepository()
    assert repository._to_model(scenario) == scenario_model
    assert repository._from_model(scenario_model) == scenario


def test_eager_loading(scenario, pipeline, cycle):
    sc_rp = _ScenarioRepository()
    pl_rp = _PipelineRepository()
    pipeline_1 = Pipeline("pipeline_1", {}, [], "pipeline_1")

    sc_rp._save(scenario)
    pl_rp._save(pipeline)
    pl_rp._save(pipeline_1)

    assert len(scenario._pipelines) == 0
    scenario.pipelines = [pipeline]

    with mock.patch("taipy.core.pipeline._pipeline_manager._PipelineManager._get") as mck:
        sc_rp.load(scenario.id, lazy_loading=False)
        mck.assert_called_once()

    scenario.pipelines = [pipeline, pipeline_1]
    with mock.patch("taipy.core.pipeline._pipeline_manager._PipelineManager._get") as mck:
        sc_rp.load(scenario.id, lazy_loading=False)
        assert mck.call_count == 2

    scenario_1 = sc_rp.load(scenario.id, lazy_loading=False)
    assert len(scenario_1._pipelines) == 2

    scenario.pipelines = [pipeline]
    with mock.patch("taipy.core.pipeline._pipeline_manager._PipelineManager._get") as mck:
        sc_rp.load(scenario.id, scenario_1, lazy_loading=False)
        mck.assert_called_once()

    scenario.cycle = cycle
    with mock.patch("taipy.core.cycle._cycle_manager._CycleManager._get") as mck:
        sc_rp.load(scenario.id, scenario, lazy_loading=False)
        mck.assert_called_once()

    with mock.patch("taipy.core.cycle._cycle_manager._CycleManager._get") as mck:
        sc_rp.load(scenario.id, scenario_1, lazy_loading=False)
        mck.assert_called_once()


def test_lazy_loading(scenario, pipeline, cycle):
    sc_rp = _ScenarioRepository()
    pl_rp = _PipelineRepository()
    pipeline_1 = Pipeline("pipeline_1", {}, [], "pipeline_1")

    sc_rp._save(scenario)
    pl_rp._save(pipeline)
    pl_rp._save(pipeline_1)

    assert len(scenario._pipelines) == 0
    scenario.pipelines = [pipeline]
    with mock.patch("taipy.core.pipeline._pipeline_manager._PipelineManager._get") as mck:
        sc_rp.load(scenario.id, scenario)
        mck.assert_not_called()

    with mock.patch("taipy.core.pipeline._pipeline_manager._PipelineManager._get") as mck:
        sc_rp.load(scenario.id)
        mck.assert_called_once()

    scenario_1 = sc_rp.load(scenario.id, scenario)
    assert len(scenario_1._pipelines) == 1

    scenario.pipelines = [pipeline, pipeline_1]
    with mock.patch("taipy.core.pipeline._pipeline_manager._PipelineManager._get") as mck:
        sc_rp.load(scenario.id, scenario)
        mck.assert_not_called()

    with mock.patch("taipy.core.pipeline._pipeline_manager._PipelineManager._get") as mck:
        sc_rp.load(scenario.id, scenario_1)
        mck.assert_called_once()

    scenario.pipelines = [pipeline]
    with mock.patch("taipy.core.pipeline._pipeline_manager._PipelineManager._get") as mck:
        sc_rp.load(scenario.id, scenario_1)
        mck.assert_not_called()

    scenario.pipelines = [pipeline_1]
    with mock.patch("taipy.core.pipeline._pipeline_manager._PipelineManager._get") as mck:
        sc_rp.load(scenario.id, scenario_1)
        mck.assert_called_once()

    scenario.pipelines = []
    with mock.patch("taipy.core.pipeline._pipeline_manager._PipelineManager._get") as mck:
        sc_rp.load(scenario.id, scenario_1)
        mck.assert_not_called()

    with mock.patch("taipy.core.pipeline._pipeline_manager._PipelineManager._get") as mck:
        sc_rp.load(scenario.id, scenario)
        mck.assert_not_called()

    scenario.cycle = cycle
    with mock.patch("taipy.core.cycle._cycle_manager._CycleManager._get") as mck:
        sc_rp.load(scenario.id)
        mck.assert_called_once()

    with mock.patch("taipy.core.cycle._cycle_manager._CycleManager._get") as mck:
        sc_rp.load(scenario.id, scenario)
        mck.assert_not_called()

    with mock.patch("taipy.core.cycle._cycle_manager._CycleManager._get") as mck:
        sc_rp.load(scenario.id, scenario_1)
        mck.assert_called_once()
