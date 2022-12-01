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

from click.testing import CliRunner

from src.taipy.core.exceptions.exceptions import VersionAlreadyExists
from src.taipy.core._version._version_cli import version_cli
from src.taipy.core._version._version_manager import _VersionManager
from src.taipy.core.cycle._cycle_manager import _CycleManager
from src.taipy.core.data._data_manager import _DataManager
from src.taipy.core.job._job_manager import _JobManager
from src.taipy.core.pipeline._pipeline_manager import _PipelineManager
from src.taipy.core.scenario._scenario_manager import _ScenarioManager
from src.taipy.core.task._task_manager import _TaskManager
from taipy.config.common.frequency import Frequency
from taipy.config.common.scope import Scope
from taipy.config.config import Config


def test_dev_mode_clean_all_entities_of_the_current_version():
    runner = CliRunner()

    # Create a scenario in development mode
    runner.invoke(version_cli, ["--development"])
    submit_scenario()

    # Initial assertion
    assert len(_DataManager._get_all()) == 2
    assert len(_TaskManager._get_all()) == 1
    assert len(_PipelineManager._get_all()) == 1
    assert len(_ScenarioManager._get_all()) == 1
    assert len(_CycleManager._get_all()) == 1
    assert len(_JobManager._get_all()) == 1

    # Create a new scenario in experiment mode
    runner.invoke(version_cli, ["--experiment"])
    submit_scenario()

    # Assert number of entities in 2nd version
    assert len(_DataManager._get_all()) == 4
    assert len(_TaskManager._get_all()) == 2
    assert len(_PipelineManager._get_all()) == 2
    assert len(_ScenarioManager._get_all()) == 2
    assert len(_CycleManager._get_all()) == 1  # No new cycle is created since old dev version use the same cycle
    assert len(_JobManager._get_all()) == 2

    # Run development mode again
    runner.invoke(version_cli, ["--development"])

    # The 1st dev version should be deleted run with development mode
    assert len(_DataManager._get_all()) == 2
    assert len(_TaskManager._get_all()) == 1
    assert len(_PipelineManager._get_all()) == 1
    assert len(_ScenarioManager._get_all()) == 1
    assert len(_CycleManager._get_all()) == 1
    assert len(_JobManager._get_all()) == 1

    # Submit new dev version
    submit_scenario()

    # Assert number of entities with 1 dev version and 1 exp version
    assert len(_DataManager._get_all()) == 4
    assert len(_TaskManager._get_all()) == 2
    assert len(_PipelineManager._get_all()) == 2
    assert len(_ScenarioManager._get_all()) == 2
    assert len(_CycleManager._get_all()) == 1
    assert len(_JobManager._get_all()) == 2


def test_version_number_when_switching_mode():
    runner = CliRunner()

    runner.invoke(version_cli, ["--development"])
    ver_1 = _VersionManager.get_current_version()
    assert len(_VersionManager._get_all()) == 1

    # Run with dev mode, the version number is the same
    runner.invoke(version_cli, ["--development"])
    ver_2 = _VersionManager.get_current_version()
    assert ver_1 == ver_2
    assert len(_VersionManager._get_all()) == 1

    # When run with experiment mode, a new version is created
    runner.invoke(version_cli, ["--experiment"])
    ver_3 = _VersionManager.get_current_version()
    assert ver_1 != ver_3
    assert len(_VersionManager._get_all()) == 2

    runner.invoke(version_cli, ["--experiment"])
    ver_4 = _VersionManager.get_current_version()
    assert ver_1 != ver_4
    assert ver_3 != ver_4
    assert len(_VersionManager._get_all()) == 3

    result = runner.invoke(version_cli, ["--experiment", "--version-number", "2.1"])
    ver_5 = _VersionManager.get_current_version()
    assert ver_5 == "2.1"
    assert len(_VersionManager._get_all()) == 4

    # Run with dev mode, the version number is the same as the first dev version to overide it
    runner.invoke(version_cli, ["--development"])
    ver_5 = _VersionManager.get_current_version()
    assert ver_1 == ver_5
    assert len(_VersionManager._get_all()) == 4


def test_override_version():
    runner = CliRunner()

    runner.invoke(version_cli, ["--experiment"])
    assert len(_VersionManager._get_all()) == 1

    runner.invoke(version_cli, ["--experiment", "--version-number", "2.1"])
    ver_2 = _VersionManager.get_current_version()
    assert ver_2 == "2.1"
    assert len(_VersionManager._get_all()) == 2

    # Without --override parameter
    result = runner.invoke(version_cli, ["--experiment", "--version-number", "2.1"])
    assert result.exit_code == 1 # Failed
    assert isinstance(result.exception, VersionAlreadyExists)
    assert result.exception.args[0] == "Version 2.1 already exists."

    # With --override parameter
    result = runner.invoke(version_cli, ["--experiment", "--version-number", "2.1", "--override"])
    assert result.exit_code == 0 # Success
    ver_2 = _VersionManager.get_current_version()
    assert ver_2 == "2.1"
    assert len(_VersionManager._get_all()) == 2


def task_test(a):
    return a


def submit_scenario():
    Config.unblock_update()
    data_node_1_config = Config.configure_data_node(id="d1", storage_type="in_memory", scope=Scope.SCENARIO)
    data_node_2_config = Config.configure_data_node(
        id="d2", storage_type="pickle", default_data="abc", scope=Scope.SCENARIO
    )
    task_config = Config.configure_task(
        "my_task", task_test, data_node_1_config, data_node_2_config, scope=Scope.SCENARIO
    )
    pipeline_config = Config.configure_pipeline("my_pipeline", task_config)
    scenario_config = Config.configure_scenario("my_scenario", pipeline_config, frequency=Frequency.DAILY)

    scenario = _ScenarioManager._create(scenario_config)

    _ScenarioManager._submit(scenario)
