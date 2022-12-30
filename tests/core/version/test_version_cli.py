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

from unittest.mock import patch

import pytest

from src.taipy.core import Core
from src.taipy.core._version._version_cli import _VersioningCLI
from src.taipy.core._version._version_manager import _VersionManager
from src.taipy.core.cycle._cycle_manager import _CycleManager
from src.taipy.core.data._data_manager import _DataManager
from src.taipy.core.exceptions.exceptions import NonExistingVersion
from src.taipy.core.job._job_manager import _JobManager
from src.taipy.core.pipeline._pipeline_manager import _PipelineManager
from src.taipy.core.scenario._scenario_manager import _ScenarioManager
from src.taipy.core.task._task_manager import _TaskManager
from taipy.config._config import _Config
from taipy.config.common.frequency import Frequency
from taipy.config.common.scope import Scope
from taipy.config.config import Config


def reset_configuration_singleton():
    Config.unblock_update()
    Config._default_config = _Config._default_config()
    Config._python_config = _Config()
    Config._file_config = None
    Config._env_file_config = None
    Config._applied_config = _Config._default_config()


def test_version_cli_return_value():
    # Test default cli values
    _VersioningCLI._create_parser()
    mode, version_number, override = _VersioningCLI._parse_arguments()
    assert mode == "development"
    assert version_number is None
    assert not override

    # Test Dev mode
    with patch("sys.argv", ["prog", "--development"]):
        mode, _, _ = _VersioningCLI._parse_arguments()
    assert mode == "development"

    with patch("sys.argv", ["prog", "-dev"]):
        mode, _, _ = _VersioningCLI._parse_arguments()
    assert mode == "development"

    # Test Experiment mode
    with patch("sys.argv", ["prog", "--experiment"]):
        mode, version_number, override = _VersioningCLI._parse_arguments()
    assert mode == "experiment"
    assert version_number is None
    assert not override

    with patch("sys.argv", ["prog", "--experiment", "--version-number", "2.1"]):
        mode, version_number, override = _VersioningCLI._parse_arguments()
    assert mode == "experiment"
    assert version_number == "2.1"
    assert not override

    with patch("sys.argv", ["prog", "--experiment", "--version-number", "2.1", "--override"]):
        mode, version_number, override = _VersioningCLI._parse_arguments()
    assert mode == "experiment"
    assert version_number == "2.1"
    assert override

    # Test Production mode
    with patch("sys.argv", ["prog", "--production"]):
        mode, version_number, override = _VersioningCLI._parse_arguments()
    assert mode == "production"
    assert version_number is None
    assert not override


def test_dev_mode_clean_all_entities_of_the_latest_version():
    scenario_config = config_scenario()

    core = Core()

    # Create a scenario in development mode
    core.run()
    scenario = _ScenarioManager._create(scenario_config)
    _ScenarioManager._submit(scenario)

    # Initial assertion
    assert len(_DataManager._get_all(version_number="all")) == 2
    assert len(_TaskManager._get_all(version_number="all")) == 1
    assert len(_PipelineManager._get_all(version_number="all")) == 1
    assert len(_ScenarioManager._get_all(version_number="all")) == 1
    assert len(_CycleManager._get_all(version_number="all")) == 1
    assert len(_JobManager._get_all(version_number="all")) == 1

    # Create a new scenario in experiment mode
    with patch("sys.argv", ["prog", "--experiment"]):
        core.run()
    scenario = _ScenarioManager._create(scenario_config)
    _ScenarioManager._submit(scenario)

    # Assert number of entities in 2nd version
    assert len(_DataManager._get_all(version_number="all")) == 4
    assert len(_TaskManager._get_all(version_number="all")) == 2
    assert len(_PipelineManager._get_all(version_number="all")) == 2
    assert len(_ScenarioManager._get_all(version_number="all")) == 2
    assert (
        len(_CycleManager._get_all(version_number="all")) == 1
    )  # No new cycle is created since old dev version use the same cycle
    assert len(_JobManager._get_all(version_number="all")) == 2

    # Run development mode again
    with patch("sys.argv", ["prog", "--development"]):
        core.run()

    # The 1st dev version should be deleted run with development mode
    assert len(_DataManager._get_all(version_number="all")) == 2
    assert len(_TaskManager._get_all(version_number="all")) == 1
    assert len(_PipelineManager._get_all(version_number="all")) == 1
    assert len(_ScenarioManager._get_all(version_number="all")) == 1
    assert len(_CycleManager._get_all(version_number="all")) == 1
    assert len(_JobManager._get_all(version_number="all")) == 1

    # Submit new dev version
    scenario = _ScenarioManager._create(scenario_config)
    _ScenarioManager._submit(scenario)

    # Assert number of entities with 1 dev version and 1 exp version
    assert len(_DataManager._get_all(version_number="all")) == 4
    assert len(_TaskManager._get_all(version_number="all")) == 2
    assert len(_PipelineManager._get_all(version_number="all")) == 2
    assert len(_ScenarioManager._get_all(version_number="all")) == 2
    assert len(_CycleManager._get_all(version_number="all")) == 1
    assert len(_JobManager._get_all(version_number="all")) == 2

    # Assert number of entities of the latest version only
    assert len(_DataManager._get_all(version_number="latest")) == 2
    assert len(_TaskManager._get_all(version_number="latest")) == 1
    assert len(_PipelineManager._get_all(version_number="latest")) == 1
    assert len(_ScenarioManager._get_all(version_number="latest")) == 1
    assert len(_JobManager._get_all(version_number="latest")) == 1

    # Assert number of entities of the development version only
    assert len(_DataManager._get_all(version_number="development")) == 2
    assert len(_TaskManager._get_all(version_number="development")) == 1
    assert len(_PipelineManager._get_all(version_number="development")) == 1
    assert len(_ScenarioManager._get_all(version_number="development")) == 1
    assert len(_JobManager._get_all(version_number="development")) == 1

    # Assert number of entities of an unknown version
    with pytest.raises(NonExistingVersion):
        assert _DataManager._get_all(version_number="foo")
    with pytest.raises(NonExistingVersion):
        assert _TaskManager._get_all(version_number="foo")
    with pytest.raises(NonExistingVersion):
        assert _PipelineManager._get_all(version_number="foo")
    with pytest.raises(NonExistingVersion):
        assert _ScenarioManager._get_all(version_number="foo")
    with pytest.raises(NonExistingVersion):
        assert _JobManager._get_all(version_number="foo")


def test_version_number_when_switching_mode():
    core = Core()

    with patch("sys.argv", ["prog", "--development"]):
        core.run()
    ver_1 = _VersionManager._get_latest_version()
    ver_dev = _VersionManager._get_development_version()
    assert ver_1 == ver_dev
    assert len(_VersionManager._get_all()) == 1

    # Run with dev mode, the version number is the same
    with patch("sys.argv", ["prog", "--development"]):
        core.run()
    ver_2 = _VersionManager._get_latest_version()
    assert ver_2 == ver_dev
    assert len(_VersionManager._get_all()) == 1

    # When run with experiment mode, a new version is created
    with patch("sys.argv", ["prog", "--experiment"]):
        core.run()
    ver_3 = _VersionManager._get_latest_version()
    assert ver_3 != ver_dev
    assert len(_VersionManager._get_all()) == 2

    with patch("sys.argv", ["prog", "--experiment", "--version-number", "2.1"]):
        core.run()
    ver_4 = _VersionManager._get_latest_version()
    assert ver_4 == "2.1"
    assert len(_VersionManager._get_all()) == 3

    with patch("sys.argv", ["prog", "--experiment"]):
        core.run()
    ver_5 = _VersionManager._get_latest_version()
    assert ver_5 != ver_3
    assert ver_5 != ver_4
    assert ver_5 != ver_dev
    assert len(_VersionManager._get_all()) == 4

    # When run with production mode, the latest version is used as production
    with patch("sys.argv", ["prog", "--production"]):
        core.run()
    ver_6 = _VersionManager._get_latest_version()
    production_versions = _VersionManager._get_production_version()
    assert ver_6 == ver_5
    assert production_versions == [ver_6]
    assert len(_VersionManager._get_all()) == 4

    # When run with production mode, the "2.1" version is used as production
    with patch("sys.argv", ["prog", "--production", "--version-number", "2.1"]):
        core.run()
    ver_7 = _VersionManager._get_latest_version()
    production_versions = _VersionManager._get_production_version()
    assert ver_7 == "2.1"
    assert production_versions == [ver_6, ver_7]
    assert len(_VersionManager._get_all()) == 4

    # Run with dev mode, the version number is the same as the first dev version to overide it
    with patch("sys.argv", ["prog", "--development"]):
        core.run()
    ver_7 = _VersionManager._get_latest_version()
    assert ver_1 == ver_7
    assert len(_VersionManager._get_all()) == 4


def test_production_mode_load_all_entities_from_previous_production_version():
    scenario_config = config_scenario()

    core = Core()

    with patch("sys.argv", ["prog", "--production"]):
        core.run()
    production_ver_1 = _VersionManager._get_latest_version()
    assert _VersionManager._get_production_version() == [production_ver_1]
    # When run production mode on a new app, a dev version is created alongside
    assert _VersionManager._get_development_version() not in _VersionManager._get_production_version()
    assert len(_VersionManager._get_all()) == 2

    scenario = _ScenarioManager._create(scenario_config)
    _ScenarioManager._submit(scenario)

    assert len(_DataManager._get_all()) == 2
    assert len(_TaskManager._get_all()) == 1
    assert len(_PipelineManager._get_all()) == 1
    assert len(_ScenarioManager._get_all()) == 1
    assert len(_CycleManager._get_all()) == 1
    assert len(_JobManager._get_all()) == 1

    with patch("sys.argv", ["prog", "--production", "--version-number", "2.0"]):
        core.run()
    production_ver_2 = _VersionManager._get_latest_version()
    assert _VersionManager._get_production_version() == [production_ver_1, production_ver_2]
    assert len(_VersionManager._get_all()) == 3

    # All entities from previous production version should be saved
    scenario = _ScenarioManager._create(scenario_config)
    _ScenarioManager._submit(scenario)

    assert len(_DataManager._get_all()) == 4
    assert len(_TaskManager._get_all()) == 2
    assert len(_PipelineManager._get_all()) == 2
    assert len(_ScenarioManager._get_all()) == 2
    assert len(_CycleManager._get_all()) == 1
    assert len(_JobManager._get_all()) == 2


def test_override_experiment_version():
    scenario_config = config_scenario()

    core = Core()

    with patch("sys.argv", ["prog", "--experiment", "--version-number", "2.1"]):
        core.run()
    ver_1 = _VersionManager._get_latest_version()
    assert ver_1 == "2.1"
    # When create new experiment version, a development version entity is also created as a placeholder
    assert len(_VersionManager._get_all()) == 2  # 2 version include 1 experiment 1 development

    scenario = _ScenarioManager._create(scenario_config)
    _ScenarioManager._submit(scenario)

    assert len(_DataManager._get_all()) == 2
    assert len(_TaskManager._get_all()) == 1
    assert len(_PipelineManager._get_all()) == 1
    assert len(_ScenarioManager._get_all()) == 1
    assert len(_CycleManager._get_all()) == 1
    assert len(_JobManager._get_all()) == 1

    # Update Config singleton to simulate conflict Config between versions
    Config.unblock_update()
    Config.configure_global_app(clean_entities_enabled=True)

    # Without --override parameter, a SystemExit will be raised
    with pytest.raises(SystemExit):
        with patch("sys.argv", ["prog", "--experiment", "--version-number", "2.1"]):
            core.run()

    # With --override parameter
    with patch("sys.argv", ["prog", "--experiment", "--version-number", "2.1", "--override"]):
        core.run()
    ver_2 = _VersionManager._get_latest_version()
    assert ver_2 == "2.1"
    assert len(_VersionManager._get_all()) == 2  # 2 version include 1 experiment 1 development

    # All entities from previous submit should be saved
    scenario = _ScenarioManager._create(scenario_config)
    _ScenarioManager._submit(scenario)

    assert len(_DataManager._get_all()) == 4
    assert len(_TaskManager._get_all()) == 2
    assert len(_PipelineManager._get_all()) == 2
    assert len(_ScenarioManager._get_all()) == 2
    assert len(_CycleManager._get_all()) == 1
    assert len(_JobManager._get_all()) == 2


def test_delete_version():
    scenario_config = config_scenario()

    core = Core()

    with patch("sys.argv", ["prog", "--development"]):
        core.run()
    scenario = _ScenarioManager._create(scenario_config)
    _ScenarioManager._submit(scenario)

    with patch("sys.argv", ["prog", "--experiment", "--version-number", "1.0"]):
        core.run()
    scenario = _ScenarioManager._create(scenario_config)
    _ScenarioManager._submit(scenario)

    with patch("sys.argv", ["prog", "--experiment", "--version-number", "1.1"]):
        core.run()
    scenario = _ScenarioManager._create(scenario_config)
    _ScenarioManager._submit(scenario)

    with patch("sys.argv", ["prog", "--production", "--version-number", "1.1"]):
        core.run()

    with patch("sys.argv", ["prog", "--experiment", "--version-number", "2.0"]):
        core.run()
    scenario = _ScenarioManager._create(scenario_config)
    _ScenarioManager._submit(scenario)

    with patch("sys.argv", ["prog", "--experiment", "--version-number", "2.1"]):
        core.run()
    scenario = _ScenarioManager._create(scenario_config)
    _ScenarioManager._submit(scenario)

    with patch("sys.argv", ["prog", "--production", "--version-number", "2.1"]):
        core.run()

    all_versions = [version.id for version in _VersionManager._get_all()]
    production_version = _VersionManager._get_production_version()
    assert len(all_versions) == 5
    assert len(production_version) == 2
    assert "1.0" in all_versions
    assert "1.1" in all_versions and "1.1" in production_version
    assert "2.0" in all_versions
    assert "2.1" in all_versions and "2.1" in production_version

    with pytest.raises(SystemExit) as e:
        with patch("sys.argv", ["prog", "--delete-version", "1.0"]):
            core.run()
    assert str(e.value) == "Successfully delete version 1.0."
    all_versions = [version.id for version in _VersionManager._get_all()]
    assert len(all_versions) == 4
    assert "1.0" not in all_versions

    # Test delete production version will change the version from production to experiment
    with pytest.raises(SystemExit) as e:
        with patch("sys.argv", ["prog", "--delete-production-version", "1.1"]):
            core.run()
    assert str(e.value) == "Successfully delete version 1.1 from production version list."
    all_versions = [version.id for version in _VersionManager._get_all()]
    production_version = _VersionManager._get_production_version()
    assert len(all_versions) == 4
    assert "1.1" in all_versions and "1.1" not in production_version


def test_compare_config_between_versions():
    scenario_config = config_scenario()

    core = Core()
    with patch("sys.argv", ["prog", "--experiment", "--version-number", "1.0"]):
        core.run()
        scenario = _ScenarioManager._create(scenario_config)
        _ScenarioManager._submit(scenario)

    reset_configuration_singleton()

    scenario_config_2 = config_scenario_2()

    with pytest.raises(SystemExit) as e:
        with patch("sys.argv", ["prog", "--experiment", "--version-number", "1.0"]):
            core.run()
            scenario = _ScenarioManager._create(scenario_config_2)
            _ScenarioManager._submit(scenario)
    error_message = str(e.value)
    assert 'DATA_NODE "d3" was added' in error_message
    assert 'DATA_NODE "d1" was removed' in error_message
    assert 'PIPELINE "my_pipeline_2" was removed' in error_message
    assert 'DATA_NODE "d2" has attribute "default_path" modified' in error_message
    assert 'Global Configuration "root_folder" was modified' in error_message
    assert 'Global Configuration "clean_entities_enabled" was modified' in error_message
    assert 'Global Configuration "repository_type" was modified' in error_message
    assert 'JOB "mode" was modified' in error_message
    assert 'JOB "max_nb_of_workers" was modified' in error_message
    assert 'TASK "my_task" has attribute "inputs" modified' in error_message
    assert 'TASK "my_task" has attribute "function" modified' in error_message
    assert 'TASK "my_task" has attribute "outputs" modified' in error_message

    # TODO: These should not in ["added_items"] since it is a default value that was changed. It should be fixed soon.
    assert 'DATA_NODE "d2" has attribute "has_header" added' in error_message
    assert 'DATA_NODE "d2" has attribute "exposed_type" added' in error_message
    assert 'Global Configuration "repository_properties" was added' in error_message


def twice(a):
    return a * 2


def config_scenario():
    data_node_1_config = Config.configure_data_node(
        id="d1", storage_type="in_memory", default_data="abc", scope=Scope.SCENARIO
    )
    data_node_2_config = Config.configure_data_node(id="d2", storage_type="csv", default_path="foo.csv")
    task_config = Config.configure_task("my_task", twice, data_node_1_config, data_node_2_config)
    pipeline_config = Config.configure_pipeline("my_pipeline", task_config)
    pipeline_config_2 = Config.configure_pipeline("my_pipeline_2", task_config)
    scenario_config = Config.configure_scenario("my_scenario", pipeline_config, frequency=Frequency.DAILY)

    return scenario_config


def triple(a):
    return a * 3


def config_scenario_2():
    Config.configure_global_app(
        root_folder="foo_root",
        # Chaging the "storage_folder" will fail since older versions are stored in older folder
        # storage_folder="foo_storage",
        clean_entities_enabled=True,
        repository_type="bar",
        repository_properties={"foo": "bar"},
    )
    Config.configure_job_executions(mode="standalone", max_nb_of_workers=5)
    data_node_2_config = Config.configure_data_node(
        id="d2", storage_type="csv", default_path="bar.csv", has_header=False, exposed_type="numpy"
    )
    data_node_3_config = Config.configure_data_node(
        id="d3", storage_type="csv", default_path="baz.csv", has_header=False, exposed_type="numpy"
    )
    task_config = Config.configure_task("my_task", triple, data_node_2_config, data_node_3_config)
    pipeline_config = Config.configure_pipeline("my_pipeline", task_config)
    scenario_config = Config.configure_scenario("my_scenario", pipeline_config, frequency=Frequency.DAILY)

    return scenario_config
