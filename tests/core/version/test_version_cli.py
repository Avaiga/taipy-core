# Copyright 2021-2024 Avaiga Private Limited
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

from time import sleep
from unittest.mock import patch

import pytest

from src.taipy.core import Core
from src.taipy.core._version._cli._version_cli import _VersionCLI
from src.taipy.core._version._version_manager import _VersionManager
from src.taipy.core.data._data_manager import _DataManager
from src.taipy.core.job._job_manager import _JobManager
from src.taipy.core.scenario._scenario_manager import _ScenarioManager
from src.taipy.core.sequence._sequence_manager import _SequenceManager
from src.taipy.core.task._task_manager import _TaskManager
from taipy.config.common.frequency import Frequency
from taipy.config.common.scope import Scope
from taipy.config.config import Config

from ...conftest import init_config


def test_delete_version(caplog):
    scenario_config = config_scenario()

    with patch("sys.argv", ["prog", "--development"]):
        core = Core()
        core.run()
        scenario = _ScenarioManager._create(scenario_config)
        _ScenarioManager._submit(scenario)
        core.stop()

    with patch("sys.argv", ["prog", "--experiment", "1.0"]):
        core = Core()
        core.run()
        scenario = _ScenarioManager._create(scenario_config)
        _ScenarioManager._submit(scenario)
        core.stop()

    with patch("sys.argv", ["prog", "--experiment", "1.1"]):
        core = Core()
        core.run()
        scenario = _ScenarioManager._create(scenario_config)
        _ScenarioManager._submit(scenario)
        core.stop()

    with patch("sys.argv", ["prog", "--production", "1.1"]):
        core = Core()
        core.run()
        core.stop()

    with patch("sys.argv", ["prog", "--experiment", "2.0"]):
        core = Core()
        core.run()
        scenario = _ScenarioManager._create(scenario_config)
        _ScenarioManager._submit(scenario)
        core.stop()

    with patch("sys.argv", ["prog", "--experiment", "2.1"]):
        core = Core()
        core.run()
        scenario = _ScenarioManager._create(scenario_config)
        _ScenarioManager._submit(scenario)
        core.stop()

    with patch("sys.argv", ["prog", "--production", "2.1"]):
        core = Core()
        core.run()
        core.stop()

    all_versions = [version.id for version in _VersionManager._get_all()]
    production_version = _VersionManager._get_production_versions()
    assert len(all_versions) == 5
    assert len(production_version) == 2
    assert "1.0" in all_versions
    assert "1.1" in all_versions and "1.1" in production_version
    assert "2.0" in all_versions
    assert "2.1" in all_versions and "2.1" in production_version

    _VersionCLI.create_parser()
    with pytest.raises(SystemExit):
        with patch("sys.argv", ["prog", "manage-versions", "--delete", "1.0"]):
            _VersionCLI.parse_arguments()

    assert "Successfully delete version 1.0." in caplog.text
    all_versions = [version.id for version in _VersionManager._get_all()]
    assert len(all_versions) == 4
    assert "1.0" not in all_versions

    # Test delete a non-existed version
    with pytest.raises(SystemExit):
        with patch("sys.argv", ["prog", "manage-versions", "--delete", "non_exist_version"]):
            _VersionCLI.parse_arguments()
    assert "Version 'non_exist_version' does not exist." in caplog.text

    # Test delete production version will change the version from production to experiment
    with pytest.raises(SystemExit):
        with patch("sys.argv", ["prog", "manage-versions", "--delete-production", "1.1"]):
            _VersionCLI.parse_arguments()

    assert "Successfully delete version 1.1 from the production version list." in caplog.text
    all_versions = [version.id for version in _VersionManager._get_all()]
    production_version = _VersionManager._get_production_versions()
    assert len(all_versions) == 4
    assert "1.1" in all_versions and "1.1" not in production_version

    # Test delete a non-existed production version
    with pytest.raises(SystemExit) as e:
        with patch("sys.argv", ["prog", "manage-versions", "--delete-production", "non_exist_version"]):
            _VersionCLI.parse_arguments()

    assert str(e.value) == "Version 'non_exist_version' is not a production version."


def test_list_versions(capsys):
    with patch("sys.argv", ["prog", "--development"]):
        core = Core()
        core.run()
        core.stop()
    sleep(0.05)
    with patch("sys.argv", ["prog", "--experiment", "1.0"]):
        core = Core()
        core.run()
        core.stop()
    sleep(0.05)
    with patch("sys.argv", ["prog", "--experiment", "1.1"]):
        core = Core()
        core.run()
        core.stop()
    sleep(0.05)
    with patch("sys.argv", ["prog", "--production", "1.1"]):
        core = Core()
        core.run()
        core.stop()
    sleep(0.05)
    with patch("sys.argv", ["prog", "--experiment", "2.0"]):
        core = Core()
        core.run()
        core.stop()
    sleep(0.05)
    with patch("sys.argv", ["prog", "--experiment", "2.1"]):
        core = Core()
        core.run()
        core.stop()
    sleep(0.05)
    with patch("sys.argv", ["prog", "--production", "2.1"]):
        core = Core()
        core.run()
        core.stop()

    _VersionCLI.create_parser()
    with pytest.raises(SystemExit):
        with patch("sys.argv", ["prog", "manage-versions", "--list"]):
            _VersionCLI.parse_arguments()

    out, _ = capsys.readouterr()
    version_list = str(out).strip().split("\n")
    assert len(version_list) == 6  # 5 versions with the header
    assert all(column in version_list[0] for column in ["Version number", "Mode", "Creation date"])
    assert all(column in version_list[1] for column in ["2.1", "Production", "latest"])
    assert all(column in version_list[2] for column in ["2.0", "Experiment"]) and "latest" not in version_list[2]
    assert all(column in version_list[3] for column in ["1.1", "Production"]) and "latest" not in version_list[3]
    assert all(column in version_list[4] for column in ["1.0", "Experiment"]) and "latest" not in version_list[4]
    assert "Development" in version_list[5] and "latest" not in version_list[5]


def test_rename_version(caplog):
    scenario_config = config_scenario()

    with patch("sys.argv", ["prog", "--experiment", "1.0"]):
        core = Core()
        core.run()
        scenario = _ScenarioManager._create(scenario_config)
        _ScenarioManager._submit(scenario)
        core.stop()

    with patch("sys.argv", ["prog", "--production", "2.0"]):
        core = Core()
        core.run()
        scenario = _ScenarioManager._create(scenario_config)
        _ScenarioManager._submit(scenario)
        core.stop()

    dev_ver = _VersionManager._get_development_version()

    _VersionCLI.create_parser()
    with pytest.raises(SystemExit):
        with patch("sys.argv", ["prog", "manage-versions", "--rename", "non_exist_version", "1.1"]):
            # This should raise an exception since version "non_exist_version" does not exist
            _VersionCLI.parse_arguments()
    assert "Version 'non_exist_version' does not exist." in caplog.text

    _VersionCLI.create_parser()
    with pytest.raises(SystemExit):
        with patch("sys.argv", ["prog", "manage-versions", "--rename", "1.0", "2.0"]):
            # This should raise an exception since 2.0 already exists
            _VersionCLI.parse_arguments()
    assert "Version name '2.0' is already used." in caplog.text

    _VersionCLI.create_parser()
    with pytest.raises(SystemExit):
        with patch("sys.argv", ["prog", "manage-versions", "--rename", "1.0", "1.1"]):
            _VersionCLI.parse_arguments()
    assert _VersionManager._get("1.0") is None
    assert [version.id for version in _VersionManager._get_all()].sort() == [dev_ver, "1.1", "2.0"].sort()
    # All entities are assigned to the new version
    assert len(_DataManager._get_all("1.1")) == 2
    assert len(_TaskManager._get_all("1.1")) == 1
    assert len(_SequenceManager._get_all("1.1")) == 1
    assert len(_ScenarioManager._get_all("1.1")) == 1
    assert len(_JobManager._get_all("1.1")) == 1

    _VersionCLI.create_parser()
    with pytest.raises(SystemExit):
        with patch("sys.argv", ["prog", "manage-versions", "--rename", "2.0", "2.1"]):
            _VersionCLI.parse_arguments()
    assert _VersionManager._get("2.0") is None
    assert [version.id for version in _VersionManager._get_all()].sort() == [dev_ver, "1.1", "2.1"].sort()
    assert _VersionManager._get_production_versions() == ["2.1"]
    # All entities are assigned to the new version
    assert len(_DataManager._get_all("2.1")) == 2
    assert len(_TaskManager._get_all("2.1")) == 1
    assert len(_SequenceManager._get_all("2.1")) == 1
    assert len(_ScenarioManager._get_all("2.1")) == 1
    assert len(_JobManager._get_all("2.1")) == 1


def test_compare_version_config(caplog):
    scenario_config_1 = config_scenario()

    with patch("sys.argv", ["prog", "--experiment", "1.0"]):
        core = Core()
        core.run()
        scenario = _ScenarioManager._create(scenario_config_1)
        _ScenarioManager._submit(scenario)
        core.stop()

    init_config()

    scenario_config_2 = config_scenario()
    Config.configure_data_node(id="d2", storage_type="csv", default_path="bar.csv")

    with patch("sys.argv", ["prog", "--experiment", "2.0"]):
        core = Core()
        core.run()
        scenario = _ScenarioManager._create(scenario_config_2)
        _ScenarioManager._submit(scenario)
        core.stop()

    _VersionCLI.create_parser()
    with pytest.raises(SystemExit):
        with patch("sys.argv", ["prog", "manage-versions", "--compare-config", "non_exist_version", "2.0"]):
            # This should raise an exception since version "non_exist_version" does not exist
            _VersionCLI.parse_arguments()
    assert "Version 'non_exist_version' does not exist." in caplog.text

    with pytest.raises(SystemExit):
        with patch("sys.argv", ["prog", "manage-versions", "--compare-config", "1.0", "non_exist_version"]):
            # This should raise an exception since 2.0 already exists
            _VersionCLI.parse_arguments()
    assert "Version 'non_exist_version' does not exist." in caplog.text

    with pytest.raises(SystemExit):
        with patch("sys.argv", ["prog", "manage-versions", "--compare-config", "1.0", "1.0"]):
            _VersionCLI.parse_arguments()
    assert "There is no difference between version 1.0 Configuration and version 1.0 Configuration." in caplog.text

    with pytest.raises(SystemExit):
        with patch("sys.argv", ["prog", "manage-versions", "--compare-config", "1.0", "2.0"]):
            _VersionCLI.parse_arguments()
    expected_message = """Differences between version 1.0 Configuration and version 2.0 Configuration:
\tDATA_NODE "d2" has attribute "default_path" modified: foo.csv -> bar.csv"""
    assert expected_message in caplog.text


def twice(a):
    return a * 2


def config_scenario():
    data_node_1_config = Config.configure_data_node(
        id="d1", storage_type="pickle", default_data="abc", scope=Scope.SCENARIO
    )
    data_node_2_config = Config.configure_data_node(id="d2", storage_type="csv", default_path="foo.csv")
    task_config = Config.configure_task("my_task", twice, data_node_1_config, data_node_2_config)
    scenario_config = Config.configure_scenario("my_scenario", [task_config], frequency=Frequency.DAILY)
    scenario_config.add_sequences({"my_sequence": [task_config]})

    return scenario_config
