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

import os
import pathlib
import shutil
from unittest.mock import patch

from src.taipy.core import Core, taipy
from src.taipy.core._version._version_manager import _VersionManager
from taipy import Config, Frequency


def test_experiment_mode_converts_old_entities_to_latest_version():
    with patch("sys.argv", ["prog", "--experiment", "1.0"]):
        config_scenario()
        shutil.rmtree(Config.global_config.storage_folder, ignore_errors=True)
        data_set_path = os.path.join(pathlib.Path(__file__).parent.resolve(), "dataset_2.0/")
        shutil.copytree(data_set_path, Config.global_config.storage_folder)

        Core().run()
        scenario = taipy.get("SCENARIO_my_scenario_c4307ae8-d2ce-4802-8b16-8307baa7cff1")
        assert _VersionManager._get_development_version() != "1.0"
        assert "1.0" not in _VersionManager._get_production_version()
        assert scenario.version == "1.0"
        taipy.set(scenario)
        assert scenario.my_pipeline.version == "1.0"
        taipy.set(scenario.my_pipeline)
        assert scenario.my_pipeline.my_task.version == "1.0"
        taipy.set(scenario.my_pipeline.my_task)
        assert scenario.d1.version == "1.0"
        taipy.set(scenario.d1)
        assert scenario.d2.version == "1.0"
        taipy.set(scenario.d2)


def test_production_mode_converts_old_entities_to_latest_version():
    with patch("sys.argv", ["prog", "--production", "1.0"]):
        config_scenario()
        shutil.rmtree(Config.global_config.storage_folder, ignore_errors=True)
        data_set_path = os.path.join(pathlib.Path(__file__).parent.resolve(), "dataset_2.0/")
        shutil.copytree(data_set_path, Config.global_config.storage_folder)

        Core().run()
        scenario = taipy.get("SCENARIO_my_scenario_c4307ae8-d2ce-4802-8b16-8307baa7cff1")
        assert _VersionManager._get_development_version() != "1.0"
        assert "1.0" in _VersionManager._get_production_version()
        assert scenario.version == "1.0"
        taipy.set(scenario)
        assert scenario.my_pipeline.version == "1.0"
        taipy.set(scenario.my_pipeline)
        assert scenario.my_pipeline.my_task.version == "1.0"
        taipy.set(scenario.my_pipeline.my_task)
        assert scenario.d1.version == "1.0"
        taipy.set(scenario.d1)
        assert scenario.d2.version == "1.0"
        taipy.set(scenario.d2)


def test_development_mode_converts_old_entities_to_latest_version():
    with patch("sys.argv", ["prog", "--development", "1.0"]):
        config_scenario()
        shutil.rmtree(Config.global_config.storage_folder, ignore_errors=True)
        data_set_path = os.path.join(pathlib.Path(__file__).parent.resolve(), "dataset_2.0/")
        shutil.copytree(data_set_path, Config.global_config.storage_folder)

        Core().run()
        scenario = taipy.get("SCENARIO_my_scenario_c4307ae8-d2ce-4802-8b16-8307baa7cff1")
        version = "legacy-version"
        assert _VersionManager._get_development_version() != version
        assert version not in _VersionManager._get_production_version()
        assert scenario.version == version
        taipy.set(scenario)
        assert scenario.my_pipeline.version == version
        taipy.set(scenario.my_pipeline)
        assert scenario.my_pipeline.my_task.version == version
        taipy.set(scenario.my_pipeline.my_task)
        assert scenario.d1.version == version
        taipy.set(scenario.d1)
        assert scenario.d2.version == version
        taipy.set(scenario.d2)


def twice(a):
    return a * 2


def config_scenario():
    data_node_1_config = Config.configure_data_node(id="d1", storage_type="in_memory", default_data="abc")
    data_node_2_config = Config.configure_data_node(id="d2", storage_type="csv", default_path="foo.csv")
    task_config = Config.configure_task("my_task", twice, data_node_1_config, data_node_2_config)
    pipeline_config = Config.configure_pipeline("my_pipeline", task_config)
    scenario_config = Config.configure_scenario("my_scenario", pipeline_config, frequency=Frequency.DAILY)
    return scenario_config
