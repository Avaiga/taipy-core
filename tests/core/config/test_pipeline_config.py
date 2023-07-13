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

import os
from unittest import mock

import pytest

from taipy.config.config import Config


def test_pipeline_config_creation():
    task1_config = Config.configure_task("task1", print, [], [])
    task2_config = Config.configure_task("task2", print, [], [])
    pipeline_config = Config.configure_pipeline("pipelines1", [task1_config, task2_config])

    assert list(Config.pipelines) == ["default", pipeline_config.id]

    pipeline2_config = Config.configure_pipeline("pipelines2", [task1_config, task2_config])
    assert list(Config.pipelines) == ["default", pipeline_config.id, pipeline2_config.id]


def test_pipeline_count():
    task1_config = Config.configure_task("task1", print, [], [])
    task2_config = Config.configure_task("task2", print, [], [])
    Config.configure_pipeline("pipelines1", [task1_config, task2_config])
    assert len(Config.pipelines) == 2

    Config.configure_pipeline("pipelines2", [task1_config, task2_config])
    assert len(Config.pipelines) == 3

    Config.configure_pipeline("pipelines3", [task1_config, task2_config])
    assert len(Config.pipelines) == 4


def test_pipeline_getitem():
    task1_config = Config.configure_task("task1", print, [], [])
    task2_config = Config.configure_task("task2", print, [], [])
    pipeline_config_id = "pipelines1"
    pipeline = Config.configure_pipeline(pipeline_config_id, [task1_config, task2_config])

    assert Config.pipelines[pipeline_config_id].id == pipeline.id
    assert Config.pipelines[pipeline_config_id]._tasks == pipeline._tasks
    assert Config.pipelines[pipeline_config_id].properties == pipeline.properties


def test_pipeline_creation_no_duplication():
    task1_config = Config.configure_task("task1", print, [], [])
    task2_config = Config.configure_task("task2", print, [], [])
    Config.configure_pipeline("pipelines1", [task1_config, task2_config])

    assert len(Config.pipelines) == 2

    Config.configure_pipeline("pipelines1", [task1_config, task2_config])
    assert len(Config.pipelines) == 2


def test_pipeline_config_with_env_variable_value():
    task1_config = Config.configure_task("task1", print, [], [])
    task2_config = Config.configure_task("task2", print, [], [])
    with mock.patch.dict(os.environ, {"FOO": "bar"}):
        Config.configure_pipeline("pipeline_name", [task1_config, task2_config], prop="ENV[FOO]")
        assert Config.pipelines["pipeline_name"].prop == "bar"
        assert Config.pipelines["pipeline_name"].properties["prop"] == "bar"
        assert Config.pipelines["pipeline_name"]._properties["prop"] == "ENV[FOO]"


def test_clean_config():
    task1_config = Config.configure_task("task1", print, [], [])
    task2_config = Config.configure_task("task2", print, [], [])
    pipeline1_config = Config.configure_pipeline("id1", [task1_config, task2_config])
    pipeline2_config = Config.configure_pipeline("id2", [task2_config, task1_config])

    assert Config.pipelines["id1"] is pipeline1_config
    assert Config.pipelines["id2"] is pipeline2_config

    pipeline1_config._clean()
    pipeline2_config._clean()

    # Check if the instance before and after _clean() is the same
    assert Config.pipelines["id1"] is pipeline1_config
    assert Config.pipelines["id2"] is pipeline2_config

    assert pipeline1_config.id == "id1"
    assert pipeline2_config.id == "id2"
    assert pipeline1_config.tasks == pipeline2_config.tasks == []
    assert pipeline1_config.properties == pipeline2_config.properties == {}


def test_pipeline_config_configure_deprecated():
    with pytest.warns(DeprecationWarning):
        Config.configure_pipeline("pipeline_id", [])

    with pytest.warns(DeprecationWarning):
        Config.set_default_pipeline_configuration([])
