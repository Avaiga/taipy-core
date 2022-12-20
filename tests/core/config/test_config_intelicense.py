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

import pytest

from taipy.config import Config
from src.taipy.core.config import DataNodeConfig, JobConfig, PipelineConfig, ScenarioConfig, TaskConfig


def test_intelicense_match_documentation():
    assert Config.configure_data_node.__doc__ == DataNodeConfig._configure.__doc__
    assert Config.configure_csv_data_node.__doc__ == DataNodeConfig._configure_csv.__doc__
    assert Config.configure_default_data_node.__doc__ == DataNodeConfig._configure_default.__doc__
    assert Config.configure_excel_data_node.__doc__ == DataNodeConfig._configure_excel.__doc__
    assert Config.configure_generic_data_node.__doc__ == DataNodeConfig._configure_generic.__doc__
    assert Config.configure_in_memory_data_node.__doc__ == DataNodeConfig._configure_in_memory.__doc__
    assert Config.configure_mongo_collection_data_node.__doc__ == DataNodeConfig._configure_mongo_collection.__doc__
    assert Config.configure_parquet_data_node.__doc__ == DataNodeConfig._configure_parquet.__doc__
    assert Config.configure_json_data_node.__doc__ == DataNodeConfig._configure_json.__doc__
    assert Config.configure_pickle_data_node.__doc__ == DataNodeConfig._configure_pickle.__doc__
    assert Config.configure_sql_data_node.__doc__ == DataNodeConfig._configure_sql.__doc__
    assert Config.configure_sql_table_data_node.__doc__ == DataNodeConfig._configure_sql_table.__doc__
    assert Config.configure_pipeline.__doc__ == PipelineConfig._configure.__doc__
    assert Config.configure_default_pipeline.__doc__ == PipelineConfig._configure_default.__doc__
    assert Config.configure_scenario.__doc__ == ScenarioConfig._configure.__doc__
    assert Config.configure_scenario_from_tasks.__doc__ == ScenarioConfig._configure_from_tasks.__doc__
    assert Config.configure_task.__doc__ == TaskConfig._configure.__doc__
    assert Config.configure_job_executions.__doc__ == JobConfig._configure.__doc__


@pytest.mark.xfail
def test_intelicense_match_documentation_fail():
    DataNodeConfig._configure.__doc__ = "This is the new documentation"
    assert Config.configure_data_node.__doc__ == DataNodeConfig._configure.__doc__
