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

from copy import copy

import pytest

from src.taipy.core.config.checkers._data_node_config_checker import _DataNodeConfigChecker
from src.taipy.core.config.data_node_config import DataNodeConfig
from taipy.config import Config
from taipy.config.checker.issue_collector import IssueCollector
from taipy.config.common.scope import Scope


class MyCustomClass:
    pass


class TestDataNodeConfigChecker:
    def test_check_config_id(self):
        collector = IssueCollector()
        config = Config._default_config
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["new"] = copy(config._sections[DataNodeConfig.name]["default"])
        config._sections[DataNodeConfig.name]["new"].id = None
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 1

        config._sections[DataNodeConfig.name]["new"].id = "new"
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

    def test_check_storage_type(self):
        collector = IssueCollector()
        config = Config._default_config
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "bar"
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 1

        config._sections[DataNodeConfig.name]["default"].storage_type = "csv"
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "excel"
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "pickle"
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "in_memory"
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 2

        config._sections[DataNodeConfig.name]["default"].storage_type = "sql_table"
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 5

        config._sections[DataNodeConfig.name]["default"].storage_type = "sql"
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 6

        config._sections[DataNodeConfig.name]["default"].storage_type = "json"
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 1

    def test_check_scope(self):
        config = Config._default_config

        config._sections[DataNodeConfig.name]["default"].scope = "bar"
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 1

        config._sections[DataNodeConfig.name]["default"].scope = 1
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 1

        config._sections[DataNodeConfig.name]["default"].scope = Scope.GLOBAL
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].scope = Scope.CYCLE
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].scope = Scope.SCENARIO
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].scope = Scope.PIPELINE
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

    def test_check_required_properties(self):
        config = Config._default_config

        config._sections[DataNodeConfig.name]["default"].storage_type = "csv"
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "csv"
        config._sections[DataNodeConfig.name]["default"].properties = {"has_header": True}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "csv"
        config._sections[DataNodeConfig.name]["default"].properties = {"path": "bar"}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "sql_table"
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 5

        config._sections[DataNodeConfig.name]["default"].storage_type = "sql"
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 6

        required_properties = ["db_username", "db_password", "db_name", "db_engine", "table_name"]
        config._sections[DataNodeConfig.name]["default"].storage_type = "sql_table"
        config._sections[DataNodeConfig.name]["default"].properties = {key: f"the_{key}" for key in required_properties}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        required_properties = [
            "db_username",
            "db_password",
            "db_name",
            "db_engine",
            "read_query",
            "write_query_builder",
        ]
        config._sections[DataNodeConfig.name]["default"].storage_type = "sql"
        config._sections[DataNodeConfig.name]["default"].properties = {key: f"the_{key}" for key in required_properties}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "excel"
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "excel"
        config._sections[DataNodeConfig.name]["default"].properties = {"has_header": True}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "excel"
        config._sections[DataNodeConfig.name]["default"].properties = {"path": "bar"}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "excel"
        config._sections[DataNodeConfig.name]["default"].properties = {
            "has_header": True,
            "path": "bar",
            "sheet_name": ["sheet_name_1", "sheet_name_2"],
        }
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 2

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"read_fct": print}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 1

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"write_fct": print}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 1

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"write_fct": print, "read_fct": print}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "json"
        config._sections[DataNodeConfig.name]["default"].properties = {"default_path": "bar"}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

    def test_check_read_write_fct(self):
        config = Config._default_config

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"write_fct": 12}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 2

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"read_fct": 5}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 2

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"write_fct": 9, "read_fct": 5}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 2

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"write_fct": print, "read_fct": 5}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 1

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"write_fct": 5, "read_fct": print}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 1

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"write_fct": print, "read_fct": print}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

    def test_check_read_write_fct_params(self):
        config = Config._default_config

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"write_fct": print, "read_fct": print}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {
            "write_fct": print,
            "read_fct": print,
            "write_fct_params": [],
        }
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 1

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {
            "write_fct": print,
            "read_fct": print,
            "write_fct_params": tuple("foo"),
        }
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {
            "write_fct": print,
            "read_fct": print,
            "read_fct_params": [],
        }
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 1

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {
            "write_fct": print,
            "read_fct": print,
            "read_fct_params": tuple("foo"),
        }
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {
            "write_fct": print,
            "read_fct": print,
            "write_fct_params": tuple("foo"),
            "read_fct_params": tuple("foo"),
        }
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

    def test_check_exposed_types(self):
        config = Config._default_config

        config._sections[DataNodeConfig.name]["default"].storage_type = "csv"
        config._sections[DataNodeConfig.name]["default"].properties = {"exposed_type": "foo"}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 1

        config._sections[DataNodeConfig.name]["default"].properties = {"exposed_type": "pandas"}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].properties = {"exposed_type": "modin"}
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].properties = {"exposed_type": "numpy"}
        collector = IssueCollector()
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].properties = {"exposed_type": MyCustomClass}
        _DataNodeConfigChecker(config, collector)._check()
        assert len(collector.errors) == 0
