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

from src.taipy.core.config.data_node_config import DataNodeConfig
from taipy.config import Config
from taipy.config.checker.issue_collector import IssueCollector
from taipy.config.common.scope import Scope


class MyCustomClass:
    pass


class TestDataNodeConfigChecker:
    def test_check_config_id(self, caplog):
        Config._collector = IssueCollector()
        config = Config._default_config
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["new"] = copy(config._sections[DataNodeConfig.name]["default"])
        config._sections[DataNodeConfig.name]["new"].id = None
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 1
        assert "config_id of DataNodeConfig `None` is empty." in caplog.text

        config._sections[DataNodeConfig.name]["new"].id = "new"
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

    def test_check_if_entity_property_key_used_is_predefined(self, caplog):
        Config._collector = IssueCollector()
        config = Config._default_config
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["new"] = copy(config._sections[DataNodeConfig.name]["default"])
        config._sections[DataNodeConfig.name]["new"]._properties["_entity_owner"] = None
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 1
        assert "Properties of DataNodeConfig `default` cannot have `_entity_owner` as its property." in caplog.text

        config._sections[DataNodeConfig.name]["new"] = copy(config._sections[DataNodeConfig.name]["default"])
        config._sections[DataNodeConfig.name]["new"]._properties["_entity_owner"] = "entity_owner"
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 1
        expected_error_message = (
            "Properties of DataNodeConfig `default` cannot have `_entity_owner` as its property."
            ' Current value of property `_entity_owner` is "entity_owner".'
        )
        assert expected_error_message in caplog.text

    def test_check_storage_type(self, caplog):
        Config._collector = IssueCollector()
        config = Config._default_config
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "bar"
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 1
        expected_error_message = (
            "`storage_type` field of DataNodeConfig `default` must be either csv, sql_table,"
            " sql, mongo_collection, pickle, excel, generic, json, parquet, or in_memory."
            ' Current value of property `storage_type` is "bar".'
        )
        assert expected_error_message in caplog.text

        config._sections[DataNodeConfig.name]["default"].storage_type = "csv"
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "excel"
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "pickle"
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "json"
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "parquet"
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "in_memory"
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 2
        expected_error_messages = [
            "DataNodeConfig `default` is missing the required property `read_fct` for type `generic`.",
            "DataNodeConfig `default` is missing the required property `write_fct` for type `generic`.",
        ]
        assert all(message in caplog.text for message in expected_error_messages)

        config._sections[DataNodeConfig.name]["default"].storage_type = "sql_table"
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 5
        expected_error_messages = [
            "DataNodeConfig `default` is missing the required property `db_username` for type `sql_table`.",
            "DataNodeConfig `default` is missing the required property `db_password` for type `sql_table`.",
            "DataNodeConfig `default` is missing the required property `db_name` for type `sql_table`.",
            "DataNodeConfig `default` is missing the required property `db_engine` for type `sql_table`.",
            "DataNodeConfig `default` is missing the required property `table_name` for type `sql_table`.",
        ]
        assert all(message in caplog.text for message in expected_error_messages)

        config._sections[DataNodeConfig.name]["default"].storage_type = "sql"
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 6
        expected_error_messages = [
            "DataNodeConfig `default` is missing the required property `db_username` for type `sql`.",
            "DataNodeConfig `default` is missing the required property `db_password` for type `sql`.",
            "DataNodeConfig `default` is missing the required property `db_name` for type `sql`.",
            "DataNodeConfig `default` is missing the required property `db_engine` for type `sql`.",
            "DataNodeConfig `default` is missing the required property `read_query` for type `sql`.",
            "DataNodeConfig `default` is missing the required property `write_query_builder` for type `sql`.",
        ]
        assert all(message in caplog.text for message in expected_error_messages)

        config._sections[DataNodeConfig.name]["default"].storage_type = "mongo_collection"
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 2
        expected_error_messages = [
            "DataNodeConfig `default` is missing the required property `db_name` for type `mongo_collection`.",
            "DataNodeConfig `default` is missing the required property `collection_name` for type `mongo_collection`.",
        ]
        assert all(message in caplog.text for message in expected_error_messages)

    def test_check_scope(self, caplog):
        config = Config._default_config

        config._sections[DataNodeConfig.name]["default"].scope = "bar"
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 1
        expected_error_message = (
            "`scope` field of DataNodeConfig `default` must be populated with a Scope"
            ' value. Current value of property `scope` is "bar".'
        )
        assert expected_error_message in caplog.text

        config._sections[DataNodeConfig.name]["default"].scope = 1
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 1
        expected_error_message = (
            "`scope` field of DataNodeConfig `default` must be populated with a Scope"
            " value. Current value of property `scope` is 1."
        )
        assert expected_error_message in caplog.text

        config._sections[DataNodeConfig.name]["default"].scope = Scope.GLOBAL
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].scope = Scope.CYCLE
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].scope = Scope.SCENARIO
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].scope = Scope.PIPELINE
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

    def test_check_required_properties(self, caplog):
        config = Config._default_config

        config._sections[DataNodeConfig.name]["default"].storage_type = "csv"
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "csv"
        config._sections[DataNodeConfig.name]["default"].properties = {"has_header": True}
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "csv"
        config._sections[DataNodeConfig.name]["default"].properties = {"path": "bar"}
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        required_properties = ["db_username", "db_password", "db_name", "db_engine", "table_name"]
        config._sections[DataNodeConfig.name]["default"].storage_type = "sql_table"
        config._sections[DataNodeConfig.name]["default"].properties = {key: f"the_{key}" for key in required_properties}
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

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
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "mongo_collection"
        config._sections[DataNodeConfig.name]["default"].properties = {"db_name": "foo", "collection_name": "bar"}
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "excel"
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "excel"
        config._sections[DataNodeConfig.name]["default"].properties = {"has_header": True}
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "excel"
        config._sections[DataNodeConfig.name]["default"].properties = {"path": "bar"}
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "excel"
        config._sections[DataNodeConfig.name]["default"].properties = {
            "has_header": True,
            "path": "bar",
            "sheet_name": ["sheet_name_1", "sheet_name_2"],
        }
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 2

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"read_fct": print}
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 1

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"write_fct": print}
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 1

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"write_fct": print, "read_fct": print}
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "json"
        config._sections[DataNodeConfig.name]["default"].properties = {"default_path": "bar"}
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

    def test_check_read_write_fct(self, caplog):
        config = Config._default_config

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"write_fct": 12}
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 2
        expected_error_messages = [
            "DataNodeConfig `default` is missing the required property `read_fct` for type `generic`.",
            "`write_fct` of DataNodeConfig `default` must be populated with a Callable function. Current value"
            " of property `write_fct` is 12.",
        ]
        assert all(message in caplog.text for message in expected_error_messages)

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"read_fct": 5}
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 2
        expected_error_messages = [
            "DataNodeConfig `default` is missing the required property `write_fct` for type `generic`.",
            "`read_fct` of DataNodeConfig `default` must be populated with a Callable function. Current value"
            " of property `read_fct` is 5.",
        ]

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"write_fct": 9, "read_fct": 5}
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 2
        expected_error_messages = [
            "`write_fct` of DataNodeConfig `default` must be populated with a Callable function. Current value"
            " of property `write_fct` is 9.",
            "`read_fct` of DataNodeConfig `default` must be populated with a Callable function. Current value"
            " of property `read_fct` is 5.",
        ]

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"write_fct": print, "read_fct": 5}
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 1

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"write_fct": 5, "read_fct": print}
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 1

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"write_fct": print, "read_fct": print}
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

    def test_check_read_write_fct_params(self, caplog):
        config = Config._default_config

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {"write_fct": print, "read_fct": print}
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {
            "write_fct": print,
            "read_fct": print,
            "write_fct_params": [],
        }
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 1
        expected_error_message = (
            "`write_fct_params` field of DataNodeConfig `default` must be populated with a" " Tuple value."
        )
        assert expected_error_message in caplog.text
        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {
            "write_fct": print,
            "read_fct": print,
            "write_fct_params": tuple("foo"),
        }
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {
            "write_fct": print,
            "read_fct": print,
            "read_fct_params": [],
        }
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 1
        expected_error_message = (
            "`read_fct_params` field of DataNodeConfig `default` must be populated with a" " Tuple value."
        )
        assert expected_error_message in caplog.text

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {
            "write_fct": print,
            "read_fct": print,
            "read_fct_params": tuple("foo"),
        }
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].storage_type = "generic"
        config._sections[DataNodeConfig.name]["default"].properties = {
            "write_fct": print,
            "read_fct": print,
            "write_fct_params": tuple("foo"),
            "read_fct_params": tuple("foo"),
        }
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

    def test_check_exposed_types(self, caplog):
        config = Config._default_config

        config._sections[DataNodeConfig.name]["default"].storage_type = "csv"
        config._sections[DataNodeConfig.name]["default"].properties = {"exposed_type": "foo"}
        with pytest.raises(SystemExit):
            Config._collector = IssueCollector()
            Config.check()
        assert len(Config._collector.errors) == 1
        expected_error_message = (
            'The `exposed_type` of DataNodeConfig `default` must be either "pandas", "modin"'
            ', "numpy", or a custom type. Current value of property `exposed_type` is "foo".'
        )
        assert expected_error_message in caplog.text

        config._sections[DataNodeConfig.name]["default"].properties = {"exposed_type": "pandas"}
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].properties = {"exposed_type": "numpy"}
        Config._collector = IssueCollector()
        Config.check()
        assert len(Config._collector.errors) == 0

        config._sections[DataNodeConfig.name]["default"].properties = {"exposed_type": MyCustomClass}
        Config.check()
        assert len(Config._collector.errors) == 0
