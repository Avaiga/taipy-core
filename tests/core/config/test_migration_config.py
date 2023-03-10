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

import pytest

from src.taipy.core._version._version_manager import _VersionManager
from src.taipy.core.exceptions.exceptions import NonExistingVersion
from taipy.config.config import Config


def migrate_pickle_path(dn):
    dn.path = "s1.pkl"


def migrate_skippable(task):
    task.skippable = True


def test_migration_config():
    latest_version = _VersionManager._get_latest_version()

    assert Config.migration_functions.migration_fcts == {}

    data_nodes1 = Config.configure_data_node("data_nodes1", "pickle")

    migration_cfg = Config.add_data_node_migration_function(
        source_version="latest",
        data_node_config=data_nodes1,
        migration_fct=migrate_pickle_path,
    )

    assert migration_cfg.migration_fcts == {latest_version: {"data_nodes1": migrate_pickle_path}}
    assert migration_cfg.properties == {}

    data_nodes2 = Config.configure_data_node("data_nodes2", "pickle")

    migration_cfg = Config.add_data_node_migration_function(
        source_version="latest",
        data_node_config=data_nodes2,
        migration_fct=migrate_pickle_path,
    )
    assert migration_cfg.migration_fcts == {
        latest_version: {"data_nodes1": migrate_pickle_path, "data_nodes2": migrate_pickle_path}
    }


def test_config_with_non_existing_version():
    data_nodes1 = Config.configure_data_node("data_nodes1", "pickle")
    task_1 = Config.configure_task("task_1", print, data_nodes1, data_nodes1)

    Config.add_data_node_migration_function("latest", data_nodes1, migrate_pickle_path)
    Config.add_task_migration_function("dev", task_1, migrate_skippable)

    with pytest.raises(NonExistingVersion):
        Config.add_data_node_migration_function("foo", data_nodes1, migrate_pickle_path)
    with pytest.raises(NonExistingVersion):
        Config.add_task_migration_function("bar", task_1, migrate_skippable)
