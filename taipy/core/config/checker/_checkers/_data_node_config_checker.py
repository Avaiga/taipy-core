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

from typing import Tuple

from taipy.core.common.scope import Scope
from taipy.core.config._config import _Config
from taipy.core.config.checker._checkers._config_checker import _ConfigChecker
from taipy.core.config.checker.issue_collector import IssueCollector
from taipy.core.config.data_node_config import DataNodeConfig


class _DataNodeConfigChecker(_ConfigChecker):
    def __init__(self, config: _Config, collector: IssueCollector):
        super().__init__(config, collector)
        from taipy.core.data.data_node import DataNode

        self._required_properties = {c.storage_type(): c._REQUIRED_PROPERTIES for c in DataNode.__subclasses__()}
        self._storage_types = [c.storage_type() for c in DataNode.__subclasses__()]

    def _check(self) -> IssueCollector:
        from taipy.core.data.generic import GenericDataNode

        data_node_configs = self._config._data_nodes
        for data_node_config_id, data_node_config in data_node_configs.items():
            self._check_existing_config_id(data_node_config)
            self._check_storage_type(data_node_config_id, data_node_config)
            self._check_scope(data_node_config_id, data_node_config)
            self._check_required_properties(data_node_config_id, data_node_config)
            if data_node_config.storage_type == GenericDataNode.storage_type():
                self._check_read_write_fct(data_node_config_id, data_node_config)
                self._check_read_write_fct_params(data_node_config_id, data_node_config)
        return self._collector

    def _check_storage_type(self, data_node_config_id: str, data_node_config: DataNodeConfig):
        if data_node_config.storage_type not in self._storage_types:
            self._error(
                data_node_config._STORAGE_TYPE_KEY,
                data_node_config.storage_type,
                f"{data_node_config._STORAGE_TYPE_KEY} field of DataNode {data_node_config_id} must be either csv, "
                f"sql, pickle, excel or in_memory.",
            )

    def _check_scope(self, data_node_config_id: str, data_node_config: DataNodeConfig):
        if not isinstance(data_node_config.scope, Scope):
            self._error(
                data_node_config._SCOPE_KEY,
                data_node_config.scope,
                f"{data_node_config._SCOPE_KEY} field of DataNode {data_node_config_id} must be populated with a Scope "
                f"value.",
            )

    def _check_read_write_fct_params(self, data_node_config_id: str, data_node_config: DataNodeConfig):

        from taipy.core.data.generic import GenericDataNode

        key_names = [
            GenericDataNode._READ_FUNCTION_PARAMS_PROPERTY,
            GenericDataNode._WRITE_FUNCTION_PARAMS_PROPERTY,
        ]
        for key_name in key_names:
            if key_name in data_node_config.properties.keys() and not isinstance(data_node_config.properties[key_name], Tuple):  # type: ignore
                self._error(
                    key_name,
                    data_node_config.properties[key_name],
                    f"{key_name} field of DataNode {data_node_config_id} must be populated with a Set." f"value.",
                )

    def _check_read_write_fct(self, data_node_config_id: str, data_node_config: DataNodeConfig):

        from taipy.core.data.generic import GenericDataNode

        key_names = [
            GenericDataNode._REQUIRED_READ_FUNCTION_PROPERTY,
            GenericDataNode._REQUIRED_WRITE_FUNCTION_PROPERTY,
        ]
        for key_name in key_names:
            if key_name in data_node_config.properties.keys() and not callable(data_node_config.properties[key_name]):
                self._error(
                    key_name,
                    data_node_config.properties[key_name],
                    f"{key_name} field of DataNode {data_node_config_id} must be populated with a Callable function.",
                )

    def _check_required_properties(self, data_node_config_id: str, data_node_config: DataNodeConfig):
        if storage_type := data_node_config.storage_type:
            if storage_type in self._required_properties.keys():
                for required_property in self._required_properties[storage_type]:
                    if required_property not in data_node_config.properties.keys():
                        self._error(
                            "properties",
                            required_property,
                            f"properties field of DataNode {data_node_config_id} is missing the required "
                            f"property {required_property} for type {storage_type}",
                        )
