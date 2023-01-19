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

import dataclasses
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class _TaskModel:
    id: str
    owner_id: Optional[str]
    parent_ids: List[str]
    config_id: str
    input_ids: List[str]
    function_name: str
    function_module: str
    output_ids: List[str]
    version: str
    skippable: bool
    properties: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]):
        return _TaskModel(
            id=data["id"],
            owner_id=data.get("owner_id", data.get("parent_id")),
            parent_ids=data["parent_ids"],
            config_id=data["config_id"],
            input_ids=data["input_ids"],
            function_name=data["function_name"],
            function_module=data["function_module"],
            output_ids=data["output_ids"],
            version=data["version"],
            skippable=data.get("skippable", False),
            properties=data.get("properties", {}),
        )
