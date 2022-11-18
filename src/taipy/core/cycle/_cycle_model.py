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
from typing import Any, Dict

from taipy.config.common.frequency import Frequency

from ..common.alias import CycleId


@dataclass
class _CycleModel:
    id: CycleId
    name: str
    frequency: Frequency
    properties: Dict[str, Any]
    creation_date: str
    start_date: str
    end_date: str

    def to_dict(self) -> Dict[str, Any]:
        return {**dataclasses.asdict(self), "frequency": repr(self.frequency)}

    @staticmethod
    def from_dict(data: Dict[str, Any]):
        return _CycleModel(
            id=data["id"],
            name=data["name"],
            frequency=Frequency._from_repr(data["frequency"]),
            properties=data["properties"],
            creation_date=data["creation_date"],
            start_date=data["start_date"],
            end_date=data["end_date"],
        )
