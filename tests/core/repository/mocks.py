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
import pathlib
from dataclasses import dataclass
from typing import Any, Dict

from src.taipy.core._repository import _FileSystemRepository
from taipy.config.config import Config


@dataclass
class MockModel:
    id: str
    name: str

    def to_dict(self):
        return dataclasses.asdict(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]):
        return MockModel(id=data["id"], name=data["name"])


@dataclass
class MockObj:
    id: str
    name: str


class MockFSRepository(_FileSystemRepository):
    def __init__(self, **kwargs):
        kwargs.update({"to_model_fct": self._to_model, "from_model_fct": self._from_model})
        super().__init__(**kwargs)

    def _to_model(self, obj: MockObj):
        return MockModel(obj.id, obj.name)

    def _from_model(self, model: MockModel):
        return MockObj(model.id, model.name)

    @property
    def _storage_folder(self) -> pathlib.Path:
        return pathlib.Path(Config.global_config.storage_folder)  # type: ignore