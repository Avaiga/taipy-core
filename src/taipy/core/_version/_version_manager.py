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

from taipy.config import Config

from .._manager._manager import _Manager
from ..exceptions.exceptions import VersionAlreadyExists
from ._version import _Version
from ._version_repository_factory import _VersionRepositoryFactory


class _VersionManager(_Manager[_Version]):
    _repository = _VersionRepositoryFactory._build_repository()  # type: ignore

    @classmethod
    def create(cls, id: str) -> _Version:
        if cls._get(id) is not None:
            raise VersionAlreadyExists(f"Version {id} already exists")

        version = _Version(id=id, config=Config._applied_config)
        cls._set(version)

        return version
