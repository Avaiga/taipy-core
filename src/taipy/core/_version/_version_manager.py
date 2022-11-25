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
    _DEFAULT_VERSION = "latest"
    _ENTITY_NAME = _Version.__name__

    _repository = _VersionRepositoryFactory._build_repository()  # type: ignore
    _current_version = _DEFAULT_VERSION

    @classmethod
    def create(cls, id: str, override: bool) -> _Version:
        if not override and cls._get(id) is not None:
            raise VersionAlreadyExists(f"Version {id} already exists.")

        version = _Version(id=id, config=Config._applied_config)
        cls._set(version)

        return version

    @classmethod
    def set_current_version(cls, version_number: str, override: bool):
        cls._current_version = version_number

        return cls.create(version_number, override)

    @classmethod
    def get_current_version(cls) -> str:
        return cls._current_version
