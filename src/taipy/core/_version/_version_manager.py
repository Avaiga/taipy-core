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

import uuid
from typing import List, Optional

from taipy.config import Config

from .._manager._manager import _Manager
from ..exceptions.exceptions import NonExistingVersion, VersionConflictWithPythonConfig
from ._version import _Version
from ._version_repository_factory import _VersionRepositoryFactory


class _VersionManager(_Manager[_Version]):
    _ENTITY_NAME = _Version.__name__

    __DEVELOPMENT_VERSION = ["development", "dev"]
    __LATEST_VERSION = "latest"
    __PRODUCTION_VERSION = "production"
    __ALL_VERSION = ["all", ""]

    __DEFAULT_VERSION = __LATEST_VERSION

    _repository = _VersionRepositoryFactory._build_repository()  # type: ignore

    @classmethod
    def get_or_create(cls, id: str, override: bool) -> _Version:
        if version := cls._get(id):
            if not Config._check_config_compatibility(Config._applied_config, version.config):
                if override:
                    version.config = Config._applied_config

                raise VersionConflictWithPythonConfig(
                    f"The Configuration of version {id} is conflict with the current Python Config."
                )

        else:
            version = _Version(id=id, config=Config._applied_config)

        cls._set(version)
        return version

    @classmethod
    def _get_all(cls, version_number: Optional[str] = "all") -> List[_Version]:
        """
        Returns all entities.
        """
        return cls._repository._load_all(version_number)

    @classmethod
    def _get_all_by(cls, by, version_number: Optional[str] = "all") -> List[_Version]:
        """
        Returns all entities based on a criteria.
        """
        return cls._repository._load_all_by(by, version_number)

    @classmethod
    def _set_development_version(cls, version_number: str) -> str:
        cls.get_or_create(version_number, override=True)
        cls._repository._set_development_version(version_number)
        return version_number

    @classmethod
    def _get_development_version(cls) -> str:
        try:
            return cls._repository._get_development_version()
        except FileNotFoundError:
            return cls._set_development_version(str(uuid.uuid4()))

    @classmethod
    def _set_latest_version(cls, version_number: str, override: bool) -> str:
        cls.get_or_create(version_number, override)
        cls._repository._set_latest_version(version_number)
        return version_number

    @classmethod
    def _get_latest_version(cls) -> str:
        try:
            return cls._repository._get_latest_version()
        except FileNotFoundError:
            return cls._set_latest_version(str(uuid.uuid4()))

    @classmethod
    def _set_production_version(cls, version_number: str, override: bool) -> str:
        production_versions = cls._get_production_version()

        # Check if all previous production versions are compatible with current Python Config
        for production_version in production_versions:
            if version := cls._get(production_version):
                if not Config._check_config_compatibility(Config._applied_config, version.config):
                    raise VersionConflictWithPythonConfig(
                        f"The Configuration of version {production_version} is conflict with the current Python Config."
                    )
            else:
                raise NonExistingVersion(production_version)

        cls.get_or_create(version_number, override)
        cls._repository._set_production_version(version_number)
        return version_number

    @classmethod
    def _get_production_version(cls) -> List[str]:
        try:
            return cls._repository._get_production_version()
        except FileNotFoundError:
            return []

    @classmethod
    def _delete_production_version(cls, version_number) -> str:
        return cls._repository._delete_production_version(version_number)

    @classmethod
    def _replace_version_number(cls, version_number):
        if version_number is None:
            version_number = cls.__DEFAULT_VERSION

        if version_number == cls.__LATEST_VERSION:
            return cls._get_latest_version()
        if version_number in cls.__DEVELOPMENT_VERSION:
            return cls._get_development_version()
        if version_number in cls.__ALL_VERSION:
            return ""
        if version := cls._get(version_number):
            return version.id

        raise NonExistingVersion(version_number)
