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

import uuid
from typing import List, Optional, Union

from taipy.config import Config
from taipy.config._config_comparator._comparator_result import _ComparatorResult
from taipy.logger._taipy_logger import _TaipyLogger

from .._manager._manager import _Manager
from ..exceptions.exceptions import ConflictedConfigurationError, ModelNotFound, NonExistingVersion
from ._version import _Version
from ._version_repository_factory import _VersionRepositoryFactory


class _VersionManager(_Manager[_Version]):
    _ENTITY_NAME = _Version.__name__

    __DEVELOPMENT_VERSION = ["development", "dev"]
    __LATEST_VERSION = "latest"
    __PRODUCTION_VERSION = "production"
    __ALL_VERSION = ["all", ""]

    _DEFAULT_VERSION = __LATEST_VERSION

    _repository = _VersionRepositoryFactory._build_repository()  # type: ignore

    @classmethod
    def _get(cls, entity: Union[str, _Version], default=None) -> _Version:
        """
        Returns the version entity by id or reference.
        """
        entity_id = entity if isinstance(entity, str) else entity.id
        try:
            return cls._repository.load(entity_id)
        except ModelNotFound:
            return default

    @classmethod
    def _get_or_create(cls, id: str, force: bool) -> _Version:
        if version := cls._get(id):
            comparator_result = Config._comparator._compare(version.config, Config._applied_config, id)

            if comparator_result.get(_ComparatorResult.CONFLICTED_SECTION_KEY):
                if force:
                    _TaipyLogger._get_logger().warning(f"Overriding version {id} ...")
                    version.config = Config._applied_config
                else:
                    raise ConflictedConfigurationError()

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
        cls._get_or_create(version_number, force=True)
        cls._repository._set_development_version(version_number)
        return version_number

    @classmethod
    def _get_development_version(cls) -> str:
        try:
            return cls._repository._get_development_version()
        except:  # noqa: E722
            return cls._set_development_version(str(uuid.uuid4()))

    @classmethod
    def _set_experiment_version(cls, version_number: str, force: bool) -> str:
        if version_number == cls._get_development_version():
            raise SystemExit(
                f"Version number {version_number} is the development version. Please choose a different name"
                f" for this experiment."
            )

        if version_number in cls._get_production_versions():
            raise SystemExit(
                f"Version number {version_number} is already a production version. Please choose a different name"
                f" for this experiment."
            )

        try:
            cls._get_or_create(version_number, force)
        except ConflictedConfigurationError:
            raise SystemExit(
                f"Please add a new experiment version or run your application with --force option to override"
                f" the Config of experiment {version_number}."
            )
        cls._repository._set_latest_version(version_number)
        return version_number

    @classmethod
    def _get_latest_version(cls) -> str:
        try:
            return cls._repository._get_latest_version()
        except:  # noqa: E722
            # If there is no version in the system yet, create a new version as development version
            # This set the default versioning behavior on Jupyter notebook to Development mode
            return cls._set_development_version(str(uuid.uuid4()))

    @classmethod
    def _set_production_version(cls, version_number: str, force: bool) -> str:
        if version_number == cls._get_development_version():
            cls._set_development_version(str(uuid.uuid4()))

        try:
            cls._get_or_create(version_number, force)
        except ConflictedConfigurationError:
            raise SystemExit(
                f"Please add a new production version with a migration function or run your application with --force"
                f" option to override the Config of production version {version_number}."
            )
        cls._repository._set_production_version(version_number)
        return version_number

    @classmethod
    def _get_production_versions(cls) -> List[str]:
        try:
            return cls._repository._get_production_versions()
        except:  # noqa: E722
            return []

    @classmethod
    def _delete_production_version(cls, version_number) -> str:
        return cls._repository._delete_production_version(version_number)

    @classmethod
    def _replace_version_number(cls, version_number):
        if version_number is None:
            version_number = cls._replace_version_number(cls._DEFAULT_VERSION)

            production_versions = cls._get_production_versions()

            if version_number not in production_versions:
                return version_number
            return production_versions

        if version_number == cls.__LATEST_VERSION:
            return cls._get_latest_version()
        if version_number in cls.__DEVELOPMENT_VERSION:
            return cls._get_development_version()
        if version_number == cls.__PRODUCTION_VERSION:
            return cls._get_production_versions()
        if version_number in cls.__ALL_VERSION:
            return ""
        if version := cls._get(version_number):
            return version.id

        raise NonExistingVersion(version_number)

    @classmethod
    def _delete_entities_of_multiple_types(cls, _entity_ids):
        return NotImplementedError
