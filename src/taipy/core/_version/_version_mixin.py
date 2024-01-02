# Copyright 2021-2024 Avaiga Private Limited
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

from typing import Dict, List

from .._version._version_manager_factory import _VersionManagerFactory


class _VersionMixin:

    _version_manager = _VersionManagerFactory._build_manager()

    @classmethod
    def __fetch_version_number(cls, version_number):
        version_number = _VersionManagerFactory._build_manager()._replace_version_number(version_number)

        if not isinstance(version_number, List):
            version_number = [version_number] if version_number else []
        return version_number

    @classmethod
    def _build_filters_with_version(cls, version_number) -> List[Dict]:
        filters = []
        if versions := cls.__fetch_version_number(version_number):
            filters = [{"version": version} for version in versions]
        return filters

    @classmethod
    def _get_latest_version(cls):
        return cls._version_manager._get_latest_version()
