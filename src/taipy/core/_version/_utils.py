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

from .._core_cli import _CoreCLI
from ..config import CoreSection
from ._version_manager_factory import _VersionManagerFactory


def _version_migration() -> str:
    """Add version attribute on old entities. Used to migrate from <=2.0 to >=2.1 version."""

    args = _CoreCLI.parse_arguments()

    if args[CoreSection._MODE_KEY] != CoreSection._DEVELOPMENT_MODE:
        return _VersionManagerFactory._build_manager()._get_latest_version()
    else:
        _VersionManagerFactory._build_manager()._get_or_create("LEGACY-VERSION", True)
        return "LEGACY-VERSION"
