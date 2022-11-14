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


from .._repository._filesystem_repository import _FileSystemRepository
from ._version import _Version
from ._version_model import _VersionModel


class _VersionFSRepository(_FileSystemRepository):
    def __init__(self):
        super().__init__(_VersionModel, "version", self._to_model, self._from_model)

    def _to_model(self, version: _Version):
        return _VersionModel(id=version.id, config=version.config)

    def _from_model(self, model):
        return _Version(id=model.id, config=model.config)
