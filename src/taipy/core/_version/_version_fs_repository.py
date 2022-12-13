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

import json
import uuid
from datetime import datetime
from typing import List, Optional

from taipy.config.config import Config

from .._repository._filesystem_repository import _FileSystemRepository
from ._version import _Version
from ._version_model import _VersionModel


class _VersionFSRepository(_FileSystemRepository):
    _LATEST_VERSION_KEY = "latest_version"
    _DEVELOPMENT_VERSION_KEY = "development_version"

    def __init__(self):
        super().__init__(_VersionModel, "version", self._to_model, self._from_model)

    @property
    def _version_file_path(self):
        return self.dir_path / "version.json"

    def _to_model(self, version: _Version):
        return _VersionModel(
            id=version.id, config=Config._to_json(version.config), creation_date=version.creation_date.isoformat()
        )

    def _from_model(self, model):
        version = _Version(id=model.id, config=Config._from_json(model.config))
        version.creation_date = datetime.fromisoformat(model.creation_date)
        return version

    def _load_all(self, version_number: Optional[str] = "all") -> List[_Version]:
        """Only load the json file that has "id" property.

        The "version.json" does not have the "id" property.
        """
        return self._load_all_by(by="id", version_number=version_number)

    def _load_all_by(self, by, version_number: Optional[str] = "all"):
        return super()._load_all_by(by, version_number)

    def _set_latest_version(self, version_number):
        if self._version_file_path.exists():
            development_version = self._get_development_version()
        else:
            self.dir_path.mkdir(parents=True, exist_ok=True)
            development_version = ""

        self._version_file_path.write_text(
            json.dumps(
                {self._LATEST_VERSION_KEY: version_number, self._DEVELOPMENT_VERSION_KEY: development_version},
                ensure_ascii=False,
                indent=0,
            )
        )

    def _get_latest_version(self):
        try:
            with open(self._version_file_path, "r") as f:
                file_content = json.load(f)

            return file_content[self._LATEST_VERSION_KEY]

        except FileNotFoundError:
            # If not found, create new version to become the latest
            version_number = str(uuid.uuid4())
            self._set_latest_version(version_number)
            return version_number

    def _set_development_version(self, version_number):
        if self._version_file_path.exists():
            latest_version = self._get_latest_version()
        else:
            self.dir_path.mkdir(parents=True, exist_ok=True)
            latest_version = version_number

        self._version_file_path.write_text(
            json.dumps(
                {self._LATEST_VERSION_KEY: latest_version, self._DEVELOPMENT_VERSION_KEY: version_number},
                ensure_ascii=False,
                indent=0,
            )
        )

    def _get_development_version(self):
        try:
            with open(self._version_file_path, "r") as f:
                file_content = json.load(f)
            if version_number := file_content[self._DEVELOPMENT_VERSION_KEY]:
                return version_number
            return str(uuid.uuid4())

        except FileNotFoundError:
            # If not found, create new development version
            version_number = str(uuid.uuid4())
            self._set_development_version(version_number)
            return version_number
