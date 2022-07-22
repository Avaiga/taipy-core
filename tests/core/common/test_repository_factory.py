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
import pytest

from src.taipy.core._repository import _FileSystemRepository, _SQLRepository
from src.taipy.core._repository._repository_factory import _RepositoryFactory
from taipy.config import Config


@pytest.mark.parametrize(
    "type,repository_class", [("foo", _FileSystemRepository.__class__), ("sql", _SQLRepository.__class__)]
)
def test_build_repository(type, repository_class):
    if not Config.global_config.repository_type:
        # Config not set, returns default repository
        repo = _RepositoryFactory.build_repository()
        assert isinstance(repo, repository_class)

    # Non existing repository, returns default repository
    Config.global_config.repository_type = type

    repo = _RepositoryFactory.build_repository()
    assert isinstance(repo, repository_class)
