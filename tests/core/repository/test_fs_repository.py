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

from taipy.core._repository import _FileSystemRepository
from taipy.core.config.config import Config


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


class MockRepository(_FileSystemRepository):
    def _to_model(self, obj: MockObj):
        return MockModel(obj.id, obj.name)

    def _from_model(self, model: MockModel, entity: MockObj = None, lazy_loading: bool = True):
        return MockObj(model.id, model.name)

    @property
    def _storage_folder(self) -> pathlib.Path:
        return pathlib.Path(Config.global_config.storage_folder)  # type: ignore


class TestFileSystemStorage:
    def test_save_and_fetch_model(self):
        r = MockRepository(model=MockModel, dir_name="foo")
        m = MockObj("uuid", "foo")
        r._save(m)

        fetched_model = r.load(m.id)
        assert m == fetched_model

    def test_get_all(self):
        objs = []
        r = MockRepository(model=MockModel, dir_name="foo")
        for i in range(5):
            m = MockObj(f"uuid-{i}", f"Foo{i}")
            objs.append(m)
            r._save(m)
        _objs = r._load_all()

        assert len(_objs) == 5

        for obj in _objs:
            assert isinstance(obj, MockObj)
        assert sorted(objs, key=lambda o: o.id) == sorted(_objs, key=lambda o: o.id)

    def test_delete_all(self):
        r = MockRepository(model=MockModel, dir_name="foo")

        for i in range(5):
            m = MockObj(f"uuid-{i}", f"Foo{i}")
            r._save(m)

        _models = r._load_all()
        assert len(_models) == 5

        r._delete_all()
        _models = r._load_all()
        assert len(_models) == 0

    def test_delete_many(self):
        r = MockRepository(model=MockModel, dir_name="foo")
        for i in range(5):
            m = MockObj(f"uuid-{i}", f"Foo{i}")
            r._save(m)
        _models = r._load_all()
        assert len(_models) == 5
        r._delete_many(["uuid-0", "uuid-1"])
        _models = r._load_all()
        assert len(_models) == 3

    def test_search(self):
        r = MockRepository(model=MockModel, dir_name="foo")

        m = MockObj("uuid", "foo")
        r._save(m)

        m1 = r._search("name", "bar")
        m2 = r._search("name", "foo")

        assert m1 is None
        assert m == m2

    def test_config_override(self):
        storage_folder = pathlib.Path("/tmp") / "fodo"
        repo = MockRepository(model=MockModel, dir_name="mock")

        assert repo.dir_path == pathlib.Path(".data") / "mock"
        Config.configure_global_app(storage_folder=str(storage_folder))
        assert repo.dir_path == storage_folder / "mock"
