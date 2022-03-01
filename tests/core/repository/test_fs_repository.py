import dataclasses
import pathlib
from dataclasses import dataclass
from typing import Any, Dict, Optional

from taipy.core.common.entity import Entity
from taipy.core.config.config import Config
from taipy.core.repository import FileSystemRepository


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
class MockEntity:
    id: str
    name: str


class MockRepository(FileSystemRepository):
    def to_model(self, obj: MockEntity):
        return MockModel(obj.id, obj.name)

    def from_model(self, model: MockModel):
        return MockEntity(model.id, model.name)

    @property
    def storage_folder(self) -> pathlib.Path:
        return pathlib.Path(Config.global_config.storage_folder)  # type: ignore


class TestFileSystemStorage:
    def test_save_and_fetch_model(self):
        r = MockRepository(model=MockModel, dir_name="foo")
        m = MockEntity("uuid", "foo")
        r.save(m)

        fetched_model = r.load(m.id)
        assert m == fetched_model

    def test_get_all(self):
        objs = []
        r = MockRepository(model=MockModel, dir_name="foo")
        for i in range(5):
            m = MockEntity(f"uuid-{i}", f"Foo{i}")
            objs.append(m)
            r.save(m)
        _objs = r.load_all()

        assert len(_objs) == 5

        for obj in _objs:
            assert isinstance(obj, MockEntity)
        assert sorted(objs, key=lambda o: o.id) == sorted(_objs, key=lambda o: o.id)

    def test_delete_all(self):
        r = MockRepository(model=MockModel, dir_name="foo")

        for i in range(5):
            m = MockEntity(f"uuid-{i}", f"Foo{i}")
            r.save(m)

        _models = r.load_all()
        assert len(_models) == 5

        r.delete_all()
        _models = r.load_all()
        assert len(_models) == 0

    def test_search(self):
        r = MockRepository(model=MockModel, dir_name="foo")

        m = MockEntity("uuid", "foo")
        r.save(m)

        m1 = r.search("name", "bar")
        m2 = r.search("name", "foo")

        assert m1 is None
        assert m == m2

    def test_load_entity(self):
        @dataclass
        class MockModel:
            id: str
            name: str
            _timestamp: Optional[int]

            def to_dict(self):
                return dataclasses.asdict(self)

            @staticmethod
            def from_dict(data: Dict[str, Any]):
                return MockModel(id=data["id"], name=data["name"], _timestamp=data.get("_timestamp", None))

        class MockEntity(Entity):
            def __init__(self, id, name):
                super().__init__()
                self.name = name
                self.id = id

        class MockRepository(FileSystemRepository):
            def to_model(self, obj: MockEntity):
                return MockModel(obj.id, obj.name, obj._timestamp)

            def from_model(self, model: MockEntity):
                return MockEntity(model.id, model.name)

            @property
            def storage_folder(self) -> pathlib.Path:
                return pathlib.Path(Config.global_config.storage_folder)  # type: ignore

        r = MockRepository(model=MockModel, dir_name="foo")

        m = MockEntity("uuid", "foo")
        r.save(m)

        # Load by string should always be up to date
        m1 = r.load(m.id)
        filepath = r._get_filepath(m1.id)
        assert isinstance(m1, MockEntity)
        assert m1.name == "foo"
        assert m1.id == "uuid"
        assert m1._timestamp == filepath.stat().st_mtime_ns
        assert r._is_up_to_date(m1, filepath)

        # m1 is never saved again so it should be up to date
        m2 = r.load(m1)
        assert isinstance(m2, MockEntity)
        # we should get the same object
        assert m2 is m1

        # Modify the entity and save
        m2.name = "bar"
        r.save(m2)
        # Should be outdated
        assert not r._is_up_to_date(m2, filepath)
        # Load the object again
        m3 = r.load(m2)
        # Should be up to date again
        assert r._is_up_to_date(m3, filepath)
        assert m3.name == "bar"
