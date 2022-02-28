import dataclasses
import pathlib
from dataclasses import dataclass
from typing import Any, Dict

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
class MockObj:
    id: str
    name: str


class MockRepository(FileSystemRepository):
    def to_model(self, obj: MockObj):
        return MockModel(obj.id, obj.name)

    def from_model(self, model: MockModel):
        return MockObj(model.id, model.name)

    @property
    def storage_folder(self) -> pathlib.Path:
        return pathlib.Path(Config.global_config.storage_folder)  # type: ignore


class TestFileSystemStorage:
    def test_save_and_fetch_model(self):
        r = MockRepository(model=MockModel, dir_name="foo")
        m = MockObj("uuid", "foo")
        r.save(m)

        fetched_model = r.load(m.id)
        assert m == fetched_model

    def test_get_all(self):
        objs = []
        r = MockRepository(model=MockModel, dir_name="foo")
        for i in range(5):
            m = MockObj(f"uuid-{i}", f"Foo{i}")
            objs.append(m)
            r.save(m)
        _objs = r.load_all()

        assert len(_objs) == 5

        for obj in _objs:
            assert isinstance(obj, MockObj)
        assert sorted(objs, key=lambda o: o.id) == sorted(_objs, key=lambda o: o.id)

    def test_delete_all(self):
        r = MockRepository(model=MockModel, dir_name="foo")

        for i in range(5):
            m = MockObj(f"uuid-{i}", f"Foo{i}")
            r.save(m)

        _models = r.load_all()
        assert len(_models) == 5

        r.delete_all()
        _models = r.load_all()
        assert len(_models) == 0

    def test_search(self):
        r = MockRepository(model=MockModel, dir_name="foo")

        m = MockObj("uuid", "foo")
        r.save(m)

        m1 = r.search("name", "bar")
        m2 = r.search("name", "foo")

        assert m1 is None
        assert m == m2

    def test_load_entity_caching(self):
        r = MockRepository(model=MockModel, dir_name="foo")

        m = MockObj("uuid", "foo")
        r.save(m)
        assert len(r._cache) == 0

        # Load object for first time, cache should be saved
        e = r.load(m.id)
        assert len(r._cache) == 1

        # Load object again, cache should be used
        e2 = r.load(m.id)
        # Expect the same object
        assert e2 is e

        # Save new object, cache should be invalidated
        m2 = MockObj(m.id, "bar")
        r.save(m2)
        assert len(r._cache) == 0
        e3 = r.load(m.id)
        # Should be a different object
        assert e3 is not e2
        assert len(r._cache) == 1

        entity_cache = list(r._cache._cache.values())[0]
        # Make the cache outdated
        entity_cache.last_modified_time = 0
        e4 = r.load(m.id)
        # Should be a different object
        assert e4 is not e3

    def test_entity_cache_limit(self):
        r = MockRepository(model=MockModel, dir_name="foo")
        limit = FileSystemRepository._CACHE_LIMIT
        # Try to add more objects
        for i in range(limit + 1):
            m = MockObj(f"uuid-{i}", f"Foo{i}")
            r.save(m)
            r.load(m.id)

        # Cache size should be limited
        assert len(r._cache) == limit

        m = MockObj(f"uuid-{limit + 2}", f"Foo{limit + 2}")
        r.save(m)
        r.load(m.id)
        assert len(r._cache) == limit
