import json
import pathlib
import shutil
from abc import abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Generic, Iterator, List, Optional, OrderedDict, Type, TypeVar, Union

from taipy.core.exceptions.repository import ModelNotFound

ModelType = TypeVar("ModelType")
Entity = TypeVar("Entity")
Json = Union[dict, list, str, int, float, bool, None]


class CustomEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Json:
        result: Json
        if isinstance(o, Enum):
            result = o.value
        elif isinstance(o, datetime):
            result = {"__type__": "Datetime", "__value__": o.isoformat()}
        else:
            result = json.JSONEncoder.default(self, o)
        return result


class CustomDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, source):
        if source.get("__type__") == "Datetime":
            return datetime.fromisoformat(source.get("__value__"))
        else:
            return source


class EntityCache(Generic[Entity]):
    def __init__(self, data: Entity, last_modified_time: float):
        self.data = data
        self.last_modified_time = last_modified_time


class EntityCacheManager(Generic[Entity]):
    def __init__(self, limit=100) -> None:
        self._cache: OrderedDict[str, EntityCache[Entity]] = OrderedDict()
        self.limit = limit

    def get(self, filepath: pathlib.Path) -> Optional[Entity]:
        if entity_cache := self._cache.get(filepath.name):
            if entity_cache.last_modified_time == filepath.stat().st_mtime_ns:
                return entity_cache.data
        return None

    def save(self, filepath: pathlib.Path, value: Entity):
        self._cache[filepath.name] = EntityCache(value, filepath.stat().st_mtime_ns)
        while len(self._cache) > self.limit:
            self._cache.popitem(False)

    def pop(self, filepath: pathlib.Path) -> Optional[Entity]:
        if v := self._cache.pop(filepath.name, None):
            return v.data
        return None

    def __len__(self):
        return len(self._cache)

    def clear(self):
        self._cache.clear()


class FileSystemRepository(Generic[ModelType, Entity]):
    """
    Holds common methods to be used and extended when the need for saving
    dataclasses as JSON files in local storage emerges.

    Some lines have type: ignore because MyPy won't recognize some generic attributes. This
    should be revised in the future.

    Attributes:
        model (ModelType): Generic dataclass.
        dir_name (str): Folder that will hold the files for this dataclass model.
    """

    _CACHE_LIMIT = 100

    @abstractmethod
    def to_model(self, obj):
        """
        Converts the object to be saved to its model.
        """
        ...

    @property
    @abstractmethod
    def storage_folder(self) -> pathlib.Path:
        """
        Base folder used by repository to store data
        """
        ...

    @abstractmethod
    def from_model(self, model) -> Entity:
        """
        Converts a model to its functional object.
        """
        ...

    def __init__(self, model: Type[ModelType], dir_name: str):
        self.model = model
        self.dir_name = dir_name
        self._cache: EntityCacheManager[Entity] = EntityCacheManager(self._CACHE_LIMIT)

    @property
    def directory(self) -> pathlib.Path:
        dir_path = self.storage_folder / self.dir_name

        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)

        return dir_path

    def load(self, model_id: str) -> Entity:
        return self.__to_entity(self.__get_model(model_id))

    def load_all(self) -> List[Entity]:
        return [self.__to_entity(f) for f in self.directory.glob("*.json")]

    def save(self, model):
        model = self.to_model(model)
        file_path = self.__get_model(model.id, False)
        self._cache.pop(file_path)
        file_path.write_text(json.dumps(model.to_dict(), ensure_ascii=False, indent=4, cls=CustomEncoder))

    def delete_all(self):
        shutil.rmtree(self.directory)

    def delete(self, model_id: str):
        self.__get_model(model_id).unlink()

    def search(self, attribute: str, value: str) -> Optional[Entity]:
        return next(self._search(attribute, value), None)

    def search_all(self, attribute: str, value: str) -> List[Entity]:
        return list(self._search(attribute, value))

    def _build_model(self, model_data: Dict) -> ModelType:
        return self.model.from_dict(model_data)  # type: ignore

    def _search(self, attribute: str, value: str) -> Iterator[Entity]:
        return filter(lambda e: hasattr(e, attribute) and getattr(e, attribute) == value, self.load_all())

    def __get_model(self, model_id, raise_if_not_exist=True) -> pathlib.Path:
        filepath = self.directory / f"{model_id}.json"

        if not filepath.exists() and raise_if_not_exist:
            raise ModelNotFound(str(self.directory), model_id)

        return filepath

    def __to_entity(self, filepath: pathlib.Path) -> Entity:
        if entity := self._cache.get(filepath):
            return entity

        with open(filepath, "r") as f:
            data = json.load(f, cls=CustomDecoder)
        model = self.model.from_dict(data)  # type: ignore
        entity = self.from_model(model)
        self._cache.save(filepath, entity)
        return entity
