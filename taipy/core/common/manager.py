from typing import Any, Generic, List, TypeVar

from taipy.core.common.logger import TaipyLogger
from taipy.core.repository import FileSystemRepository

EntityType = TypeVar("EntityType")


class Manager(Generic[EntityType]):
    _repository: FileSystemRepository
    _logger = TaipyLogger.get_logger()

    @classmethod
    def delete_all(cls):
        """
        Deletes all entities.
        """
        cls._repository.delete_all()

    @classmethod
    def delete(cls, id: Any, *args: Any, **kwargs: Any):
        """
        Deletes an entity by id.
        """
        cls._repository.delete(id)

    @classmethod
    def set(cls, entity: EntityType):
        """
        Save or update an entity.
        """
        cls._repository.save(entity)

    @classmethod
    def get_all(cls) -> List[EntityType]:
        """
        Returns all entities.
        """
        return cls._repository.load_all()

    @classmethod
    def _get_all_by_config_name(cls, config_name: str) -> List[EntityType]:
        return cls._repository.search_all("config_name", config_name)
