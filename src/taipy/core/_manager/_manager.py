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

import pathlib
from typing import Generic, Iterable, List, Optional, TypeVar, Union

from taipy.logger._taipy_logger import _TaipyLogger

from .._entity._entity_ids import _EntityIds
from .._repository import _AbstractRepository
from ..exceptions.exceptions import ModelNotFound
from ..notification import EventOperation, _publish_event

EntityType = TypeVar("EntityType")


class _Manager(Generic[EntityType]):
    _repository: _AbstractRepository
    _logger = _TaipyLogger._get_logger()
    _ENTITY_NAME: str = "Entity"

    @classmethod
    def _delete_all(cls):
        """
        Deletes all entities.
        """
        cls._repository._delete_all()
        if hasattr(cls, "_EVENT_ENTITY_TYPE"):
            _publish_event(cls._EVENT_ENTITY_TYPE, "all", EventOperation.DELETION, None)

    @classmethod
    def _delete_many(cls, ids: Iterable):
        """
        Deletes entities by a list of ids.
        """
        cls._repository._delete_many(ids)
        if hasattr(cls, "_EVENT_ENTITY_TYPE"):
            for entity_id in ids:
                _publish_event(cls._EVENT_ENTITY_TYPE, entity_id, EventOperation.DELETION, None)  # type: ignore

    @classmethod
    def _delete_by_version(cls, version_number: str):
        """
        Deletes entities by version number.
        """
        cls._repository._delete_by(attribute="version", value=version_number, version_number="all")  # type: ignore
        if hasattr(cls, "_EVENT_ENTITY_TYPE"):
            _publish_event(cls._EVENT_ENTITY_TYPE, None, EventOperation.DELETION, None)  # type: ignore

    @classmethod
    def _delete(cls, id):
        """
        Deletes an entity by id.
        """
        cls._repository._delete(id)
        if hasattr(cls, "_EVENT_ENTITY_TYPE"):
            _publish_event(cls._EVENT_ENTITY_TYPE, id, EventOperation.DELETION, None)

    @classmethod
    def _set(cls, entity: EntityType):
        """
        Save or update an entity.
        """
        cls._repository._save(entity)

    @classmethod
    def _get_all(cls, version_number: Optional[str] = None) -> List[EntityType]:
        """
        Returns all entities.
        """
        return cls._repository._load_all(version_number)

    @classmethod
    def _get_all_by(cls, by, version_number: Optional[str] = None) -> List[EntityType]:
        """
        Returns all entities based on a criteria.
        """
        return cls._repository._load_all_by(by, version_number)

    @classmethod
    def _get(cls, entity: Union[str, EntityType], default=None) -> EntityType:
        """
        Returns an entity by id or reference.
        """
        entity_id = entity if isinstance(entity, str) else entity.id  # type: ignore
        try:
            return cls._repository.load(entity_id)
        except ModelNotFound:
            cls._logger.error(f"{cls._ENTITY_NAME} not found: {entity_id}")
            return default

    @classmethod
    def _delete_entities_of_multiple_types(cls, _entity_ids: _EntityIds):
        """
        Deletes entities of multiple types.
        """
        from ..cycle._cycle_manager_factory import _CycleManagerFactory
        from ..data._data_manager_factory import _DataManagerFactory
        from ..job._job_manager_factory import _JobManagerFactory
        from ..pipeline._pipeline_manager_factory import _PipelineManagerFactory
        from ..scenario._scenario_manager_factory import _ScenarioManagerFactory
        from ..task._task_manager_factory import _TaskManagerFactory

        _CycleManagerFactory._build_manager()._delete_many(_entity_ids.cycle_ids)
        _ScenarioManagerFactory._build_manager()._delete_many(_entity_ids.scenario_ids)
        _PipelineManagerFactory._build_manager()._delete_many(_entity_ids.pipeline_ids)
        _TaskManagerFactory._build_manager()._delete_many(_entity_ids.task_ids)
        _JobManagerFactory._build_manager()._delete_many(_entity_ids.job_ids)
        _DataManagerFactory._build_manager()._delete_many(_entity_ids.data_node_ids)

    @classmethod
    def _export(cls, id: str, folder_path: Union[str, pathlib.Path]):
        """
        Export an entity.
        """
        return cls._repository._export(id, folder_path)

    @classmethod
    def _get_by_config_id(cls, config_id: str) -> List[EntityType]:
        """
        Get all entities by its config id.
        """
        return cls._repository._get_by_config_id(config_id)
