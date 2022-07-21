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
import sqlite3
from typing import Any, Iterable, List, Optional, Type, TypeVar, Union

from sqlalchemy import create_engine
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import sessionmaker

from taipy.config import Config
from taipy.core.exceptions.exceptions import MissingRequiredProperty, ModelNotFound

from ._repository import _AbstractRepository, _CustomDecoder
from ._sql_model import Base, _TaipyModel

ModelType = TypeVar("ModelType")
Entity = TypeVar("Entity")
# Json = Union[dict, list, str, int, float, bool, None]


class _SQLRepository(_AbstractRepository[ModelType, Entity]):
    def __init__(self, model: Type[ModelType]):
        properties = Config.global_config.repository_properties
        self.model = model
        try:
            # More sql databases can be easily added in the future
            self.engine = create_engine(f"sqlite3://{properties['db_location']}")

            # Maybe this should be in the taipy package? So it's not executed every time
            # the class is instantiated
            Base.metadata.create_all(self.engine)
            _session = sessionmaker(bind=self.engine)
            self.session = _session()

        except KeyError:
            raise MissingRequiredProperty("Missing property db_location")

    def __to_entity(self, entry: _TaipyModel):
        return self.__model_to_entity(entry.document)

    def __model_to_entity(self, file_content):
        data = json.loads(file_content, cls=_CustomDecoder)
        model = self.model.from_dict(data)  # type: ignore
        return self._from_model(model)

    def load(self, model_id: str) -> Entity:
        try:
            entry = self.session.query(_TaipyModel).filter_by(model_id=model_id).first()

            return self.__to_entity(entry)
        except NoResultFound:
            raise ModelNotFound(self.model, model_id)  # type: ignore

    def _load_all(self) -> List[Entity]:
        try:
            entries = self.session.query(_TaipyModel).filter_by(entiy_type=self.model)
            return [self.__to_entity(e) for e in entries]
        except NoResultFound:
            return []

    def _load_all_by(self, by) -> List[Entity]:
        pass

    def _save(self, entity: Entity):
        pass

    def _delete(self, entity_id: str):
        pass

    def _delete_all(self):
        pass

    def _delete_many(self, ids: Iterable[str]):
        pass

    def _search(self, attribute: str, value: Any) -> Optional[Entity]:
        pass
