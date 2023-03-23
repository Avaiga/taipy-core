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

import json
import pathlib
import shutil
from typing import Any, Dict, Iterable, Iterator, List, Optional, Type, Union

from src.taipy.core.common._utils import _retry
from src.taipy.core.common.typing import Converter, Entity, Json, ModelType
from taipy.config.config import Config

from ...exceptions import InvalidExportPath, ModelNotFound
from .._v2._abstract_repository import _AbstractRepository, _Decoder, _Encoder


class _FileSystemRepository(_AbstractRepository[ModelType, Entity]):
    """
    Holds common methods to be used and extended when the need for saving
    dataclasses as JSON files in local storage emerges.

    Some lines have type: ignore because MyPy won't recognize some generic attributes. This
    should be revised in the future.

    Attributes:
        model (ModelType): Generic dataclass.
        dir_name (str): Folder that will hold the files for this dataclass model.
    """

    def __init__(self, model: Type[ModelType], converter: Type[Converter], dir_name: str):
        self.model = model
        self.converter = converter
        self._dir_name = dir_name

    @property
    def dir_path(self):
        return self._storage_folder / self._dir_name

    @property
    def _storage_folder(self) -> pathlib.Path:
        return pathlib.Path(Config.global_config.storage_folder)

    def _save(self, entity: Entity):
        self.__create_directory_if_not_exists()

        model = self.converter._entity_to_model(entity)
        self.__get_model_filepath(model.id).write_text(
            json.dumps(model.to_dict(), ensure_ascii=False, indent=0, cls=_Encoder, check_circular=False)
        )

    def _get(self, filepath: pathlib.Path) -> Json:
        with pathlib.Path(filepath).open(encoding="UTF-8") as source:
            return json.load(source)

    @_retry(Config.global_config.read_entity_retry or 0, (Exception,))
    def _load(self, model_id: str) -> Entity:
        try:
            model = self.model(**self._get(self.__get_model_filepath(model_id)))
            return self.converter._model_to_entity(model)
        except FileNotFoundError:
            raise ModelNotFound(str(self.dir_path), model_id)

    @_retry(Config.global_config.read_entity_retry or 0, (Exception,))
    def _load_all(self, filters: Optional[List[Dict]] = None) -> List[Entity]:
        entities = []
        try:
            for f in self.dir_path.iterdir():
                if data := self.__filter_by(f, filters):
                    entities.append(self.converter._model_to_entity(self.model(**data)))
        except FileNotFoundError:
            pass
        return entities

    def _delete(self, model_id: str):
        try:
            self.__get_model_filepath(model_id).unlink()
        except FileNotFoundError:
            raise ModelNotFound(str(self.dir_path), model_id)

    def _delete_all(self):
        shutil.rmtree(self.dir_path, ignore_errors=True)

    def _delete_many(self, ids: Iterable[str]):
        for model_id in ids:
            self._delete(model_id)

    def _search(self, attribute: str, value: Any, filters: List[Dict] = None) -> Optional[Entity]:
        return next(self.__search(attribute, value), None)

    def _export(self, entity_id: str, folder_path: Union[str, pathlib.Path]):
        if isinstance(folder_path, str):
            folder: pathlib.Path = pathlib.Path(folder_path)
        else:
            folder = folder_path

        if folder.resolve() == self._storage_folder.resolve():
            raise InvalidExportPath("The export folder must not be the storage folder.")

        export_dir = folder / self._dir_name
        if not export_dir.exists():
            export_dir.mkdir(parents=True)

        export_path = export_dir / f"{entity_id}.json"
        # Delete if exists.
        if export_path.exists():
            export_path.unlink()

        shutil.copy2(self.__get_model_filepath(entity_id), export_path)

    def __create_directory_if_not_exists(self):
        self.dir_path.mkdir(parents=True, exist_ok=True)

    def __search(self, attribute: str, value: str, filters: List[Dict] = None) -> Iterator[Entity]:
        return filter(lambda e: getattr(e, attribute, None) == value, self._load_all(filters))

    def __get_model_filepath(self, model_id) -> pathlib.Path:
        return self.dir_path / f"{model_id}.json"

    def __filter_by(self, filepath: pathlib.Path, filters: Optional[List[Dict]]) -> Json:
        if not filters:
            filters = []
        with open(filepath, "r") as f:
            contents = f.read()
            for filter in filters:
                if not all(f'"{key}": "{value}"' in contents for key, value in filter.items()):
                    return None

        return json.loads(contents, cls=_Decoder)