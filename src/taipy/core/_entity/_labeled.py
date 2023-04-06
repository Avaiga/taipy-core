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

from typing import Optional


class _Labeled:
    __LABEL_SEPARATOR = ">"

    def get_label(self) -> str:
        return self._get_explicit_label() or self._generate_label()

    def get_simple_label(self) -> str:
        return self._get_explicit_label() or self._generate_label(True)

    def _generate_label(self, simple=False) -> str:
        ls = []
        if not simple:
            if owner_id := self._get_owner_id():
                if self.id != owner_id:  # type: ignore
                    from ... import core

                    owner = core.get(owner_id)
                    ls.append(owner.get_label())
        ls.append(self._generate_entity_label())
        return self.__LABEL_SEPARATOR.join(ls)

    def _get_explicit_label(self) -> Optional[str]:
        try:
            return self._properties.get("label")  # type: ignore
        except AttributeError:
            return None

    def _get_owner_id(self) -> Optional[str]:
        try:
            return self.owner_id  # type: ignore
        except AttributeError:
            return None

    def _get_name(self) -> Optional[str]:
        try:
            return self.name  # type: ignore
        except AttributeError:
            try:
                return self._properties.get("name")  # type: ignore
            except AttributeError:
                return None

    def _get_config_id(self) -> Optional[str]:
        try:
            return self.config_id  # type: ignore
        except AttributeError:
            return None

    def _generate_entity_label(self) -> str:
        if name := self._get_name():
            return name
        
        if config_id := self._get_config_id():
            return config_id

        return self.id  # type: ignore
