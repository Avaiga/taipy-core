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

from copy import copy
from typing import Any, Dict, List, Optional, TypedDict

from taipy.config import Config, UniqueSection

ServiceConfig = TypedDict(
    "ServiceConfig",
    {
        "mode": str,
        "version_number": str,
        "force": bool,
        "clean_entities": bool,
    },
    total=False,
)


default_service_config: ServiceConfig = {
    "mode": "development",
    "version_number": "",
    "force": False,
    "clean_entities": False,
}


class CoreSection(UniqueSection):
    name = "core"

    def __init__(self, property_list: Optional[List] = None, **properties):
        self._property_list = property_list
        super().__init__(**properties)

    def __copy__(self):
        return CoreSection(property_list=copy(self._property_list), **copy(self._properties))

    def _to_dict(self):
        as_dict = {}
        as_dict.update(self._properties)
        return as_dict

    @classmethod
    def _from_dict(cls, as_dict: Dict[str, Any], *_):
        return CoreSection(property_list=list(default_service_config), **as_dict)

    def _update(self, as_dict: Dict[str, Any]):
        if self._property_list:
            as_dict = {k: v for k, v in as_dict.items() if k in self._property_list}
        self._properties.update(as_dict)

    @staticmethod
    def _configure(**properties):
        """Configure the Core service.

        Parameters:
            **properties (Dict[str, Any]): Keyword arguments that configure the behavior of the `Core^` instances.
        Returns:
            `CoreSection^`: The Core service configuration.
        """
        section = CoreSection(property_list=list(default_service_config), **properties)
        Config._register(section)
        return Config.unique_sections[CoreSection.name]
