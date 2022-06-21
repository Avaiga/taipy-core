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

from typing import Type

from ._scenario_manager import _ScenarioManager
from .._manager._manager_factory import _ManagerFactory
from ..common._utils import _load_fct


class _ScenarioManagerFactory(_ManagerFactory):
    @classmethod
    def _build_manager(cls) -> Type[_ScenarioManager]:  # type: ignore
        if cls._using_enterprise():
            return _load_fct(cls._TAIPY_ENTERPRISE_CORE_MODULE + ".scenario._scenario_manager", "_ScenarioManager")  # type: ignore
        return _ScenarioManager
