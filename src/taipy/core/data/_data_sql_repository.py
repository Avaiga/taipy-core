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

# © 2022, Avaiga Pte Ltd. All Rights Reserved. The use of the Taipy software and any part thereof is governed by Avaiga
# Pte Ltd’s Software License and Maintenance Agreement. Unauthorised use, reproduction and modification is strictly not
# allowed.
from typing import Optional

from ._data_model import _DataNodeModel
from ._data_repository import _DataRepository


class _DataSQLRepository(_DataRepository):
    def __init__(self):
        super().__init__(model=_DataNodeModel, model_name="data")

    def _get_by_config_and_parent_id(self, config_id: str, parent_id: Optional[str]):
        super().repository._get_by_config_and_parent_id(config_id, parent_id)

    def _get_by_configs_and_parent_ids(self, configs_and_parent_ids):
        return super().repository._get_by_configs_and_parent_ids(configs_and_parent_ids)
