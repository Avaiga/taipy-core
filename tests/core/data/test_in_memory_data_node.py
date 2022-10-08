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

import pytest

from src.taipy.core.common.alias import DataNodeId
from src.taipy.core.data.in_memory import InMemoryDataNode
from src.taipy.core.exceptions.exceptions import NoData
from taipy.config.common.scope import Scope
from taipy.config.exceptions.exceptions import InvalidConfigurationId


class TestInMemoryDataNodeEntity:
    def test_create(self):
        dn = InMemoryDataNode(
            "foobar_bazy",
            Scope.SCENARIO,
            DataNodeId("id_uio"),
            "my name",
            "owner_id",
            properties={"default_data": "In memory Data Node"},
        )
        assert isinstance(dn, InMemoryDataNode)
        assert dn.storage_type() == "in_memory"
        assert dn.config_id == "foobar_bazy"
        assert dn.scope == Scope.SCENARIO
        assert dn.id == "id_uio"
        assert dn.name == "my name"
        assert dn.owner_id == "owner_id"
        assert dn.last_edition_date is not None
        assert dn.job_ids == []
        assert dn.is_ready_for_reading
        assert dn.read() == "In memory Data Node"

        dn_2 = InMemoryDataNode("foo", Scope.PIPELINE)
        assert dn_2.last_edition_date is None
        assert not dn_2.is_ready_for_reading

        with pytest.raises(InvalidConfigurationId):
            InMemoryDataNode("foo bar", Scope.PIPELINE, DataNodeId("dn_id"))

    def test_read_and_write(self):
        no_data_dn = InMemoryDataNode("foo", Scope.PIPELINE, DataNodeId("dn_id"))
        with pytest.raises(NoData):
            assert no_data_dn.read() is None
            no_data_dn.read_or_raise()
        in_mem_dn = InMemoryDataNode("foo", Scope.PIPELINE, properties={"default_data": "bar"})
        assert isinstance(in_mem_dn.read(), str)
        assert in_mem_dn.read() == "bar"
        in_mem_dn.properties["default_data"] = "baz"  # this modifies the default data value but not the data itself
        assert in_mem_dn.read() == "bar"
        in_mem_dn.write("qux")
        assert in_mem_dn.read() == "qux"
        in_mem_dn.write(1998)
        assert isinstance(in_mem_dn.read(), int)
        assert in_mem_dn.read() == 1998
