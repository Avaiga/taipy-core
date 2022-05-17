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

import os
import pathlib

import pytest

from taipy.core.common.alias import DataNodeId
from taipy.core.common.scope import Scope
from taipy.core.config.config import Config
from taipy.core.config.data_node_config import DataNodeConfig
from taipy.core.data._data_manager import _DataManager
from taipy.core.data.csv import CSVDataNode
from taipy.core.data.in_memory import InMemoryDataNode
from taipy.core.data.pickle import PickleDataNode
from taipy.core.exceptions.exceptions import InvalidDataNodeType, ModelNotFound


def file_exists(file_path: str) -> bool:
    return os.path.exists(file_path)


class TestDataManager:
    def test_create_data_node_and_modify_properties_does_not_modify_config(self):
        dn_config = Config.configure_data_node(id="name", foo="bar")
        dn = _DataManager._create_and_set(dn_config, None)
        assert dn_config.properties.get("foo") == "bar"
        assert dn_config.properties.get("baz") is None

        # dn._properties["baz"] = "qux"
        dn.properties["baz"] = "qux"
        _DataManager._set(dn)
        assert dn_config.properties.get("foo") == "bar"
        assert dn_config.properties.get("baz") is None
        assert dn.properties.get("foo") == "bar"
        assert dn.properties.get("baz") == "qux"

    def test_create_and_get_csv_data_node(self):
        # Test we can instantiate a CsvDataNode from DataNodeConfig with :
        # - a csv type
        # - a default scenario scope
        # - No parent_id
        csv_dn_config = Config.configure_data_node(id="foo", storage_type="csv", path="bar", has_header=True)
        csv_dn = _DataManager._create_and_set(csv_dn_config, None)

        assert isinstance(csv_dn, CSVDataNode)
        assert isinstance(_DataManager._get(csv_dn.id), CSVDataNode)

        assert _DataManager._get(csv_dn.id) is not None
        assert _DataManager._get(csv_dn.id).id == csv_dn.id
        assert _DataManager._get(csv_dn.id).config_id == "foo"
        assert _DataManager._get(csv_dn.id).config_id == csv_dn.config_id
        assert _DataManager._get(csv_dn.id).scope == Scope.SCENARIO
        assert _DataManager._get(csv_dn.id).scope == csv_dn.scope
        assert _DataManager._get(csv_dn.id).parent_id is None
        assert _DataManager._get(csv_dn.id).parent_id == csv_dn.parent_id
        assert _DataManager._get(csv_dn.id).last_edition_date is None
        assert _DataManager._get(csv_dn.id).last_edition_date == csv_dn.last_edition_date
        assert _DataManager._get(csv_dn.id).job_ids == []
        assert _DataManager._get(csv_dn.id).job_ids == csv_dn.job_ids
        assert not _DataManager._get(csv_dn.id).is_ready_for_reading
        assert _DataManager._get(csv_dn.id).is_ready_for_reading == csv_dn.is_ready_for_reading
        assert len(_DataManager._get(csv_dn.id).properties) == 3
        assert not _DataManager._get(csv_dn.id).properties["cacheable"]
        assert _DataManager._get(csv_dn.id).properties.get("path") == "bar"
        assert _DataManager._get(csv_dn.id).properties.get("has_header")
        assert _DataManager._get(csv_dn.id).properties == csv_dn.properties

        assert _DataManager._get(csv_dn) is not None
        assert _DataManager._get(csv_dn).id == csv_dn.id
        assert _DataManager._get(csv_dn).config_id == "foo"
        assert _DataManager._get(csv_dn).config_id == csv_dn.config_id
        assert _DataManager._get(csv_dn).scope == Scope.SCENARIO
        assert _DataManager._get(csv_dn).scope == csv_dn.scope
        assert _DataManager._get(csv_dn).parent_id is None
        assert _DataManager._get(csv_dn).parent_id == csv_dn.parent_id
        assert _DataManager._get(csv_dn).last_edition_date is None
        assert _DataManager._get(csv_dn).last_edition_date == csv_dn.last_edition_date
        assert _DataManager._get(csv_dn).job_ids == []
        assert _DataManager._get(csv_dn).job_ids == csv_dn.job_ids
        assert not _DataManager._get(csv_dn).is_ready_for_reading
        assert _DataManager._get(csv_dn).is_ready_for_reading == csv_dn.is_ready_for_reading
        assert len(_DataManager._get(csv_dn).properties) == 3
        assert not _DataManager._get(csv_dn.id).properties["cacheable"]
        assert _DataManager._get(csv_dn).properties.get("path") == "bar"
        assert _DataManager._get(csv_dn).properties.get("has_header")
        assert _DataManager._get(csv_dn).properties == csv_dn.properties

    def test_create_and_get_in_memory_data_node(self):
        # Test we can instantiate an InMemoryDataNode from DataNodeConfig with :
        # - an in_memory type
        # - a scenario scope
        # - a parent id
        # - some default data
        in_memory_dn_config = Config.configure_data_node(
            id="baz",
            storage_type="in_memory",
            scope=Scope.SCENARIO,
            default_data="qux",
            other_data="foo",
            cacheable=True,
        )
        in_mem_dn = _DataManager._create_and_set(in_memory_dn_config, "Scenario_id")

        assert isinstance(in_mem_dn, InMemoryDataNode)
        assert isinstance(_DataManager._get(in_mem_dn.id), InMemoryDataNode)

        assert _DataManager._get(in_mem_dn.id) is not None
        assert _DataManager._get(in_mem_dn.id).id == in_mem_dn.id
        assert _DataManager._get(in_mem_dn.id).config_id == "baz"
        assert _DataManager._get(in_mem_dn.id).config_id == in_mem_dn.config_id
        assert _DataManager._get(in_mem_dn.id).scope == Scope.SCENARIO
        assert _DataManager._get(in_mem_dn.id).scope == in_mem_dn.scope
        assert _DataManager._get(in_mem_dn.id).parent_id == "Scenario_id"
        assert _DataManager._get(in_mem_dn.id).parent_id == in_mem_dn.parent_id
        assert _DataManager._get(in_mem_dn.id).last_edition_date is not None
        assert _DataManager._get(in_mem_dn.id).last_edition_date == in_mem_dn.last_edition_date
        assert _DataManager._get(in_mem_dn.id).job_ids == []
        assert _DataManager._get(in_mem_dn.id).job_ids == in_mem_dn.job_ids
        assert _DataManager._get(in_mem_dn.id).is_ready_for_reading
        assert _DataManager._get(in_mem_dn.id).is_ready_for_reading == in_mem_dn.is_ready_for_reading
        assert len(_DataManager._get(in_mem_dn.id).properties) == 2
        assert _DataManager._get(in_mem_dn.id).properties["cacheable"]
        assert _DataManager._get(in_mem_dn.id).properties.get("other_data") == "foo"
        assert _DataManager._get(in_mem_dn.id).properties == in_mem_dn.properties

        assert _DataManager._get(in_mem_dn) is not None
        assert _DataManager._get(in_mem_dn).id == in_mem_dn.id
        assert _DataManager._get(in_mem_dn).config_id == "baz"
        assert _DataManager._get(in_mem_dn).config_id == in_mem_dn.config_id
        assert _DataManager._get(in_mem_dn).scope == Scope.SCENARIO
        assert _DataManager._get(in_mem_dn).scope == in_mem_dn.scope
        assert _DataManager._get(in_mem_dn).parent_id == "Scenario_id"
        assert _DataManager._get(in_mem_dn).parent_id == in_mem_dn.parent_id
        assert _DataManager._get(in_mem_dn).last_edition_date is not None
        assert _DataManager._get(in_mem_dn).last_edition_date == in_mem_dn.last_edition_date
        assert _DataManager._get(in_mem_dn).job_ids == []
        assert _DataManager._get(in_mem_dn).job_ids == in_mem_dn.job_ids
        assert _DataManager._get(in_mem_dn).is_ready_for_reading
        assert _DataManager._get(in_mem_dn).is_ready_for_reading == in_mem_dn.is_ready_for_reading
        assert len(_DataManager._get(in_mem_dn).properties) == 2
        assert _DataManager._get(in_mem_dn.id).properties["cacheable"]
        assert _DataManager._get(in_mem_dn).properties.get("other_data") == "foo"
        assert _DataManager._get(in_mem_dn).properties == in_mem_dn.properties

    def test_create_and_get_pickle_data_node(self):
        # Test we can instantiate a PickleDataNode from DataNodeConfig with :
        # - an in_memory type
        # - a business cycle scope
        # - No parent id
        # - no default data
        dn_config = Config.configure_data_node(id="plop", storage_type="pickle", scope=Scope.CYCLE)
        pickle_dn = _DataManager._create_and_set(dn_config, None)

        assert isinstance(pickle_dn, PickleDataNode)
        assert isinstance(_DataManager._get(pickle_dn.id), PickleDataNode)

        assert _DataManager._get(pickle_dn.id) is not None
        assert _DataManager._get(pickle_dn.id).id == pickle_dn.id
        assert _DataManager._get(pickle_dn.id).config_id == "plop"
        assert _DataManager._get(pickle_dn.id).config_id == pickle_dn.config_id
        assert _DataManager._get(pickle_dn.id).scope == Scope.CYCLE
        assert _DataManager._get(pickle_dn.id).scope == pickle_dn.scope
        assert _DataManager._get(pickle_dn.id).parent_id is None
        assert _DataManager._get(pickle_dn.id).parent_id == pickle_dn.parent_id
        assert _DataManager._get(pickle_dn.id).last_edition_date is None
        assert _DataManager._get(pickle_dn.id).last_edition_date == pickle_dn.last_edition_date
        assert _DataManager._get(pickle_dn.id).job_ids == []
        assert _DataManager._get(pickle_dn.id).job_ids == pickle_dn.job_ids
        assert not _DataManager._get(pickle_dn.id).is_ready_for_reading
        assert _DataManager._get(pickle_dn.id).is_ready_for_reading == pickle_dn.is_ready_for_reading
        assert len(_DataManager._get(pickle_dn.id).properties) == 1
        assert not _DataManager._get(pickle_dn.id).properties["cacheable"]
        assert _DataManager._get(pickle_dn.id).properties == pickle_dn.properties

        assert _DataManager._get(pickle_dn) is not None
        assert _DataManager._get(pickle_dn).id == pickle_dn.id
        assert _DataManager._get(pickle_dn).config_id == "plop"
        assert _DataManager._get(pickle_dn).config_id == pickle_dn.config_id
        assert _DataManager._get(pickle_dn).scope == Scope.CYCLE
        assert _DataManager._get(pickle_dn).scope == pickle_dn.scope
        assert _DataManager._get(pickle_dn).parent_id is None
        assert _DataManager._get(pickle_dn).parent_id == pickle_dn.parent_id
        assert _DataManager._get(pickle_dn).last_edition_date is None
        assert _DataManager._get(pickle_dn).last_edition_date == pickle_dn.last_edition_date
        assert _DataManager._get(pickle_dn).job_ids == []
        assert _DataManager._get(pickle_dn).job_ids == pickle_dn.job_ids
        assert not _DataManager._get(pickle_dn).is_ready_for_reading
        assert _DataManager._get(pickle_dn).is_ready_for_reading == pickle_dn.is_ready_for_reading
        assert len(_DataManager._get(pickle_dn).properties) == 1
        assert not _DataManager._get(pickle_dn.id).properties["cacheable"]
        assert _DataManager._get(pickle_dn).properties == pickle_dn.properties

    def test_create_raises_exception_with_wrong_type(self):
        wrong_type_dn_config = DataNodeConfig(id="foo", storage_type="bar", scope=DataNodeConfig._DEFAULT_SCOPE)
        with pytest.raises(InvalidDataNodeType):
            _DataManager._create_and_set(wrong_type_dn_config, None)

    def test_create_from_same_config_generates_new_data_node_and_new_id(self):
        dn_config = Config.configure_data_node(id="foo", storage_type="in_memory")
        dn = _DataManager._create_and_set(dn_config, None)
        dn_2 = _DataManager._create_and_set(dn_config, None)
        assert dn_2.id != dn.id

    def test_create_uses_overridden_attributes_in_config_file(self):
        Config.load(os.path.join(pathlib.Path(__file__).parent.resolve(), "data_sample/config.toml"))

        csv_dn = Config.configure_data_node(id="foo", storage_type="csv", path="bar", has_header=True)
        csv = _DataManager._create_and_set(csv_dn, None)
        assert csv.config_id == "foo"
        assert isinstance(csv, CSVDataNode)
        assert csv.path == "path_from_config_file"
        assert csv.has_header

        csv_dn = Config.configure_data_node(id="baz", storage_type="csv", path="bar", has_header=True)
        csv = _DataManager._create_and_set(csv_dn, None)
        assert csv.config_id == "baz"
        assert isinstance(csv, CSVDataNode)
        assert csv.path == "bar"
        assert csv.has_header

    def test_get_if_not_exists(self):
        with pytest.raises(ModelNotFound):
            _DataManager._repository.load("test_data_node_2")

    def test_get_all(self):
        assert len(_DataManager._get_all()) == 0
        dn_config_1 = Config.configure_data_node(id="foo", storage_type="in_memory")
        _DataManager._create_and_set(dn_config_1, None)
        assert len(_DataManager._get_all()) == 1
        dn_config_2 = Config.configure_data_node(id="baz", storage_type="in_memory")
        _DataManager._create_and_set(dn_config_2, None)
        _DataManager._create_and_set(dn_config_2, None)
        assert len(_DataManager._get_all()) == 3
        assert len([dn for dn in _DataManager._get_all() if dn.config_id == "foo"]) == 1
        assert len([dn for dn in _DataManager._get_all() if dn.config_id == "baz"]) == 2

    def test_set(self):
        dn = InMemoryDataNode(
            "config_id",
            Scope.PIPELINE,
            id=DataNodeId("id"),
            parent_id=None,
            last_edit_date=None,
            job_ids=[],
            edit_in_progress=False,
            properties={"foo": "bar"},
        )
        assert len(_DataManager._get_all()) == 0
        _DataManager._set(dn)
        assert len(_DataManager._get_all()) == 1

        # changing data node attribute
        dn.config_id = "foo"
        assert dn.config_id == "foo"
        _DataManager._set(dn)
        assert len(_DataManager._get_all()) == 1
        assert dn.config_id == "foo"
        assert _DataManager._get(dn.id).config_id == "foo"

    def test_delete(self):
        dn_1 = InMemoryDataNode("config_id", Scope.PIPELINE, id="id_1")
        dn_2 = InMemoryDataNode("config_id", Scope.PIPELINE, id="id_2")
        dn_3 = InMemoryDataNode("config_id", Scope.PIPELINE, id="id_3")
        assert len(_DataManager._get_all()) == 0
        _DataManager._set(dn_1)
        _DataManager._set(dn_2)
        _DataManager._set(dn_3)
        assert len(_DataManager._get_all()) == 3
        _DataManager._delete(dn_1.id)
        assert len(_DataManager._get_all()) == 2
        assert _DataManager._get(dn_2.id).id == dn_2.id
        assert _DataManager._get(dn_3.id).id == dn_3.id
        assert _DataManager._get(dn_1.id) is None
        _DataManager._delete_all()
        assert len(_DataManager._get_all()) == 0

    def test_get_or_create(self):
        _DataManager._delete_all()

        global_dn_config = Config.configure_data_node(
            id="test_data_node", storage_type="in_memory", scope=Scope.GLOBAL, data="In memory Data Node"
        )
        scenario_dn_config = Config.configure_data_node(
            id="test_data_node2", storage_type="in_memory", scope=Scope.SCENARIO, data="In memory scenario"
        )
        pipeline_dn_config = Config.configure_data_node(
            id="test_data_node2", storage_type="in_memory", scope=Scope.PIPELINE, data="In memory pipeline"
        )

        assert len(_DataManager._get_all()) == 0
        global_dn = _DataManager._get_or_create(global_dn_config, None, None)
        assert len(_DataManager._get_all()) == 1
        global_dn_bis = _DataManager._get_or_create(global_dn_config, None)
        assert len(_DataManager._get_all()) == 1
        assert global_dn.id == global_dn_bis.id

        scenario_dn = _DataManager._get_or_create(scenario_dn_config, "scenario_id")
        assert len(_DataManager._get_all()) == 2
        scenario_dn_bis = _DataManager._get_or_create(scenario_dn_config, "scenario_id")
        assert len(_DataManager._get_all()) == 2
        assert scenario_dn.id == scenario_dn_bis.id
        scenario_dn_ter = _DataManager._get_or_create(scenario_dn_config, "scenario_id", "whatever_pipeline_id")
        assert len(_DataManager._get_all()) == 2
        assert scenario_dn.id == scenario_dn_bis.id
        assert scenario_dn_bis.id == scenario_dn_ter.id
        scenario_dn_quater = _DataManager._get_or_create(scenario_dn_config, "scenario_id_2")
        assert len(_DataManager._get_all()) == 3
        assert scenario_dn.id == scenario_dn_bis.id
        assert scenario_dn_bis.id == scenario_dn_ter.id
        assert scenario_dn_ter.id != scenario_dn_quater.id

        pipeline_dn = _DataManager._get_or_create(pipeline_dn_config, "scenario_id", "pipeline_1")
        assert len(_DataManager._get_all()) == 4
        pipeline_dn_bis = _DataManager._get_or_create(pipeline_dn_config, "scenario_id", "pipeline_1")
        assert len(_DataManager._get_all()) == 4
        assert pipeline_dn.id == pipeline_dn_bis.id
        pipeline_dn_ter = _DataManager._get_or_create(pipeline_dn_config, "scenario_id", "pipeline_2")
        assert len(_DataManager._get_all()) == 5
        assert pipeline_dn.id == pipeline_dn_bis.id
        assert pipeline_dn.id != pipeline_dn_ter.id
        pipeline_dn_quater = _DataManager._get_or_create(pipeline_dn_config, "other_scenario_id", "pipeline_2")
        assert len(_DataManager._get_all()) == 5
        assert pipeline_dn.id == pipeline_dn_bis.id
        assert pipeline_dn_bis.id != pipeline_dn_ter.id
        assert pipeline_dn_ter.id == pipeline_dn_quater.id

        pipeline_dn_config.id = "test_data_node4"
        pipeline_dn_quinquies = _DataManager._get_or_create(pipeline_dn_config, None)
        assert len(_DataManager._get_all()) == 6
        assert pipeline_dn.id == pipeline_dn_bis.id
        assert pipeline_dn_bis.id != pipeline_dn_ter.id
        assert pipeline_dn_bis.id != pipeline_dn_quinquies.id
        assert pipeline_dn_ter.id != pipeline_dn_quinquies.id

    def test_ensure_persistence_of_data_node(self):
        dm = _DataManager()
        dm._delete_all()

        dn_config_1 = Config.configure_data_node(
            id="data_node_1", storage_type="in_memory", data="In memory pipeline 2"
        )
        dn_config_2 = Config.configure_data_node(
            id="data_node_2", storage_type="in_memory", data="In memory pipeline 2"
        )

        # Create and save
        dm._get_or_create(dn_config_1)
        dm._get_or_create(dn_config_2)
        assert len(dm._get_all()) == 2

        # Delete the DataManager to ensure it's get from the storage system
        del dm
        dm = _DataManager()
        dm._get_or_create(dn_config_1)
        assert len(dm._get_all()) == 2

        dm._delete_all()

    def test_clean_generated_pickle_files(self, pickle_file_path):
        user_pickle_dn_config = Config.configure_data_node(
            id="d1", storage_type="pickle", path=pickle_file_path, default_data="d"
        )
        generated_pickle_dn_config_1 = Config.configure_data_node(id="d2", storage_type="pickle", default_data="d")
        generated_pickle_dn_config_2 = Config.configure_data_node(id="d3", storage_type="pickle", default_data="d")

        user_pickle_dn = _DataManager._get_or_create(user_pickle_dn_config)
        generated_pickle_dn_1 = _DataManager._get_or_create(generated_pickle_dn_config_1)
        generated_pickle_dn_2 = _DataManager._get_or_create(generated_pickle_dn_config_2)

        _DataManager._clean_pickle_file(user_pickle_dn.id)
        assert file_exists(user_pickle_dn.path)

        _DataManager._clean_pickle_files([generated_pickle_dn_1.id, generated_pickle_dn_2])
        assert not file_exists(generated_pickle_dn_1.path)
        assert not file_exists(generated_pickle_dn_2.path)

    def test_delete_does_clean_generated_pickle_files(self, pickle_file_path):
        user_pickle_dn_config = Config.configure_data_node(
            id="d1", storage_type="pickle", path=pickle_file_path, default_data="d"
        )
        generated_pickle_dn_config_1 = Config.configure_data_node(id="d2", storage_type="pickle", default_data="d")
        generated_pickle_dn_config_2 = Config.configure_data_node(id="d3", storage_type="pickle", default_data="d")
        generated_pickle_dn_config_3 = Config.configure_data_node(id="d4", storage_type="pickle", default_data="d")

        user_pickle_dn = _DataManager._get_or_create(user_pickle_dn_config)
        generated_pickle_dn_1 = _DataManager._get_or_create(generated_pickle_dn_config_1)
        generated_pickle_dn_2 = _DataManager._get_or_create(generated_pickle_dn_config_2)
        generated_pickle_dn_3 = _DataManager._get_or_create(generated_pickle_dn_config_3)

        _DataManager._delete(user_pickle_dn.id)
        assert file_exists(user_pickle_dn.path)

        _DataManager._delete_many([generated_pickle_dn_1.id, generated_pickle_dn_2.id])
        assert not file_exists(generated_pickle_dn_1.path)
        assert not file_exists(generated_pickle_dn_2.path)

        _DataManager._delete_all()
        assert not file_exists(generated_pickle_dn_3.path)
