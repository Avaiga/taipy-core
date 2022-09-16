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

import copy
import datetime
import json
from dataclasses import dataclass
from enum import Enum
from unittest import mock

import mongomock
import numpy as np
import pandas as pd
import pymongo
import pytest

from src.taipy.core.common.alias import DataNodeId
from src.taipy.core.data.mongo import MongoDataNode
from src.taipy.core.exceptions.exceptions import InvalidExposedType, MissingRequiredProperty
from taipy.config.common.scope import Scope


class MyCustomObject:
    def __init__(self, foo=None, bar=None, *args, **kwargs):
        self.foo = foo
        self.bar = bar
        self.args = args
        self.kwargs = kwargs


class AnotherCustomObject:
    def __init__(self, id, integer, text):
        self.id = id
        self.integer = integer
        self.text = text


class MyCustomEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, AnotherCustomObject):
            return {"__type__": "AnotherCustomObject", "id": o.id, "integer": o.integer, "text": o.text}
        return super().default(self, o)


class MyCustomDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, o):
        if o.get("__type__") == "AnotherCustomObject":
            return AnotherCustomObject(o["id"], o["integer"], o["text"])
        else:
            return o


class TestMongoDataNode:
    __properties = [
        {
            "db_username": "",
            "db_password": "",
            "db_name": "taipy",
            "collection_name": "test",
            "read_query": {},
            "write_table": "foo",
            "db_extra_args": {
                "TrustServerCertificate": "yes",
            },
        }
    ]

    @pytest.mark.parametrize("properties", __properties)
    def test_create(self, properties):
        dn = MongoDataNode(
            "foo_bar",
            Scope.PIPELINE,
            properties=properties,
        )
        assert isinstance(dn, MongoDataNode)
        assert dn.storage_type() == "mongo"
        assert dn.config_id == "foo_bar"
        assert dn.scope == Scope.PIPELINE
        assert dn.id is not None
        assert dn.parent_id is None
        assert dn.job_ids == []
        assert dn.is_ready_for_reading
        assert dn.read_query != ""
        assert dn.exposed_type == "pandas"

    @pytest.mark.parametrize(
        "properties",
        [
            {},
            {"db_username": "foo"},
            {"db_username": "foo", "db_password": "foo"},
            {"db_username": "foo", "db_password": "foo", "db_name": "foo"},
        ],
    )
    def test_create_with_missing_parameters(self, properties):
        with pytest.raises(MissingRequiredProperty):
            MongoDataNode("foo", Scope.PIPELINE, DataNodeId("dn_id"))
        with pytest.raises(MissingRequiredProperty):
            MongoDataNode("foo", Scope.PIPELINE, DataNodeId("dn_id"), properties=properties)

    @mock.patch("src.taipy.core.data.mongo.MongoDataNode._read_as", return_value="custom")
    @mock.patch("src.taipy.core.data.mongo.MongoDataNode._read_as_pandas_dataframe", return_value="pandas")
    @mock.patch("src.taipy.core.data.mongo.MongoDataNode._read_as_numpy", return_value="numpy")
    @pytest.mark.parametrize("properties", __properties)
    def test_read(self, mock_read_as, mock_read_as_pandas_dataframe, mock_read_as_numpy, properties):

        # Create MongoDataNode without exposed_type (Default is pandas.DataFrame)
        mongo_data_node_as_pandas = MongoDataNode(
            "foo",
            Scope.PIPELINE,
            properties=properties,
        )

        assert mongo_data_node_as_pandas.read() == "pandas"

        # Create the same MongoDataNode but with custom exposed_type
        custom_properties = properties.copy()
        custom_properties["exposed_type"] = MyCustomObject
        mongo_data_node_as_custom_object = MongoDataNode(
            "foo",
            Scope.PIPELINE,
            properties=custom_properties,
        )
        assert mongo_data_node_as_custom_object.read() == "custom"

        # Create the same MongoDataSource but with numpy exposed_type
        custom_properties = properties.copy()
        custom_properties["exposed_type"] = "numpy"
        mongo_data_source_as_numpy_object = MongoDataNode(
            "foo",
            Scope.PIPELINE,
            properties=custom_properties,
        )

        assert mongo_data_source_as_numpy_object.read() == "numpy"

    @mongomock.patch(servers=(("localhost", 27017),))
    @pytest.mark.parametrize("properties", __properties)
    def test_read_as(self, properties):
        mock_client = pymongo.MongoClient("localhost")
        mock_client[properties["db_name"]][properties["collection_name"]].insert_many(
            [
                {"foo": "baz", "bar": "qux"},
                {"foo": "quux", "bar": "quuz"},
                {"foo": "corge"},
                {"bar": "grault"},
                {"KWARGS_KEY": "KWARGS_VALUE"},
                {},
            ]
        )

        custom_properties = properties.copy()
        custom_properties["exposed_type"] = MyCustomObject
        mongo_data_node = MongoDataNode(
            "foo",
            Scope.PIPELINE,
            properties=custom_properties,
        )

        data = mongo_data_node._read_as()

        assert isinstance(data, list)
        assert isinstance(data[0], MyCustomObject)
        assert isinstance(data[1], MyCustomObject)
        assert isinstance(data[2], MyCustomObject)
        assert isinstance(data[3], MyCustomObject)
        assert isinstance(data[4], MyCustomObject)
        assert isinstance(data[5], MyCustomObject)

        assert data[0].foo == "baz"
        assert data[0].bar == "qux"
        assert data[1].foo == "quux"
        assert data[1].bar == "quuz"
        assert data[2].foo == "corge"
        assert data[2].bar is None
        assert data[3].foo is None
        assert data[3].bar == "grault"
        assert data[4].foo is None
        assert data[4].bar is None
        assert data[4].kwargs["KWARGS_KEY"] == "KWARGS_VALUE"
        assert data[5].foo is None
        assert data[5].bar is None
        assert len(data[5].args) == 0
        assert len(data[5].kwargs) == 0

    @mongomock.patch(servers=(("localhost", 27017),))
    @pytest.mark.parametrize("properties", __properties)
    def test_read_empty_as(self, properties):
        custom_properties = properties.copy()
        custom_properties["exposed_type"] = MyCustomObject
        mongo_data_node = MongoDataNode(
            "foo",
            Scope.PIPELINE,
            properties=custom_properties,
        )
        data = mongo_data_node._read_as()
        assert isinstance(data, list)
        assert len(data) == 0

    @mongomock.patch(servers=(("localhost", 27017),))
    @pytest.mark.parametrize("properties", __properties)
    @pytest.mark.parametrize(
        "data,written_data",
        [
            ([{"a": 1, "b": 2}, {"a": 3, "b": 4}], [{"a": 1, "b": 2}, {"a": 3, "b": 4}]),
            ({"a": 1, "b": 2}, [{"a": 1, "b": 2}]),
        ],
    )
    def test_write(self, properties, data, written_data):
        dn = MongoDataNode("foo", Scope.PIPELINE, properties=properties)
        dn.write(data)

        assert np.array_equal(dn.read().values, np.array([list(data.values()) for data in written_data]))

    @pytest.mark.parametrize("properties", __properties)
    def test_raise_error_invalid_exposed_type(self, properties):
        custom_properties = properties.copy()
        custom_properties["exposed_type"] = "foo"
        with pytest.raises(InvalidExposedType):
            MongoDataNode(
                "foo",
                Scope.PIPELINE,
                properties=custom_properties,
            )

    @mongomock.patch(servers=(("localhost", 27017),))
    @pytest.mark.parametrize("properties", __properties)
    def test_write_dataframe(self, properties):
        df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})

        dn = MongoDataNode(
            "foo",
            Scope.PIPELINE,
            properties=properties,
        )
        dn.write(copy.deepcopy(df))

        assert dn.read().equals(df)

    @mongomock.patch(servers=(("localhost", 27017),))
    @pytest.mark.parametrize("properties", __properties)
    @pytest.mark.parametrize(
        "data",
        [
            [],
            {},
        ],
    )
    def test_write_empty_list(self, properties, data):
        dn = MongoDataNode(
            "foo",
            Scope.PIPELINE,
            properties=properties,
        )
        dn.write(data)

        assert dn.read().empty

    @pytest.mark.parametrize("properties", __properties)
    def test_write_non_serializable(self, properties):
        dn = MongoDataNode("foo", Scope.PIPELINE, properties=properties)
        data = {"a": 1, "b": dn}
        with pytest.raises(TypeError):
            dn.write(data)

    @pytest.mark.parametrize("properties", __properties)
    def test_write_date(self, properties):
        dn = MongoDataNode("foo", Scope.PIPELINE, properties=properties)
        now = datetime.datetime.now()
        now_str = now.isoformat()
        data = {"date": now}
        dn.write(data)
        read_data = dn.read()
        assert read_data["date"][0] == now_str

    @pytest.mark.parametrize("properties", __properties)
    def test_write_enum(self, properties):
        class MyEnum(Enum):
            A = 1
            B = 2
            C = 3

        data = {"data": [MyEnum.A, MyEnum.B, MyEnum.C]}

        dn = MongoDataNode("foo", Scope.PIPELINE, properties=properties)
        dn.write(data)
        read_data = dn.read()

        assert read_data.to_dict("records")[0] == {"data": [1, 2, 3]}

    @pytest.mark.parametrize("properties", __properties)
    def test_write_dataclass(self, properties):
        @dataclass
        class CustomDataclass:
            integer: int
            string: str

        dn = MongoDataNode("foo", Scope.PIPELINE, properties=properties)
        dn.write(CustomDataclass(integer=1, string="foo"))
        read_data = dn.read()
        assert read_data["integer"][0] == 1
        assert read_data["string"][0] == "foo"

    @pytest.mark.parametrize("properties", __properties)
    def test_write_custom_encoder(self, properties):
        custom_properties = properties.copy()
        custom_properties["encoder"] = MyCustomEncoder
        dn = MongoDataNode("foo", Scope.PIPELINE, properties=custom_properties)
        data = [{"data": AnotherCustomObject("1", 1, "abc"), "data2": 100}]
        dn.write(data)

        read_data = dn.read()

        assert read_data["data"][0]["__type__"] == "AnotherCustomObject"
        assert read_data["data"][0]["id"] == "1"
        assert read_data["data"][0]["integer"] == 1
        assert read_data["data"][0]["text"] == "abc"
        assert read_data["data2"][0] == 100

    @pytest.mark.parametrize("properties", __properties)
    def test_read_write_custom_encoder_decoder(self, properties):
        custom_properties = properties.copy()
        custom_properties["encoder"] = MyCustomEncoder
        custom_properties["decoder"] = MyCustomDecoder
        dn = MongoDataNode("foo", Scope.PIPELINE, properties=custom_properties)
        data = [{"data": AnotherCustomObject("1", 1, "abc"), "data2": 100}]
        dn.write(data)

        read_data = dn.read()
        print("read_data: ", read_data, "\n\n")

        assert isinstance(read_data["data"][0], AnotherCustomObject)
        assert read_data["data"][0].id == "1"
        assert read_data["data"][0].integer == 1
        assert read_data["data"][0].text == "abc"
        assert read_data["data2"][0] == 100
