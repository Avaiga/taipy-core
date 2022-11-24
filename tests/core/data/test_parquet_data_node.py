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
from datetime import datetime
from time import sleep

import modin.pandas as modin_pd
import numpy as np
import pandas as pd
import pytest

from src.taipy.core.common.alias import DataNodeId
from src.taipy.core.data._data_manager import _DataManager
from src.taipy.core.data.parquet import ParquetDataNode
from src.taipy.core.exceptions.exceptions import InvalidExposedType, MissingRequiredProperty, NoData
from taipy.config.common.scope import Scope
from taipy.config.config import Config
from taipy.config.exceptions.exceptions import InvalidConfigurationId


class MyCustomObject:
    def __init__(self, id, integer, text):
        self.id = id
        self.integer = integer
        self.text = text


class MyOtherCustomObject:
    def __init__(self, id, sentence):
        self.id = id
        self.sentence = sentence


def create_custom_class(**kwargs):
    return MyOtherCustomObject(id=kwargs["id"], sentence=kwargs["text"])


class TestParquetDataNode:
    def test_create(self):
        path = "data/node/path"
        compression = "snappy"
        dn = ParquetDataNode(
            "foo_bar", Scope.PIPELINE, name="super name", properties={"path": path, "compression": compression}
        )
        assert isinstance(dn, ParquetDataNode)
        assert dn.storage_type() == "parquet"
        assert dn.config_id == "foo_bar"
        assert dn.name == "super name"
        assert dn.scope == Scope.PIPELINE
        assert dn.id is not None
        assert dn.owner_id is None
        assert dn.last_edition_date is None
        assert dn.job_ids == []
        assert not dn.is_ready_for_reading
        assert dn.path == path
        assert dn.exposed_type == "pandas"
        assert dn.compression == "snappy"

        with pytest.raises(InvalidConfigurationId):
            dn = ParquetDataNode("foo bar", Scope.PIPELINE, name="super name", properties={"path": path})

    def test_new_parquet_data_node_with_existing_file_is_ready_for_reading(self, parquet_file_path):
        not_ready_dn_cfg = Config.configure_data_node(
            "not_ready_data_node_config_id", "parquet", path="NOT_EXISTING.parquet"
        )
        not_ready_dn = _DataManager._bulk_get_or_create([not_ready_dn_cfg])[not_ready_dn_cfg]
        assert not not_ready_dn.is_ready_for_reading

        path = str(parquet_file_path)
        ready_dn_cfg = Config.configure_data_node("ready_data_node_config_id", "parquet", path=path)
        ready_dn = _DataManager._bulk_get_or_create([ready_dn_cfg])[ready_dn_cfg]
        assert ready_dn.is_ready_for_reading

    def test_create_with_missing_parameters(self):
        with pytest.raises(MissingRequiredProperty):
            ParquetDataNode("foo", Scope.PIPELINE, DataNodeId("dn_id"))
        with pytest.raises(MissingRequiredProperty):
            ParquetDataNode("foo", Scope.PIPELINE, DataNodeId("dn_id"), properties={})

    def test_read_with_exposed_types(self, parquet_file_path):
        not_existing_parquet = ParquetDataNode("foo", Scope.PIPELINE, properties={"path": "nonexistent.parquet"})
        with pytest.raises(NoData):
            assert not_existing_parquet.read() is None
            not_existing_parquet.read_or_raise()

        path = str(parquet_file_path)
        df = pd.read_parquet(path)
        # Create ParquetDataNode without exposed_type (Default is pandas.DataFrame)
        parquet_data_node_as_pandas = ParquetDataNode("bar", Scope.PIPELINE, properties={"path": path})
        data_pandas = parquet_data_node_as_pandas.read()
        assert isinstance(data_pandas, pd.DataFrame)
        assert len(data_pandas) == 2
        assert data_pandas.equals(df)
        assert np.array_equal(data_pandas.to_numpy(), df.to_numpy())

        # Create ParquetDataNode with modin exposed_type
        parquet_data_node_as_modin = ParquetDataNode(
            "bar", Scope.PIPELINE, properties={"path": path, "exposed_type": "modin"}
        )
        data_modin = parquet_data_node_as_modin.read()
        assert isinstance(data_modin, modin_pd.DataFrame)
        assert len(data_modin) == 2
        assert data_modin.equals(df)
        assert np.array_equal(data_modin.to_numpy(), df.to_numpy())

        # Create ParquetDataNode with numpy exposed_type
        parquet_data_node_as_numpy = ParquetDataNode(
            "bar", Scope.PIPELINE, properties={"path": path, "has_header": True, "exposed_type": "numpy"}
        )
        data_numpy = parquet_data_node_as_numpy.read()
        assert isinstance(data_numpy, np.ndarray)
        assert len(data_numpy) == 2
        assert np.array_equal(data_numpy, df.to_numpy())

    def test_set_path(self):
        dn = ParquetDataNode("foo", Scope.PIPELINE, properties={"path": "foo.parquet"})
        assert dn.path == "foo.parquet"
        dn.path = "bar.parquet"
        assert dn.path == "bar.parquet"

    def test_raise_error_when_path_not_exist(self):
        with pytest.raises(MissingRequiredProperty):
            ParquetDataNode("foo", Scope.PIPELINE)

    def test_read_write_after_modify_path(self, parquet_file_path):
        path = str(parquet_file_path)
        new_path = str(pathlib.Path(parquet_file_path).with_name("nonexistent.parquet"))
        dn = ParquetDataNode("foo", Scope.PIPELINE, properties={"path": path})
        read_data = dn.read()
        assert read_data is not None
        dn.path = new_path
        with pytest.raises(FileNotFoundError):
            dn.read()
        dn.write(read_data)
        assert dn.read().equals(read_data)

    def test_pandas_exposed_type(self, parquet_file_path):
        dn = ParquetDataNode("foo", Scope.PIPELINE, properties={"path": parquet_file_path, "exposed_type": "pandas"})
        assert isinstance(dn.read(), pd.DataFrame)

    def test_numpy_exposed_type(self, parquet_file_path):
        dn = ParquetDataNode("foo", Scope.PIPELINE, properties={"path": parquet_file_path, "exposed_type": "numpy"})
        assert isinstance(dn.read(), np.ndarray)

    def test_custom_exposed_type(self):
        example_parquet_path = os.path.join(pathlib.Path(__file__).parent.resolve(), "data_sample/example.parquet")

        dn = ParquetDataNode(
            "foo", Scope.PIPELINE, properties={"path": example_parquet_path, "exposed_type": MyCustomObject}
        )
        assert all([isinstance(obj, MyCustomObject) for obj in dn.read()])

        dn = ParquetDataNode(
            "foo", Scope.PIPELINE, properties={"path": example_parquet_path, "exposed_type": create_custom_class}
        )
        assert all([isinstance(obj, MyOtherCustomObject) for obj in dn.read()])

    def test_raise_error_invalid_exposed_type(self):
        path = os.path.join(pathlib.Path(__file__).parent.resolve(), "data_sample/example.parquet")
        with pytest.raises(InvalidExposedType):
            ParquetDataNode("foo", Scope.PIPELINE, properties={"path": path, "exposed_type": "foo"})

    def test_read_empty_data(self, tmpdir_factory):
        temp_file_path = str(tmpdir_factory.mktemp("data").join("temp.parquet"))
        empty_df = pd.DataFrame([])
        empty_df.to_parquet(temp_file_path)

        # Pandas
        dn = ParquetDataNode("foo", Scope.PIPELINE, properties={"path": temp_file_path, "exposed_type": "pandas"})
        assert dn.read().equals(empty_df)

        # Numpy
        dn = ParquetDataNode("foo", Scope.PIPELINE, properties={"path": temp_file_path, "exposed_type": "numpy"})
        assert np.array_equal(dn.read(), empty_df.to_numpy())

        # Custom
        dn = ParquetDataNode("foo", Scope.PIPELINE, properties={"path": temp_file_path, "exposed_type": MyCustomObject})
        assert dn.read() == []

    def test_get_system_modified_date_instead_of_last_edit_date(self, tmpdir_factory):
        temp_file_path = str(tmpdir_factory.mktemp("data").join("temp.parquet"))
        pd.DataFrame([]).to_parquet(temp_file_path)
        dn = ParquetDataNode("foo", Scope.PIPELINE, properties={"path": temp_file_path, "exposed_type": "pandas"})

        dn.write(pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]}))
        previous_edit_date = dn.last_edit_date

        sleep(0.1)

        pd.DataFrame(pd.DataFrame(data={"col1": [5, 6], "col2": [7, 8]})).to_parquet(temp_file_path)
        new_edit_date = datetime.fromtimestamp(os.path.getmtime(temp_file_path))

        assert previous_edit_date < dn.last_edit_date
        assert new_edit_date == dn.last_edit_date

        sleep(0.1)

        dn.write(pd.DataFrame(data={"col1": [9, 10], "col2": [10, 12]}))
        assert new_edit_date < dn.last_edit_date
        os.unlink(temp_file_path)

    def test_write_to_disk(self, tmpdir_factory):
        temp_file_path = str(tmpdir_factory.mktemp("data").join("temp.parquet"))
        dn = ParquetDataNode("foo", Scope.PIPELINE, properties={"path": temp_file_path})

        example_csv_path = os.path.join(pathlib.Path(__file__).parent.resolve(), "data_sample/example.csv")
        df = pd.read_csv(example_csv_path)
        dn.write(df)

        assert pathlib.Path(temp_file_path).exists()
        assert isinstance(dn.read(), pd.DataFrame)

    def test_pandas_parquet_defaults(self, default_data_frame: pd.DataFrame):
        # If Pandas changes their defaults, we may consider doing the same
        pyarrow_snappy_bytes = default_data_frame.to_parquet(engine="pyarrow", compression="snappy")

        assumed_default_engine = "pyarrow"
        default_compression_bytes = default_data_frame.to_parquet(engine=assumed_default_engine)
        assert default_compression_bytes == pyarrow_snappy_bytes

        assumed_default_compression = "snappy"
        default_engine_bytes = default_data_frame.to_parquet(compression=assumed_default_compression)
        assert default_engine_bytes == pyarrow_snappy_bytes
