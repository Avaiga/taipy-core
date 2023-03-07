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

import os
import pathlib
from datetime import datetime
from importlib import util
from time import sleep

import modin.pandas as modin_pd
import numpy as np
import pandas as pd
import pytest

from src.taipy.core.common.alias import DataNodeId
from src.taipy.core.data._data_manager import _DataManager
from src.taipy.core.data.parquet import ParquetDataNode
from src.taipy.core.exceptions.exceptions import (
    InvalidExposedType,
    MissingRequiredProperty,
    NoData,
    UnknownCompressionAlgorithm,
    UnknownParquetEngine,
)
from taipy.config.common.scope import Scope
from taipy.config.config import Config
from taipy.config.exceptions.exceptions import InvalidConfigurationId


@pytest.fixture(scope="function", autouse=True)
def cleanup():
    yield
    path = os.path.join(pathlib.Path(__file__).parent.resolve(), "data_sample/temp.parquet")
    if os.path.isfile(path):
        os.remove(path)


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
    __engine = ["pyarrow"]
    if util.find_spec("fastparquet"):
        __engine.append("fastparquet")

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
        assert dn.engine == "pyarrow"

        with pytest.raises(InvalidConfigurationId):
            dn = ParquetDataNode("foo bar", Scope.PIPELINE, name="super name", properties={"path": path})

    def test_new_parquet_data_node_with_existing_file_is_ready_for_reading(self, parquet_file_path):
        not_ready_dn_cfg = Config.configure_data_node(
            "not_ready_data_node_config_id", "parquet", path="NOT_EXISTING.parquet"
        )
        not_ready_dn = _DataManager._bulk_get_or_create([not_ready_dn_cfg])[not_ready_dn_cfg]
        assert not not_ready_dn.is_ready_for_reading

        ready_dn_cfg = Config.configure_data_node("ready_data_node_config_id", "parquet", path=parquet_file_path)
        ready_dn = _DataManager._bulk_get_or_create([ready_dn_cfg])[ready_dn_cfg]
        assert ready_dn.is_ready_for_reading

    @pytest.mark.parametrize("engine", __engine)
    def test_read(self, engine, parquet_file_path):
        not_existing_parquet = ParquetDataNode(
            "foo", Scope.PIPELINE, properties={"path": "nonexistent.parquet", "engine": engine}
        )
        with pytest.raises(NoData):
            assert not_existing_parquet.read() is None
            not_existing_parquet.read_or_raise()

        df = pd.read_parquet(parquet_file_path)
        # Create ParquetDataNode without exposed_type (Default is pandas.DataFrame)
        parquet_data_node_as_pandas = ParquetDataNode(
            "bar", Scope.PIPELINE, properties={"path": parquet_file_path, "engine": engine}
        )
        data_pandas = parquet_data_node_as_pandas.read()
        assert isinstance(data_pandas, pd.DataFrame)
        assert len(data_pandas) == 2
        assert data_pandas.equals(df)
        assert np.array_equal(data_pandas.to_numpy(), df.to_numpy())

        # !!! Modin still check for pyarrow eventhough it is not necessary when using `fastparquet`
        if engine == "pyarrow":
            # Create ParquetDataNode with modin exposed_type
            parquet_data_node_as_modin = ParquetDataNode(
                "bar", Scope.PIPELINE, properties={"path": parquet_file_path, "exposed_type": "modin", "engine": engine}
            )
            data_modin = parquet_data_node_as_modin.read()
            assert isinstance(data_modin, modin_pd.DataFrame)
            assert len(data_modin) == 2
            assert data_modin.equals(df)
            assert np.array_equal(data_modin.to_numpy(), df.to_numpy())

        # Create ParquetDataNode with numpy exposed_type
        parquet_data_node_as_numpy = ParquetDataNode(
            "bar", Scope.PIPELINE, properties={"path": parquet_file_path, "exposed_type": "numpy", "engine": engine}
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

    @pytest.mark.parametrize("engine", __engine)
    def test_read_write_after_modify_path(self, engine):
        path = os.path.join(pathlib.Path(__file__).parent.resolve(), "data_sample/example.parquet")
        new_path = os.path.join(pathlib.Path(__file__).parent.resolve(), "data_sample/temp.parquet")
        dn = ParquetDataNode("foo", Scope.PIPELINE, properties={"path": path, "engine": engine})
        read_data = dn.read()
        assert read_data is not None
        dn.path = new_path
        with pytest.raises(FileNotFoundError):
            dn.read()
        dn.write(read_data)
        assert dn.read().equals(read_data)

    def test_read_custom_exposed_type(self):
        example_parquet_path = os.path.join(pathlib.Path(__file__).parent.resolve(), "data_sample/example.parquet")

        dn = ParquetDataNode(
            "foo", Scope.PIPELINE, properties={"path": example_parquet_path, "exposed_type": MyCustomObject}
        )
        assert all([isinstance(obj, MyCustomObject) for obj in dn.read()])

        dn = ParquetDataNode(
            "foo", Scope.PIPELINE, properties={"path": example_parquet_path, "exposed_type": create_custom_class}
        )
        assert all([isinstance(obj, MyOtherCustomObject) for obj in dn.read()])

    def test_raise_error_unknown_parquet_engine(self):
        path = os.path.join(pathlib.Path(__file__).parent.resolve(), "data_sample/example.parquet")
        with pytest.raises(UnknownParquetEngine):
            ParquetDataNode("foo", Scope.PIPELINE, properties={"path": path, "engine": "foo"})

    def test_raise_error_unknown_compression_algorithm(self):
        path = os.path.join(pathlib.Path(__file__).parent.resolve(), "data_sample/example.parquet")
        with pytest.raises(UnknownCompressionAlgorithm):
            ParquetDataNode("foo", Scope.PIPELINE, properties={"path": path, "compression": "foo"})

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

    @pytest.mark.parametrize(
        "data",
        [
            [{"a": 11, "b": 22, "c": 33}, {"a": 44, "b": 55, "c": 66}],
            pd.DataFrame([{"a": 11, "b": 22, "c": 33}, {"a": 44, "b": 55, "c": 66}]),
            modin_pd.DataFrame([{"a": 11, "b": 22, "c": 33}, {"a": 44, "b": 55, "c": 66}]),
        ],
    )
    def test_write_to_disk(self, tmpdir_factory, data):
        temp_file_path = str(tmpdir_factory.mktemp("data").join("temp.parquet"))
        dn = ParquetDataNode("foo", Scope.PIPELINE, properties={"path": temp_file_path})
        dn.write(data)

        assert pathlib.Path(temp_file_path).exists()
        assert isinstance(dn.read(), pd.DataFrame)

    @pytest.mark.parametrize("engine", __engine)
    def test_pandas_parquet_config_kwargs(self, engine, tmpdir_factory):
        read_kwargs = {"filters": [("integer", "<", 10)], "columns": ["integer"]}
        temp_file_path = str(tmpdir_factory.mktemp("data").join("temp.parquet"))
        dn = ParquetDataNode(
            "foo", Scope.PIPELINE, properties={"path": temp_file_path, "engine": engine, "read_kwargs": read_kwargs}
        )

        df = pd.read_csv(os.path.join(pathlib.Path(__file__).parent.resolve(), "data_sample/example.csv"))
        dn.write(df)

        assert set(pd.read_parquet(temp_file_path).columns) == {"id", "integer", "text"}
        print(dn.read())
        assert set(dn.read().columns) == set(read_kwargs["columns"])

        # !!! filter doesn't work with `fastparquet` without partition_cols
        if engine == "pyarrow":
            assert len(dn.read()) != len(df)
            assert len(dn.read()) == 2

    @pytest.mark.parametrize("engine", __engine)
    def test_kwarg_precedence(self, engine, tmpdir_factory, default_data_frame):
        # Precedence:
        # 1. Class read/write methods
        # 2. Defined in read_kwargs and write_kwargs, in properties
        # 3. Defined top-level in properties

        temp_file_path = str(tmpdir_factory.mktemp("data").join("temp.parquet"))
        temp_file_2_path = str(tmpdir_factory.mktemp("data").join("temp_2.parquet"))
        df = default_data_frame.copy(deep=True)

        # Write
        # 3
        comp3 = "snappy"
        dn = ParquetDataNode(
            "foo", Scope.PIPELINE, properties={"path": temp_file_path, "engine": engine, "compression": comp3}
        )
        dn.write(df)
        df.to_parquet(path=temp_file_2_path, compression=comp3, engine=engine)
        with open(temp_file_2_path, "rb") as tf:
            with pathlib.Path(temp_file_path).open("rb") as f:
                assert f.read() == tf.read()

        # 3 and 2
        comp2 = "gzip"
        dn = ParquetDataNode(
            "foo",
            Scope.PIPELINE,
            properties={
                "path": temp_file_path,
                "engine": engine,
                "compression": comp3,
                "write_kwargs": {"compression": comp2},
            },
        )
        dn.write(df)
        df.to_parquet(path=temp_file_2_path, compression=comp2, engine=engine)
        with open(temp_file_2_path, "rb") as tf:
            with pathlib.Path(temp_file_path).open("rb") as f:
                assert f.read() == tf.read()

        # 3, 2 and 1
        comp1 = "brotli"
        dn = ParquetDataNode(
            "foo",
            Scope.PIPELINE,
            properties={
                "path": temp_file_path,
                "engine": engine,
                "compression": comp3,
                "write_kwargs": {"compression": comp2},
            },
        )
        dn.write_with_kwargs(df, compression=comp1)
        df.to_parquet(path=temp_file_2_path, compression=comp1, engine=engine)
        with open(temp_file_2_path, "rb") as tf:
            with pathlib.Path(temp_file_path).open("rb") as f:
                assert f.read() == tf.read()

        # Read
        df.to_parquet(temp_file_path, engine=engine)
        # 2
        cols2 = ["a", "b"]
        dn = ParquetDataNode(
            "foo",
            Scope.PIPELINE,
            properties={"path": temp_file_path, "engine": engine, "read_kwargs": {"columns": cols2}},
        )
        assert set(dn.read().columns) == set(cols2)

        # 1
        cols1 = ["a"]
        dn = ParquetDataNode(
            "foo",
            Scope.PIPELINE,
            properties={"path": temp_file_path, "engine": engine, "read_kwargs": {"columns": cols2}},
        )
        assert set(dn.read_with_kwargs(columns=cols1).columns) == set(cols1)

    def test_partition_cols(self, tmpdir_factory, default_data_frame: pd.DataFrame):
        temp_dir_path = str(tmpdir_factory.mktemp("data").join("temp_dir"))

        write_kwargs = {"partition_cols": ["a", "b"]}
        dn = ParquetDataNode("foo", Scope.PIPELINE, properties={"path": temp_dir_path, "write_kwargs": write_kwargs})
        dn.write(default_data_frame)

        assert pathlib.Path(temp_dir_path).is_dir()
        # dtypes change during round-trip with partition_cols
        pd.testing.assert_frame_equal(
            dn.read().sort_index(axis=1),
            default_data_frame.sort_index(axis=1),
            check_dtype=False,
            check_categorical=False,
        )

    def test_read_with_kwargs_never_written(self):
        path = "data/node/path"
        dn = ParquetDataNode("foo", Scope.PIPELINE, properties={"path": path})
        assert dn.read_with_kwargs() is None
