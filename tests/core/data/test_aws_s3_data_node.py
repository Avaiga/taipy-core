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
from dataclasses import dataclass
from unittest.mock import patch

import boto3
from moto import mock_s3
import pytest

from src.taipy.core.data.data_node_id import DataNodeId
from src.taipy.core.data.aws_s3 import S3DataNode
from src.taipy.core.exceptions.exceptions import InvalidCustomDocument, MissingRequiredProperty
from taipy.config.common.scope import Scope



class TestS3DataNode:
    __properties = [
        {
            "aws_access_key": "testing",
            "aws_secret_acces_key": "testing",
            "aws_region": "us-east-1",
            "aws_s3_bucket_name":"taipy",
            "db_extra_args": {},
        }
    ]

    @mock_s3
    @pytest.mark.parametrize("properties", __properties)
    def test_create(self, properties):
        aws_s3_dn = S3DataNode(
            "foo_bar_aws_s3",
            Scope.SCENARIO,
            properties=properties,
        )
        assert isinstance(aws_s3_dn, S3DataNode)
        assert aws_s3_dn.storage_type() == "aws_s3"
        assert aws_s3_dn.config_id == "foo_bar_aws_s3"
        assert aws_s3_dn.scope == Scope.SCENARIO
        assert aws_s3_dn.id is not None
        assert aws_s3_dn.owner_id is None
        assert aws_s3_dn.job_ids == []
        assert aws_s3_dn.is_ready_for_reading


    @mock_s3
    @pytest.mark.parametrize(' object_key, data', [
        ('taipy-object', 'Hello, world!'),
    ])
    @pytest.mark.parametrize("properties", __properties)
    def test_write(self, properties , object_key, data ):
        bucket_name = properties["aws_s3_bucket_name"]

        # Create an S3 client
        s3_client = boto3.client('s3')

        # Create a bucket
        s3_client.create_bucket(Bucket=bucket_name)

        aws_s3_dn = S3DataNode("foo_aws_s3", Scope.SCENARIO, properties=properties)
        aws_s3_dn._write( object_key ,  data)

        response = aws_s3_dn._read( object_key)

        assert response == data


    @mock_s3
    @pytest.mark.parametrize(' object_key, data', [
        ( 'taipy-other-object', 'Hello, other world!'),
    ])
    @pytest.mark.parametrize("properties", __properties)
    def test_read(self, properties , object_key, data ):

        bucket_name = properties["aws_s3_bucket_name"]
        # Create an S3 client
        s3_client = boto3.client('s3')

        # Create a bucket
        s3_client.create_bucket(Bucket=bucket_name)

        aws_s3_dn = S3DataNode("foo_aws_s3", Scope.SCENARIO, properties=properties)
        aws_s3_dn._write( object_key ,  data)

        response = aws_s3_dn._read( object_key)

        assert response == data



