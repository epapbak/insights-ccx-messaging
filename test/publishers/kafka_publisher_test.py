# Copyright 2023 Red Hat, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may naot use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for the KafkaPublisher class."""
import json
import gzip
from unittest.mock import MagicMock

import pytest
from ccx_messaging.publishers.kafka_publisher import KafkaPublisher

VALID_INPUT_MSG = [
    pytest.param(
        {
            "OrgID": 10,
            "AccountNumber": 1,
            "ClusterName": "uuid",
            "Report": {},
            "LastChecked": "a timestamp",
            "Version": 2,
            "RequestId": "a request id",
            "Metadata": {
                "gathering_time": "2012-01-14T00:00:00Z"
            }
        }
    ),
    pytest.param(
        {
            "OrgID": 10,
            "AccountNumber": "",
            "ClusterName": "uuid",
            "Report": {},
            "LastChecked": "a timestamp",
            "Version": 2,
            "RequestId": "a request id",
            "Metadata": {
                "gathering_time": "2012-01-14T00:00:00Z"
            }
        }
    ),
    pytest.param(
        {
            "OrgID": 10,
            "AccountNumber": "",
            "ClusterName": "uuid",
            "Report": {},
            "LastChecked": "a timestamp",
            "Version": 2,
            "RequestId": "a request id",
            "Metadata": {
                "gathering_time": "2012-01-14T00:00:00Z"
            }
        }
    ),
    pytest.param(
        {
            "OrgID": 10,
            "AccountNumber": "",
            "ClusterName": "uuid",
            "Report": {},
            "LastChecked": "a timestamp",
            "Version": 2,
            "RequestId": "a request id",
            "Metadata": {
                "gathering_time": "2012-01-14T00:00:00Z"
            }
        }
    ),
    pytest.param(
        {
            "OrgID": 10,
            "AccountNumber": "",
            "ClusterName": "uuid",
            "Report": {},
            "LastChecked": "a timestamp",
            "Version": 2,
            "RequestId": "a request id",
            "Metadata": {
                "gathering_time": "2023-08-14T09:31:46Z"
            }
        }
    ),
    pytest.param(
        {
            "OrgID": 10,
            "AccountNumber": "",
            "ClusterName": "uuid",
            "Report": {},
            "LastChecked": "a timestamp",
            "Version": 2,
            "RequestId": "a request id",
            "Metadata": {
                "gathering_time": "2023-08-14T09:31:46Z"
            }
        }
    ),
]

def test_init():
    """Check that init creates a valid object."""
    kakfa_config = {
        "bootstrap.servers": "kafka:9092",
    }
    KafkaPublisher(outgoing_topic="topic name", **kakfa_config)

def test_init_compression():
    """Check that init creates a valid object."""
    kakfa_config = {
        "bootstrap.servers": "kafka:9092",
        "compression" : "gzip"
    }
    KafkaPublisher(outgoing_topic="topic name", **kakfa_config)

@pytest.mark.parametrize("input", VALID_INPUT_MSG)
def test_compressing_enabled(input):
    input = bytes(json.dumps(input) + "\n",'utf-8')
    expected_output = gzip.compress(input)
    kakfa_config = {
        "bootstrap.servers": "kafka:9092",
        "compression" : "gzip"
    }
    pub = KafkaPublisher(outgoing_topic="topic-name", **kakfa_config)
    pub.producer = MagicMock()
    pub.produce(input)
    pub.producer.produce.assert_called_with("topic-name",expected_output)

@pytest.mark.parametrize("input", VALID_INPUT_MSG)
def test_compressing_disabled(input):
    input = bytes(json.dumps(input) + "\n",'utf-8')
    expected_output = input
    kakfa_config = {
        "bootstrap.servers": "kafka:9092"
    }
    pub = KafkaPublisher(outgoing_topic="topic-name", **kakfa_config)
    pub.producer = MagicMock()
    pub.produce(input)
    pub.producer.produce.assert_called_with("topic-name",expected_output)