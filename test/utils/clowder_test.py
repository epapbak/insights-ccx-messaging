# Copyright 2023 Red Hat Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test suite for the ccx_messaging.utils.clowder module."""

from app_common_python.types import (
    BrokerConfigAuthtypeEnum,
    KafkaSASLConfig,
    TopicConfig
)
from ccx_messaging.utils.clowder import ClowderIntegration

import os
import unittest
from unittest.mock import Mock, patch

os.environ['ACG_CONFIG'] = 'test/utils/clowder_cfg.json'


class TestClowderIntegration(unittest.TestCase):

    """Test case class for the methods in ClowderIntegration."""

    def setUp(self):
        """Set up the test fixture before each test."""
        self.clowder = ClowderIntegration()
        self.clowder.clowder_app.LoadedConfig._kafka_ca = "/tmp/path/to/kafkacert"

        # Variables changed during the tests are stored for easy reset
        self.loadedBrokers = self.clowder.clowder_app.LoadedConfig.kafka.brokers
        self.loadedKafkaTopics = self.clowder.clowder_app.KafkaTopics

    def tearDown(self):
        """Clean up after each test."""
        # Reset fields changed by UTs
        self.clowder.clowder_app.LoadedConfig.kafka.brokers = self.loadedBrokers
        self.clowder.clowder_app.KafkaTopics = self.loadedKafkaTopics

    def test_get_clowder_bootstrap_servers(self):
        """Check the creation of bootstrap servers list from clowder's BrokerConfig objects."""
        result = self.clowder._get_clowder_bootstrap_servers()
        self.assertEqual(result, ["broker-host:27015", "broker-host2:27016"])

    def test_get_clowder_broker_config_no_sasl(self):
        """Check the setup of the broker from clowder's BrokerConfig objects."""
        expectedItems = {
            "bootstrap.servers": "broker-host:27015,broker-host2:27016",
            "ssl.ca.location": "/tmp/path/to/kafkacert"
        }
        broker_cfg = self.clowder._get_clowder_broker_config()

        self.assertEqual(expectedItems, broker_cfg)

    def test_get_clowder_broker_config_with_sasl_config(self):
        """Check the setup of the broker from clowder's BrokerConfig objects."""
        mock_broker = Mock(
            hostname="broker-host",
            port=27015,
            cacert="kafkaca",
            authtype=BrokerConfigAuthtypeEnum.SASL,
            sasl=KafkaSASLConfig.dictToObject({
                "saslMechanism": "PLAIN",
                "username": "user",
                "password": "pass",
                "securityProtocol": "SASL_SSL"
            }),
        )
        self.clowder.clowder_app.LoadedConfig.kafka.brokers = [mock_broker]

        expectedItems = {
            "bootstrap.servers": "broker-host:27015",
            "ssl.ca.location": "/tmp/path/to/kafkacert",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "user",
            "sasl.password": "pass",
            "security.protocol": "SASL_SSL",
        }
        broker_cfg = self.clowder._get_clowder_broker_config()
        self.assertEqual(expectedItems, broker_cfg)

    def test_handle_cacert_parameters(self):
        """Test the _handle_cacert_parameters helper function."""
        kafka_broker_config = {}
        self.clowder._handle_cacert_parameters(kafka_broker_config)
        self.assertEqual(kafka_broker_config, {"ssl.ca.location": "/tmp/path/to/kafkacert"})

    def test_handle_sasl_parameters(self):
        """Test the _handle_sasl_parameters helper function."""
        mock_broker = Mock(
            sasl=KafkaSASLConfig.dictToObject({
                "saslMechanism": "PLAIN",
                "username": "user",
                "password": "pass",
                "securityProtocol": "SASL_SSL"
            }),
        )
        self.clowder.broker_config = mock_broker

        kafka_broker_config = {}
        self.clowder._handle_sasl_parameters(kafka_broker_config)

        expected_config = {
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "user",
            "sasl.password": "pass",
            "security.protocol": "SASL_SSL",
        }

        self.assertEqual(kafka_broker_config, expected_config)

    def test_find_payload_tracker_watcher(self):
        """Test the _find_payload_tracker_watcher helper function."""
        config = {
            "service": {"watchers": [{"name": "watcher1"}, {"name": "watcher2"}]}
        }
        result = self.clowder._find_payload_tracker_watcher(config, "watcher2")
        self.assertEqual(result, {"name": "watcher2"})

    def test_find_payload_tracker_watcher_not_found(self):
        """Test the _find_payload_tracker_watcher helper function."""
        config = {"service": {"watchers": [{"name": "watcher1"}]}}
        result = self.clowder._find_payload_tracker_watcher(config, "watcher2")
        self.assertIsNone(result)

    def test_update_topics(self):
        """Test the _update_topics helper function."""
        topicConfig = TopicConfig.dictToObject({"name": "Topic1"})
        self.clowder.clowder_app.KafkaTopics = {"topic1": topicConfig}

        config = {
            "service": {
                "consumer": {"kwargs": {"incoming_topic": "topic1"}},
                "publisher": {"kwargs": {"outgoing_topic": "topic1"}},
            }
        }

        self.clowder._update_topics(config)

        expected_config = {
            "service": {
                "consumer": {"kwargs": {"incoming_topic": "Topic1"}},
                "publisher": {"kwargs": {"outgoing_topic": "Topic1"}},
            }
        }

        self.assertEqual(config, expected_config)

    def test_update_consumer_topic_warning_not_found(self):
        """Test the _update_topics helper function when topic is not found."""
        topic = "unknown_topic"
        config = {
            "service": {
                "consumer": {"kwargs": {"incoming_topic": topic}},
            }
        }

        with patch("ccx_messaging.utils.clowder.logger.warning") as mock_warning:
            self.clowder._update_consumer_topic(config, topic)
            mock_warning.assert_called_with(
                "The consumer topic cannot be found in Clowder mapping."
                " It can cause errors")

    def test_update_publisher_topic_warning_not_found(self):
        """Test the _update_topics helper function when topic is not found."""
        topic = "unknown_topic"
        config = {
            "service": {
                "publisher": {"kwargs": {"outgoing_topic": topic}},
            }
        }

        with patch("ccx_messaging.utils.clowder.logger.warning") as mock_warning:
            self.clowder._update_producer_topic(config, topic)
            mock_warning.assert_called_with(
                "The publisher topic cannot be found in Clowder mapping."
                " It can cause errors")

    def test_update_payload_tracker_topic_warning_not_found(self):
        """Test the _update_topics helper function when topic is not found."""
        with patch("ccx_messaging.utils.clowder.logger.warning") as mock_warning:
            self.clowder._update_payload_tracker_topic("random_watcher_name", "unexisting_topic")
            mock_warning.assert_called_with(
                "The Payload Tracker watcher topic cannot be found in Clowder mapping."
                " It can cause errors")

    def test_update_dlq_topic_warning_not_found(self):
        """Test the _update_topics helper function when topic is not found."""
        config = {
            "service": {
                "consumer": {"kwargs": {"dead_letter_queue_topic": "unknown_topic"}},
            }
        }

        with patch("ccx_messaging.utils.clowder.logger.warning") as mock_warning:
            self.clowder._update_dlq_topic(config,
                                           config["service"]["consumer"]["kwargs"].get("dead_letter_queue_topic"))
            mock_warning.assert_not_called()

    def test_apply_clowder_config(self):
        """Check that config loaded by Clowder integration is properly applied."""
        # our config's YAML
        manifest = "service:\n" \
                   "  consumer:\n" \
                   "    kwargs:\n" \
                   "      incoming_topic: topic1\n" \
                   "  publisher:\n" \
                   "    kwargs:\n" \
                   "      outgoing_topic: topic1"
        # Broker config loaded by Clowder
        mock_broker = Mock(
            hostname="broker-host",
            port=27015,
            cacert="/path/to/cacert",
            authtype=BrokerConfigAuthtypeEnum.SASL,
            sasl=KafkaSASLConfig.dictToObject({
                "saslMechanism": "PLAIN",
                "username": "user",
                "password": "pass",
                "securityProtocol": "SASL_SSL"
            }),
        )
        # Topic configuration loaded by clowder
        topicConfig = TopicConfig.dictToObject({"name": "Topic1"})

        # Set the _kafka_ca attribute so the path is not randomly generated
        self.clowder.clowder_app.LoadedConfig._kafka_ca = "/tmp/path/to/kafkacert"
        self.clowder.clowder_app.KafkaTopics = {"topic1": topicConfig}
        self.clowder.clowder_app.LoadedConfig.kafka.brokers = [mock_broker]

        # Apply clowder config over the manifest
        result = self.clowder.apply_clowder_config(manifest)

        # Assert that topic1 has been replaced by Topic1 and broker config is added
        expected_result = {
            "service": {
                "consumer": {
                    "kwargs": {
                        "incoming_topic": "Topic1",
                        'kafka_broker_config': {
                            'bootstrap.servers': 'broker-host:27015',
                            'sasl.mechanisms': 'PLAIN',
                            'sasl.password': 'pass',
                            'sasl.username': 'user',
                            'security.protocol': 'SASL_SSL',
                            'ssl.ca.location': '/tmp/path/to/kafkacert'
                        }
                    }
                },
                "publisher": {
                    "kwargs": {
                        'kafka_broker_config': {
                            'bootstrap.servers': 'broker-host:27015',
                            'sasl.mechanisms': 'PLAIN',
                            'sasl.password': 'pass',
                            'sasl.username': 'user',
                            'security.protocol': 'SASL_SSL',
                            'ssl.ca.location': '/tmp/path/to/kafkacert'
                        },
                        "outgoing_topic": "Topic1",
                    }
                }
            }
        }

        self.assertEqual(result, expected_result)
