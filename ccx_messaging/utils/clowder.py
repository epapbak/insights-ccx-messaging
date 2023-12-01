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

"""Clowder integration functions."""

import app_common_python
import logging
import yaml

logger = logging.getLogger(__name__)


class ClowderIntegration:

    """Clowder integration functions and helpers."""

    def __init__(self, clowder_app=app_common_python):
        """Initialize ClowderIntegration with the given reference to the app_common_python.

        Args:
        ----
            clowder_app: the imported app_common_python package (default: app_common_python).
        """
        self.clowder_app = clowder_app
        self.broker_config = None

    def _get_clowder_bootstrap_servers(self):
        """Get a list of Clowder bootstrap servers."""
        broker_addresses = []
        for broker in self.clowder_app.LoadedConfig.kafka.brokers:
            if broker.port:
                broker_addresses.append(f"{broker.hostname}:{broker.port}")
            else:
                broker_addresses.append(broker.hostname)
        return broker_addresses

    def _get_clowder_broker_config(self):
        """Get Clowder broker config."""
        bootstrap_servers = ",".join(self._get_clowder_bootstrap_servers())
        logger.debug("Kafka bootstrap server URLs: %s", bootstrap_servers)
        kafka_broker_config = {"bootstrap.servers": bootstrap_servers}

        self.broker_config = self.clowder_app.LoadedConfig.kafka.brokers[0]

        if self.broker_config.cacert:
            self._handle_cacert_parameters(kafka_broker_config)

        if app_common_python.types.BrokerConfigAuthtypeEnum.\
                valueAsString(self.broker_config.authtype) == "sasl":
            self._handle_sasl_parameters(kafka_broker_config)

        return kafka_broker_config

    def _handle_cacert_parameters(self, kafka_broker_config):
        """Handle CA certificate parameters."""
        ssl_ca_location = self.clowder_app.LoadedConfig.kafka_ca()
        kafka_broker_config["ssl.ca.location"] = ssl_ca_location

    def _handle_sasl_parameters(self, kafka_broker_config):
        """Handle SASL parameters."""
        kafka_broker_config.update(
            {
                "sasl.mechanisms": self.broker_config.sasl.saslMechanism,
                "sasl.username": self.broker_config.sasl.username,
                "sasl.password": self.broker_config.sasl.password,
                "security.protocol": self.broker_config.sasl.securityProtocol,
            }
        )

    def _find_payload_tracker_watcher(self, config, pt_watcher_name):
        """Find the Payload Tracker watcher in the config."""
        if "watchers" in config["service"]:
            for watcher in config["service"]["watchers"]:
                if watcher["name"] == pt_watcher_name:
                    return watcher
        return None

    def _update_kafka_config(self, config, pt_watcher, kafka_broker_config):
        """Update Kafka configuration in the ICM config manifest."""
        config["service"]["consumer"]["kwargs"]["kafka_broker_config"] = kafka_broker_config
        config["service"]["publisher"]["kwargs"]["kafka_broker_config"] = kafka_broker_config
        if pt_watcher:
            pt_watcher["kwargs"]["kafka_broker_config"] = kafka_broker_config
        logger.info("Kafka brokers configuration updated from Clowder configuration")

    def _update_consumer_topic(self, config, consumer_topic):
        if consumer_topic in self.clowder_app.KafkaTopics:
            topic_cfg = self.clowder_app.KafkaTopics[consumer_topic]
            config["service"]["consumer"]["kwargs"]["incoming_topic"] = topic_cfg.name
        else:
            logger.warning("The consumer topic cannot be found in Clowder mapping."
                           " It can cause errors")

    def _update_dlq_topic(self, config, dlq_topic):
        if dlq_topic in self.clowder_app.KafkaTopics:
            topic_cfg = self.clowder_app.KafkaTopics[dlq_topic]
            config["service"]["consumer"]["kwargs"]["dead_letter_queue_topic"] = topic_cfg.name

    def _update_producer_topic(self, config, producer_topic):
        if producer_topic in self.clowder_app.KafkaTopics:
            topic_cfg = self.clowder_app.KafkaTopics[producer_topic]
            config["service"]["publisher"]["kwargs"]["outgoing_topic"] = topic_cfg.name
        else:
            logger.warning("The publisher topic cannot be found in Clowder mapping."
                           " It can cause errors")

    def _update_payload_tracker_topic(self, pt_watcher, payload_tracker_topic):
        if pt_watcher and payload_tracker_topic in self.clowder_app.KafkaTopics:
            topic_cfg = self.clowder_app.KafkaTopics[payload_tracker_topic]
            pt_watcher["kwargs"]["topic"] = topic_cfg.name
        else:
            logger.warning(
                "The Payload Tracker watcher topic cannot be found in Clowder mapping."
                " It can cause errors")

    def _update_topics(self, config):
        consumer_topic = config["service"]["consumer"]["kwargs"].get("incoming_topic")
        dlq_topic = config["service"]["consumer"]["kwargs"].get("dead_letter_queue_topic")
        producer_topic = config["service"]["publisher"]["kwargs"].get("outgoing_topic")
        pt_watcher = self._find_payload_tracker_watcher(
            config,
            "ccx_messaging.watchers.payload_tracker_watcher.PayloadTrackerWatcher"
        )
        payload_tracker_topic = pt_watcher["kwargs"].pop("topic") if pt_watcher else None

        self._update_consumer_topic(config, consumer_topic)
        self._update_dlq_topic(config, dlq_topic)
        self._update_producer_topic(config, producer_topic)
        self._update_payload_tracker_topic(pt_watcher, payload_tracker_topic)

    def apply_clowder_config(self, manifest):
        """Apply Clowder config values to ICM config manifest."""
        Loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
        config = yaml.load(manifest, Loader=Loader)

        pt_watcher_name = "ccx_messaging.watchers.payload_tracker_watcher.PayloadTrackerWatcher"
        pt_watcher = self._find_payload_tracker_watcher(config, pt_watcher_name)

        if len(self.clowder_app.LoadedConfig.kafka.brokers) > 0:
            kafka_broker_config = self._get_clowder_broker_config()
            self._update_kafka_config(config, pt_watcher, kafka_broker_config)

        self._update_topics(config)

        return config


def apply_clowder_config(manifest):
    """Apply Clowder config values to ICM config manifest."""
    clowder = ClowderIntegration(clowder_app=app_common_python)
    return clowder.apply_clowder_config(manifest)
