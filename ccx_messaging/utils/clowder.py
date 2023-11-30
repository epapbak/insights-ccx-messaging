# Copyright 2022 Red Hat Inc.
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
import logging

import yaml
import app_common_python
from app_common_python.types import BrokerConfigAuthtypeEnum


logger = logging.getLogger(__name__)


def get_clowder_bootstrap_servers(clowder_broker_list):
    """Get list of bootstrap servers from loaded Clowder broker config."""
    broker_addresses = []
    for broker in clowder_broker_list:
        if broker.Port:
            broker_addresses.append(f"{broker.Hostname}:{broker.Port}")
        else:
            broker_addresses.append(broker.Hostname)
    return broker_addresses


def get_clowder_broker_config(clowder_broker_list):
    """Get application's broker config from loaded Clowder broker config."""
    bootstrap_servers = "".join(get_clowder_bootstrap_servers(clowder_broker_list), ",")
    logger.debug("Kafka bootstrap server URLs: %s", bootstrap_servers)
    kafka_broker_config = {"bootstrap.servers": bootstrap_servers}

    clowder_broker_config = app_common_python.LoadedConfig.kafka.brokers[0]
    if clowder_broker_config.cacert:
        # Current Kafka library is not able to handle the CA file, only a path to it
        # FIXME: Duplicating parameters in order to be used by both Kafka libraries
        ssl_ca_location = app_common_python.LoadedConfig.kafka_ca()
        kafka_broker_config["ssl.ca.location"] = ssl_ca_location

    if BrokerConfigAuthtypeEnum.valueAsString(clowder_broker_config.authtype) == "sasl":
        kafka_broker_config.update(
            {
                "sasl.mechanisms": clowder_broker_config.sasl.saslMechanism,
                "sasl.username": clowder_broker_config.sasl.username,
                "sasl.password": clowder_broker_config.sasl.password,
                "security.protocol": clowder_broker_config.sasl.securityProtocol,
            }
        )

    return kafka_broker_config


def apply_clowder_config(manifest):
    """Apply Clowder config values to ICM config manifest."""
    Loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
    config = yaml.load(manifest, Loader=Loader)

    # Find the Payload Tracker watcher, as it might be affected by config changes
    pt_watcher_name = "ccx_messaging.watchers.payload_tracker_watcher.PayloadTrackerWatcher"
    pt_watcher = None
    for watcher in config["service"]["watchers"]:
        if watcher["name"] == pt_watcher_name:
            pt_watcher = watcher
            break

    brokers = app_common_python.LoadedConfig.kafka.brokers
    if len(brokers) > 0:
        kafka_broker_config = get_clowder_broker_config(brokers)
        config["service"]["consumer"]["kwargs"]["kafka_broker_config"] = kafka_broker_config
        config["service"]["publisher"]["kwargs"]["kafka_broker_config"] = kafka_broker_config
        if pt_watcher:
            pt_watcher["kwargs"]["kafka_broker_config"] = kafka_broker_config
        logger.info("Kafka brokers configuration updated from Clowder configuration")

    consumer_topic = config["service"]["consumer"]["kwargs"].get("incoming_topic")
    dlq_topic = config["service"]["consumer"]["kwargs"].get("dead_letter_queue_topic")
    producer_topic = config["service"]["publisher"]["kwargs"].get("outgoing_topic")
    payload_tracker_topic = pt_watcher["kwargs"].pop("topic") if pt_watcher else None

    if consumer_topic in app_common_python.KafkaTopics:
        topic_cfg = app_common_python.KafkaTopics[consumer_topic]
        config["service"]["consumer"]["kwargs"]["incoming_topic"] = topic_cfg.name
    else:
        logger.warn("The consumer topic cannot be found in Clowder mapping. It can cause errors")

    if dlq_topic in app_common_python.KafkaTopics:
        topic_cfg = app_common_python.KafkaTopics[dlq_topic]
        config["service"]["consumer"]["kwargs"]["dead_letter_queue_topic"] = topic_cfg.name

    if producer_topic in app_common_python.KafkaTopics:
        topic_cfg = app_common_python.KafkaTopics[producer_topic]
        config["service"]["publisher"]["kwargs"]["outgoing_topic"] = topic_cfg.name
    else:
        logger.warn("The publisher topic cannot be found in Clowder mapping. It can cause errors")

    if pt_watcher and payload_tracker_topic in app_common_python.KafkaTopics:
        topic_cfg = app_common_python.KafkaTopics[payload_tracker_topic]
        pt_watcher["kwargs"]["topic"] = topic_cfg.name
    else:
        logger.warn(
            "The Payload Tracker watcher topic cannot be found in Clowder mapping. "
            "It can cause errors",
        )

    return config
