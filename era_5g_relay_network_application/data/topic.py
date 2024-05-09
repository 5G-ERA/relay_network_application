import os
from multiprocessing import Queue
from typing import Any, Optional, Union

import rclpy  # pants: no-infer-dep
from rclpy.qos import QoSProfile  # pants: no-infer-dep

from era_5g_interface.channels import ChannelType
from era_5g_relay_network_application import queue_len_from_qos
from era_5g_relay_network_application.utils import (
    IMAGE_CHANNEL_TYPES,
    ActionSubscribers,
    ActionTopicVariant,
    Compressions,
)
from era_5g_relay_network_application.worker_image_publisher import WorkerImagePublisher
from era_5g_relay_network_application.worker_image_subscriber import WorkerImageSubscriber
from era_5g_relay_network_application.worker_publisher import WorkerPublisher
from era_5g_relay_network_application.worker_subscriber import WorkerSubscriber

QUEUE_LENGTH_TOPICS = int(os.getenv("QUEUE_LENGTH_TOPICS", 1))
EXTENDED_MEASURING = bool(os.getenv("EXTENDED_MEASURING", False))


class RelayTopic:
    """Base class that holds information about topic and its type."""

    def __init__(
        self,
        topic_name: str,
        topic_type: str,
        channel_type: ChannelType,
        compression: Optional[Compressions] = None,
        qos: Optional[QoSProfile] = None,
    ):
        self.topic_name = topic_name
        self.topic_type = topic_type
        self.compression = compression
        self.qos = qos
        self.channel_type = channel_type
        self.channel_name = f"topic/{self.topic_name}"
        self.queue: Queue[Any] = Queue(queue_len_from_qos(QUEUE_LENGTH_TOPICS, self.qos))


class RelayTopicIncoming(RelayTopic):
    """Class that holds information about incoming topic (i.e. topic that is received from the relay client and is
    published here), its type and related publisher."""

    def __init__(
        self,
        topic_name: str,
        topic_type: str,
        channel_type: ChannelType,
        node,
        compression: Optional[Compressions] = None,
        qos: Optional[QoSProfile] = None,
    ):
        super().__init__(topic_name, topic_type, channel_type, compression, qos)

        # this sucks, the classes should have some common ancestor or there should be two properties
        self.worker: Union[WorkerImagePublisher, WorkerPublisher]

        if self.channel_type in IMAGE_CHANNEL_TYPES:
            self.worker = WorkerImagePublisher(
                self.queue,
                self.topic_name,
                self.topic_type,
                compression=compression,
                node=node,
                extended_measuring=EXTENDED_MEASURING,
            )
        else:
            self.worker = WorkerPublisher(
                self.queue,
                self.topic_name,
                self.topic_type,
                node,
                self.compression,
                self.qos,
                extended_measuring=EXTENDED_MEASURING,
            )
        self.worker.daemon = True
        self.worker.start()


class RelayTopicOutgoing(RelayTopic):
    """Class that holds information about outgoing topic (i.e. topic that is subscribed to be sent to the relay client),
    its type and related subscriber."""

    def __init__(
        self,
        topic_name: str,
        topic_type: str,
        channel_type: ChannelType,
        node: rclpy.node.Node,
        compression: Optional[Compressions] = None,
        qos: Optional[QoSProfile] = None,
        action_topic_variant: ActionTopicVariant = ActionTopicVariant.NONE,
        action_subscribers: Optional[ActionSubscribers] = None,
    ):
        super().__init__(topic_name, topic_type, channel_type, compression, qos)

        self.action_topic_variant = action_topic_variant

        # this sucks, the classes should have some common ancestor or there should be two properties
        self.worker: Union[WorkerImageSubscriber, WorkerSubscriber]

        if self.channel_type in IMAGE_CHANNEL_TYPES:
            self.worker = WorkerImageSubscriber(
                topic_name, topic_type, node, self.queue, extended_measuring=EXTENDED_MEASURING
            )
        else:
            self.worker = WorkerSubscriber(
                topic_name,
                topic_type,
                node,
                self.queue,
                compression,
                qos,
                action_topic_variant,
                action_subscribers,
                extended_measuring=EXTENDED_MEASURING,
            )
            # Topic name may be changed in case of action-related topics
            self.channel_name = f"topic/{self.worker.topic_name}"
            self.topic_name = self.worker.topic_name
