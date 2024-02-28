import time
from queue import Full
from typing import Any, Optional

import DracoPy
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup  # pants: no-infer-dep
from rclpy.node import Node  # pants: no-infer-dep
from rclpy.qos import QoSProfile  # pants: no-infer-dep
from rosbridge_library.internal import ros_loader  # pants: no-infer-dep
from rosbridge_library.internal.message_conversion import extract_values  # pants: no-infer-dep
from sensor_msgs.msg import PointCloud2  # pants: no-infer-dep

from era_5g_interface.channels import Channels
from era_5g_interface.measuring import Measuring

try:
    from sensor_msgs_py.point_cloud2 import read_points_numpy  # pants: no-infer-dep
except ImportError:
    # Compatibility with ROS2 Foxy
    from era_5g_relay_network_application.compatibility.point_cloud_msg_conv import (
        read_points_numpy,
    )

from era_5g_relay_network_application import AnyQueue
from era_5g_relay_network_application.utils import ActionSubscribers, ActionTopicVariant, Compressions

try:
    from rosbridge_library.internal.ros_loader import get_action_class
except ImportError:
    from era_5g_relay_network_application.compatibility.rosbridge_action_loader import get_action_class


class WorkerSubscriber:
    """Worker object that subscribes to a topic.

    Messages are placed into a queue for sending to the other part of the relay.
    """

    def __init__(
        self,
        topic_name: str,
        topic_type: str,
        node: Node,
        queue: AnyQueue,
        compression: Optional[Compressions] = None,
        qos: Optional[QoSProfile] = None,
        action_topic_variant: ActionTopicVariant = ActionTopicVariant.NONE,
        action_subscribers: Optional[ActionSubscribers] = None,
        extended_measuring: bool = True,
        **kw,
    ):
        """Constructor.

        Args:
            topic_name (str): The name of the topic for which the subscriber is created.
            topic_type (str): The type of the topic as a string.
            queue (Queue): The queue to put the messages to.
            node (Node): The ROS node for subscription.
            action_topic_variant (ActionTopicVariant, Optional):
                Specification of variant in case action-related topic.
                Default is ActionTopicVariant.NONE (used for regular topics).
            action_subscribers (ActionSubscribers, Optional): Used only for action-related topics.
                The action_subscribers structure holds information about which client requested which goal.
                Default is None (for regular topics).
            extended_measuring (bool): Enable logging of measuring.
        """

        super().__init__(**kw)

        self.topic_name = topic_name
        self.topic_type = topic_type
        self.compression = compression
        self.node = node
        self.queue = queue
        self.action_topic_variant = action_topic_variant
        self.action_subscribers = action_subscribers

        # each topic has its own group
        # callbacks for different topics can overlap
        # there will be only one running callback for particular topic
        self._cb_group = MutuallyExclusiveCallbackGroup()

        if action_topic_variant == ActionTopicVariant.NONE:
            self.topic_type_class = ros_loader.get_message_class(topic_type)
        else:
            # Get Action type (instead of regular topic type)
            action_type_class = get_action_class(topic_type)
            action_name = topic_name

        if action_topic_variant == ActionTopicVariant.ACTION_FEEDBACK:
            self.topic_name = f"{action_name}/_action/feedback"
            self.topic_type_class = action_type_class.Impl.FeedbackMessage

        elif action_topic_variant == ActionTopicVariant.ACTION_STATUS:
            self.topic_name = f"{action_name}/_action/status"
            self.topic_type_class = action_type_class.Impl.GoalStatusMessage

        self.node.get_logger().debug(f"Create Subscription: {self.topic_type_class} {self.topic_name}")
        self.sub = node.create_subscription(
            self.topic_type_class,
            self.topic_name,
            self.callback,
            qos if qos is not None else 10,
            callback_group=self._cb_group,
        )

        self._extended_measuring = extended_measuring
        self._measuring = Measuring(
            measuring_items={
                "key_timestamp": 0,
                "before_callback_timestamp": 0,
                "after_callback_timestamp": 0,
            },
            enabled=self._extended_measuring,
            filename_prefix="subscription-" + self.topic_name.replace("/", ""),
        )

    def callback(self, data: Any):
        before_callback_timestamp = time.perf_counter_ns()

        msg = extract_values(data)
        timestamp = Channels.get_timestamp_from_data(msg)

        if isinstance(data, PointCloud2) and self.compression == Compressions.DRACO:
            np_arr = read_points_numpy(data, field_names=["x", "y", "z"], skip_nans=True)  # drop intensity, etc....
            cpc = DracoPy.encode(np_arr, compression_level=1)
            msg = cpc

        try:
            if self.action_topic_variant == ActionTopicVariant.ACTION_FEEDBACK:
                # Send action feedback only to the client that sent the corresponding action goal
                goal_uuid = msg["goal_id"]["uuid"]
                assert self.action_subscribers is not None  # prevents mypy error
                sid = self.action_subscribers.get_sid_for_goal_id(goal_uuid)

                self.queue.put_nowait((sid, msg))
            else:
                self.queue.put_nowait(msg)

        except Full:
            pass
        self._measuring.log_measuring(timestamp, "before_callback_timestamp", before_callback_timestamp)
        self._measuring.log_timestamp(timestamp, "after_callback_timestamp")
        self._measuring.store_measuring(timestamp)
