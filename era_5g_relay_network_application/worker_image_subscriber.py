import time
from queue import Full
from typing import Any, Optional

from cv_bridge import CvBridge  # pants: no-infer-dep
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup  # pants: no-infer-dep
from rclpy.node import Node  # pants: no-infer-dep
from rclpy.qos import QoSProfile  # pants: no-infer-dep
from rclpy.time import Time  # pants: no-infer-dep
from rosbridge_library.internal import ros_loader  # pants: no-infer-dep
from sensor_msgs.msg import Image  # pants: no-infer-dep

from era_5g_interface.measuring import Measuring
from era_5g_relay_network_application import AnyQueue


class WorkerImageSubscriber:
    """Worker object that subscribes to image topic.

    Transform the image to numpy array and put it into queue for sending to the other part of the relay.
    """

    def __init__(
        self,
        topic_name: str,
        topic_type: str,
        node: Node,
        queue: AnyQueue,
        qos: Optional[QoSProfile] = None,
        extended_measuring: bool = False,
        **kw,
    ):
        """Constructor.

        Args:
            topic_name (str): The name of the topic to subscribe to.
            topic_type (str): The type of the topic to subscribe to in the string format.
            node (Node): The ROS node for subscription.
            queue (Queue): The queue for storing the received image.
            extended_measuring (bool): Enable logging of measuring.
        """

        super().__init__(**kw)

        inst = ros_loader.get_message_instance(topic_type)
        self.node = node
        self.node.get_logger().debug(f"Create Subscription: {type(inst)} {topic_name}")

        # each topic has its own group
        # callbacks for different topics can overlap
        # there will be only one running callback for particular topic
        self._cb_group = MutuallyExclusiveCallbackGroup()

        self.sub = node.create_subscription(
            type(inst),
            topic_name,
            self.callback,
            qos if qos is not None else 10,
            callback_group=self._cb_group,
        )
        self.inst = inst
        self.queue = queue
        self.topic_name = topic_name
        self.topic_type = topic_type
        self.bridge = CvBridge()

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
        cv_image: Image = self.bridge.imgmsg_to_cv2(data, desired_encoding="bgr8")
        if cv_image is None:
            print("Empty image received!")
            return
        timestamp = Time.from_msg(data.header.stamp).nanoseconds
        try:
            self.queue.put_nowait((timestamp, cv_image))
        except Full:
            pass
        self._measuring.log_measuring(timestamp, "before_callback_timestamp", before_callback_timestamp)
        self._measuring.log_timestamp(timestamp, "after_callback_timestamp")
        self._measuring.store_measuring(timestamp)
