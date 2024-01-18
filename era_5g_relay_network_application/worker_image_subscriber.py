from queue import Full
from typing import Any

from cv_bridge import CvBridge  # pants: no-infer-dep
from rclpy.node import Node  # pants: no-infer-dep
from rclpy.time import Time  # pants: no-infer-dep
from rosbridge_library.internal import ros_loader  # pants: no-infer-dep
from sensor_msgs.msg import Image  # pants: no-infer-dep

from era_5g_relay_network_application import AnyQueue


class WorkerImageSubscriber:
    """Worker object that subscribes to image topic.

    Transform the image to numpy array and put it into queue for sending to the other part of the relay.
    """

    def __init__(self, topic_name: str, topic_type: str, node: Node, queue: AnyQueue, **kw):
        """Constructor.

        Args:
            topic_name (str): The name of the topic to subscribe to
            topic_type (str): The type of the topic to subscribe to in the string format
            node (Node): The ROS node for subscription
            queue (Queue): The queue for storing the received image
        """

        super().__init__(**kw)

        inst = ros_loader.get_message_instance(topic_type)
        self.node = node
        self.node.get_logger().debug(f"Create Subscription: {type(inst)} {topic_name}")
        self.sub = node.create_subscription(type(inst), topic_name, self.callback, 10)
        self.inst = inst
        self.queue = queue
        self.topic_name = topic_name
        self.topic_type = topic_type
        self.bridge = CvBridge()

    def callback(self, data: Any):
        cv_image: Image = self.bridge.imgmsg_to_cv2(data, desired_encoding="bgr8")
        if cv_image is None:
            print("Empty image received!")
            return
        try:
            self.queue.put_nowait((Time.from_msg(data.header.stamp).nanoseconds, cv_image))
        except Full:
            pass
