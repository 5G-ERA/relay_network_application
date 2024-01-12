from queue import Full
from threading import Event
from typing import List

from geometry_msgs.msg import TransformStamped  # pants: no-infer-dep
from rclpy.node import Node  # pants: no-infer-dep
from rosbridge_library.internal.message_conversion import extract_values  # pants: no-infer-dep
from tf2_msgs.msg import TFMessage  # pants: no-infer-dep

from era_5g_relay_network_application import AnyQueue
from era_5g_tf2json.tf2_web_republisher import TFRepublisher


class WorkerTF:
    """Worker object for processing transforms."""

    def __init__(self, transforms_to_listen: List, queue: AnyQueue, node: Node, **kw):
        super().__init__(**kw)
        self.stop_event = Event()
        self.node = node
        self.queue = queue
        tf_republisher = TFRepublisher(node, self.tf_callback, rate=100)
        for tr in transforms_to_listen:
            node.get_logger().info(f"Subscribing for: {tr}")
            tf_republisher.subscribe_transform(*tr)

    def tf_callback(self, transforms: List[TransformStamped]) -> None:
        if transforms:
            try:
                self.queue.put_nowait(extract_values(TFMessage(transforms=transforms)))
            except Full:
                pass
