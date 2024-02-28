from queue import Empty
from typing import Optional, Tuple

from cv_bridge import CvBridge  # pants: no-infer-dep
from rclpy.node import Node  # pants: no-infer-dep
from rclpy.qos import QoSProfile  # pants: no-infer-dep
from rclpy.time import Time  # pants: no-infer-dep
from sensor_msgs.msg import Image  # pants: no-infer-dep

from era_5g_relay_network_application import AnyQueue
from era_5g_relay_network_application.utils import Compressions
from era_5g_relay_network_application.worker_publisher import WorkerPublisher


class WorkerImagePublisher(WorkerPublisher):
    """Worker object for publishing images."""

    def __init__(
        self,
        queue: AnyQueue,
        topic_name,
        topic_type,
        node: Node,
        compression: Optional[Compressions] = None,
        qos: Optional[QoSProfile] = None,
        **kw,
    ) -> None:
        super().__init__(queue, topic_name, topic_type, node, compression, qos, **kw)
        self.bridge = CvBridge()

    def get_data(self) -> Optional[Tuple[Image, int]]:
        try:
            cv_image, timestamp = self.queue.get(block=True, timeout=1)
            msg: Image = self.bridge.cv2_to_imgmsg(cv_image, encoding="bgr8")
            msg.header.stamp = Time(nanoseconds=timestamp).to_msg()  # .secs = int(ts / 10**9)
            return msg, timestamp
        except Empty:
            return None
