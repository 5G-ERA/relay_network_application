from queue import Empty, Queue

import cv2
import numpy as np
from cv_bridge import CvBridge
from rclpy.node import Node
from rclpy.time import Time
from sensor_msgs.msg import Image

from era_5g_relay_network_application.worker_publisher import WorkerPublisher


class WorkerImagePublisher(WorkerPublisher):
    """ Worker object for publishing images. 


    """
    
    def __init__(self, queue: Queue, topic_name, topic_type, compression, node: Node, **kw):
        super().__init__(queue, topic_name, topic_type, compression, node, **kw)
        self.bridge = CvBridge()

    def get_data(self):
        try:
            img, ts = self.queue.get(block=True, timeout=1)
            msg: Image = self.bridge.cv2_to_imgmsg(img, encoding="bgr8")
            msg.header.stamp = Time(nanoseconds=ts).to_msg()  # .secs = int(ts / 10**9)
            return msg
        except Empty:
            return None
