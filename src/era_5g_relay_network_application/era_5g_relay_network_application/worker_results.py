import time
from typing import Any

from rclpy.node import Node
from rosbridge_library.internal import ros_loader
from rosbridge_library.internal.message_conversion import extract_values
from socketio import Server

from era_5g_relay_network_application.data.packets import MessagePacket, PacketType

from sensor_msgs.msg import PointCloud2
from sensor_msgs_py.point_cloud2 import read_points
import DracoPy
import numpy as np


class WorkerResults:
    """
    Worker object for data processing in standalone variant. Reads
    data from passed queue, performs detection and returns results using
    the flask app.
    """

    def __init__(self, topic_name: str, topic_type: str, node: Node, results_queue, **kw):
        """
        Constructor

        Args:
            data_queue (Queue): The queue with all to-be-processed data
            app (_type_): The flask app for results publishing
        """

        super().__init__(**kw)

        inst = ros_loader.get_message_instance(topic_type)
        self.node = node
        self.node.get_logger().debug(f"Create Subscription: {type(inst)} {topic_name}")
        self.sub = node.create_subscription(type(inst), topic_name, self.callback, 10)
        self.inst = inst
        self.results_queue = results_queue

        self.topic_name = topic_name
        self.topic_type = topic_type

    def callback(self, data: Any):
        msg = extract_values(data)
        message = MessagePacket(
            packet_type=PacketType.MESSAGE, data=msg, topic_name=self.topic_name, topic_type=self.topic_type
        )

        if isinstance(data, PointCloud2):
            np_arr = np.array(list(read_points(data)))[:, :3]  # drop intensity...
            cpc = DracoPy.encode(np_arr, compression_level=1)
            message.data["data"] = cpc

        self.results_queue.put_nowait(message)
