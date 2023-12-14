from queue import Full
from typing import Any

from rclpy.node import Node
from rosbridge_library.internal import ros_loader
from rosbridge_library.internal.message_conversion import extract_values


from sensor_msgs.msg import PointCloud2
#from sensor_msgs_py.point_cloud2 import read_points_numpy
import DracoPy
from era_5g_relay_network_application.utils import Compressions



class WorkerSubscriber:
    """
    Worker object for data processing in standalone variant. Reads
    data from passed queue, performs detection and returns results using
    the flask app.
    """

    def __init__(self, topic_name: str, topic_type: str, compression: Compressions, node: Node, queue, **kw):
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
        self.queue = queue
        self.compression = compression
        self.topic_name = topic_name
        self.topic_type = topic_type

    def callback(self, data: Any):
        print("callback")
        msg = extract_values(data)
        
        
        if isinstance(data, PointCloud2) and self.compression == Compressions.DRACO:
            np_arr = read_points_numpy(data, field_names=["x", "y", "z"], skip_nans=True)  # drop intensity, etc....
            cpc = DracoPy.encode(np_arr, compression_level=1)
            msg = cpc

        try:
            self.queue.put_nowait(msg)
        except Full:
            pass
