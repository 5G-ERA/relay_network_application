from functools import partial
import logging
from queue import Empty, Full, Queue
from threading import Event, Thread
import time
from typing import List
from rclpy.task import Future

from rclpy.node import Node

from rosbridge_library.internal.message_conversion import extract_values



from geometry_msgs.msg import TransformStamped
from tf2_msgs.msg import TFMessage
from era_5g_tf2json.tf2_web_republisher import TFRepublisher

class WorkerTF():
    """
    Worker object for data processing in standalone variant. Reads
    data from passed queue, performs detection and returns results using
    the flask app.
    """

    def __init__(self, transforms_to_listen: List, queue: Queue, node: Node, **kw):
        """
        Constructor

        Args:
            data_queue (Queue): The queue with all to-be-processed data
            app (_type_): The flask app for results publishing
        """

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
        

   
