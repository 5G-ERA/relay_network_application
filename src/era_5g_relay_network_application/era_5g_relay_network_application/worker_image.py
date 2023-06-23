import base64
from queue import Empty, Queue
import time
import cv2
import flask_socketio
import logging
from threading import Event, Thread

import numpy as np
import rospy
from rosbridge_library.internal.message_conversion import populate_instance
from rosbridge_library.internal import ros_loader

from sensor_msgs.msg import LaserScan
from cv_bridge import CvBridge

from era_5g_relay_network_application.worker import Worker


class WorkerImage(Worker):
    """
    Worker object for data processing in standalone variant. Reads 
    data from passed queue, performs detection and returns results using
    the flask app. 
    """

    def __init__(self, queue: Queue, topic_name, topic_type, **kw):
        super().__init__(queue, topic_name, topic_type, **kw)
        self.bridge = CvBridge()

    def get_data(self):
        try:
            d = self.queue.get(block=True, timeout=1)
            
            frame = base64.b64decode(d)
            img = cv2.imdecode(np.frombuffer(frame, dtype=np.uint8), cv2.IMREAD_COLOR)
            return self.bridge.cv2_to_imgmsg(img, encoding='bgr8')
            # TODO: add timestamp
        except Empty:
            return None 
    
        
        