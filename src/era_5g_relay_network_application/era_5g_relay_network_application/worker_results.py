from queue import Empty, Queue
import time
from typing import Any, Set
import flask_socketio
import logging
from threading import Event, Thread
import rospy
from rosbridge_library.internal.message_conversion import populate_instance, extract_values
from rosbridge_library.internal import ros_loader
from socketio import Server


from sensor_msgs.msg import LaserScan


class WorkerResults():
    """
    Worker object for data processing in standalone variant. Reads 
    data from passed queue, performs detection and returns results using
    the flask app. 
    """

    def __init__(self, topic_name: str, topic_type: str, sio: Server, subscribers: Set, **kw):
        """
        Constructor

        Args:
            data_queue (Queue): The queue with all to-be-processed data
            app (_type_): The flask app for results publishing
        """

        super().__init__(**kw)
        self.stop_event = Event()
        
        inst = ros_loader.get_message_instance(topic_type)
        self.pub = rospy.Subscriber(topic_name, type(inst), queue_size=10, callback=self.callback)
        self.inst = inst
        self.sio = sio
        self.subscribers = subscribers
        self.topic_name = topic_name
        self.topic_type = topic_type

    def callback(self, data: Any):
        msg = extract_values(data)
        message = {"topic_name": self.topic_name, "topic_type": self.topic_type, "msg": msg} 
        
        for s in self.subscribers:
            self.sio.emit("message", message, namespace="/results", to=self.sio.manager.sid_from_eio_sid(s, "/results"))
        

    
