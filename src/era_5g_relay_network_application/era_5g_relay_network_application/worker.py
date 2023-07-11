from queue import Empty, Queue
import time
import flask_socketio
import logging
from threading import Event, Thread
import rospy
from rosbridge_library.internal.message_conversion import populate_instance, FieldTypeMismatchException
from rosbridge_library.internal import ros_loader

from sensor_msgs.msg import LaserScan


class Worker(Thread):
    """
    Worker object for data processing in standalone variant. Reads 
    data from passed queue, performs detection and returns results using
    the flask app. 
    """

    def __init__(self, queue: Queue, topic_name, topic_type, **kw):
        """
        Constructor

        Args:
            data_queue (Queue): The queue with all to-be-processed data
            app (_type_): The flask app for results publishing
        """

        super().__init__(**kw)
        self.queue: Queue = queue
        self.stop_event = Event()
        #self.nh = rospy.init_node('publisher', anonymous=True, disable_signals=True)
        inst = ros_loader.get_message_instance(topic_type)
        self.pub = rospy.Publisher(topic_name, type(inst), queue_size=10)
        self.inst = inst


    def stop(self):
        self.stop_event.set()

    def get_data(self):
        try:
            d = self.queue.get(block=True, timeout=1)
            return populate_instance(d, self.inst)
        except Empty:
            return None 
        except FieldTypeMismatchException:
            logging.warn("Failed to convert message")
            return None
    
        
        
    def run(self):
        """
        Periodically reads data from python internal queue process them.
        """

        logging.debug(f"{self.name} thread is running.")

        while not self.stop_event.is_set():
            data = self.get_data()

            if data is None: 
                continue
            #message = {"topic_name": "/robot/front_laser/scan", "topic_type": "sensor_msgs/LaserScan", "msg": d}
            self.pub.publish(data)
