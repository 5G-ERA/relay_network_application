from queue import Empty, Queue
import logging
from threading import Event, Thread
#import rospy
import rclpy
from rclpy.node import Node
from rosbridge_library.internal.message_conversion import populate_instance, FieldTypeMismatchException
from rosbridge_library.internal import ros_loader


class Worker(Thread):
    """
    Worker object for data processing in standalone variant. Reads
    data from passed queue, performs detection and returns results using
    the flask app.
    """

    def __init__(self, queue: Queue, topic_name, topic_type, node: Node, **kw):
        """
        Constructor

        Args:
            data_queue (Queue): The queue with all to-be-processed data
            app (_type_): The flask app for results publishing
        """

        super().__init__(**kw)
        self.queue: Queue = queue
        self.stop_event = Event()
        inst = ros_loader.get_message_instance(topic_type)
        self.pub = node.create_publisher(type(inst), topic_name, 10)
        #self.pub = rospy.Publisher(topic_name, type(inst), queue_size=10)
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

            self.pub.publish(data)
