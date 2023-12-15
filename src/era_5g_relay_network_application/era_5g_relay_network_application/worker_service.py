from functools import partial
import logging
from queue import Empty, Full, Queue
from threading import Event, Thread
from rclpy.task import Future

from rclpy.node import Node
import rclpy
from rosbridge_library.internal import ros_loader
from rosbridge_library.internal.message_conversion import populate_instance, extract_values



class WorkerService(Thread):
    """ Worker object for processing service requests.
    """

    def __init__(self, service_name: str, service_type: str, requests_queue: Queue, responses_queue: Queue, node: Node, **kw):
        """ Constructor

        Args:
            service_name (str): The name of the service for which the client is created
            service_type (str): The type of the service as a string (e.g. std_srvs.srv.SetBool)
            requests_queue (Queue): The queue with all to-be-processed requests
            responses_queue (Queue): The queue to put the responses to
            node (Node): The ROS node for subscription
        """

        super().__init__(**kw)
        self.stop_event = Event()
        self.node = node
        self.requests_queue = requests_queue
        self.responses_queue = responses_queue
        self.inst = ros_loader.get_service_request_instance(service_type)
        
        self.client = self.node.create_client(ros_loader.get_service_class(service_type), service_name)
        
        
        while not self.client.wait_for_service(timeout_sec=1.0):
            self.node.get_logger().info(f'Service {service_name} is not available, waiting again...')
        

    def stop(self):
        self.stop_event.set()

    def run(self):
        """
        Periodically reads data from python internal queue and process them.
        """

        logging.debug(f"{self.name} thread is running.")

        while not self.stop_event.is_set():
            try:
                sid, data = self.requests_queue.get(block=True, timeout=1)
                
                future: Future = self.client.call_async(populate_instance(data, self.inst))
                future.add_done_callback(partial(self.callback_future, sid=sid))
            except (Empty, Full):
                pass
            
    def callback_future(self, future: Future, sid: str):
        """ Called once the service call is done. Puts the response to the responses queue.

        Args:
            future (Future): The response from the service call
            sid (str): ID of the client that requested the service
        """
        d = extract_values(future.result())
        logging.debug(sid, d)
        self.responses_queue.put_nowait((sid, d))


            
