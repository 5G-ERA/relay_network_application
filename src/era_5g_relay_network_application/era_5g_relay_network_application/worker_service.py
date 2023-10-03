import logging
from queue import Empty, Full, Queue
from threading import Event, Thread
from rclpy.task import Future

from rclpy.node import Node
import rclpy
from era_5g_relay_network_application.data.packets import PacketType, ServiceRequestPacket, ServiceResponsePacket
from rosbridge_library.internal import ros_loader
from rosbridge_library.internal.message_conversion import populate_instance, extract_values

from sensor_msgs.msg import LaserScan


class WorkerService(Thread):
    """
    Worker object for data processing in standalone variant. Reads
    data from passed queue, performs detection and returns results using
    the flask app.
    """

    def __init__(self, services_requests_queue: Queue, services_responses_queue: Queue, node: Node, **kw):
        """
        Constructor

        Args:
            data_queue (Queue): The queue with all to-be-processed data
            app (_type_): The flask app for results publishing
        """

        super().__init__(**kw)
        self.stop_event = Event()
        self.node = node
        self.services_requests_queue = services_requests_queue
        self.services_responses_queue = services_responses_queue

    def stop(self):
        self.stop_event.set()

    def run(self):
        """
        Periodically reads data from python internal queue process them.
        """

        logging.debug(f"{self.name} thread is running.")

        while not self.stop_event.is_set():
            try:
                sid: str
                packet: ServiceRequestPacket
                sid, packet = self.services_requests_queue.get(block=True, timeout=1)
                proxy = self.node.create_client(ros_loader.get_service_class(packet.service_type), packet.service_name)
                inst = ros_loader.get_service_request_instance(packet.service_type)
                # Populate the instance with the provided args
                while not proxy.wait_for_service(timeout_sec=1.0):
                    self.node.get_logger().info('Service is not available, waiting again...')
                future: Future = proxy.call_async(populate_instance(packet.data, inst))
                rclpy.spin_until_future_complete(self.node, future)
                d = extract_values(future.result())
                message = ServiceResponsePacket(
                    packet_type=PacketType.SERVICE_RESPONSE,
                    data=d,
                    service_name=packet.service_name,
                    service_type=packet.service_type,
                    id=packet.id,
                )
                self.services_responses_queue.put_nowait((sid, message))
            except (Empty, Full) as _:
                pass


            
