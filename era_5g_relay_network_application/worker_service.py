import logging
from functools import partial
from multiprocessing.queues import Queue as MpQueue
from queue import Empty, Full, Queue
from threading import Event, Thread
from typing import Any, Dict, Tuple, Union

from rclpy.node import Node  # pants: no-infer-dep
from rclpy.task import Future  # pants: no-infer-dep
from rosbridge_library.internal import ros_loader  # pants: no-infer-dep
from rosbridge_library.internal.message_conversion import extract_values, populate_instance  # pants: no-infer-dep

# TODO can't be used (Union[Queue[ServiceData], MpQueue[ServiceData]]) with Python 3.10 (or older)
# ...mypy is happy but it throws TypeError
ServiceData = Tuple[str, Dict[str, Any]]

SrvQueue = Union[Queue, MpQueue]


class WorkerService(Thread):
    """Worker object for processing service requests."""

    def __init__(
        self,
        service_name: str,
        service_type: str,
        requests_queue: SrvQueue,
        responses_queue: SrvQueue,
        node: Node,
        **kw,
    ) -> None:
        """Constructor.

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
            self.node.get_logger().info(f"Service {service_name} is not available, waiting again...")

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> None:
        """Periodically reads data from python internal queue and process them."""

        logging.debug(f"{self.name} thread is running.")

        while not self.stop_event.is_set():
            try:
                sid, data = self.requests_queue.get(block=True, timeout=1)

                future: Future = self.client.call_async(populate_instance(data, self.inst))
                future.add_done_callback(partial(self.callback_future, sid=sid))
            except (Empty, Full):
                pass

    def callback_future(self, future: Future, sid: str) -> None:
        """Called once the service call is done. Puts the response to the responses queue.

        Args:
            future (Future): The response from the service call
            sid (str): ID of the client that requested the service
        """
        d = extract_values(future.result())
        logging.debug(sid, d)
        self.responses_queue.put_nowait((sid, d))
