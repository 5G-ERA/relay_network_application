import logging
from queue import Full
from threading import Event
from typing import Any

from rclpy.callback_groups import MutuallyExclusiveCallbackGroup  # pants: no-infer-dep
from rclpy.node import Node  # pants: no-infer-dep
from rosbridge_library.internal import ros_loader  # pants: no-infer-dep
from rosbridge_library.internal.message_conversion import extract_values, populate_instance  # pants: no-infer-dep

from era_5g_relay_network_application import AnyQueue


class WorkerServiceServer:
    """Worker object that create a service server to mirror remote functionality."""

    def __init__(
        self,
        service_name: str,
        service_type: str,
        request_queue: AnyQueue,
        response_queue: AnyQueue,
        node: Node,
        **kw,
    ):
        """Constructor.

        Args:
            service_name (str): The name of the service to be mirrored
            service_type (str): The type of the service as a string (e.g. std_srvs.srv.SetBool)
            request_queue (Queue): The queue to put the requests to
            response_queue (Queue): The queue with all responses from the service call
            node (Node): The ROS node for service creation
        """

        super().__init__(**kw)
        self.stop_event = Event()
        self.node = node
        self.request_queue = request_queue
        self.response_queue = response_queue
        service_type_class = ros_loader.get_service_class(service_type)
        self.response_inst = ros_loader.get_service_response_instance(service_type)
        self.service_name = service_name
        node.create_service(
            service_type_class,
            service_name,
            self.callback_service,
            callback_group=MutuallyExclusiveCallbackGroup(),  # each service server has its own callback group to avoid blocking each other
        )

    def callback_service(self, request: Any, response: Any) -> Any:
        """The callback function for the service server. Puts the request to the request queue and waits for the
        response.

        Args:
            request (Any): The request obtained from the service call
            response (Any): The response to be sent back to the service caller

        Returns:
            Any: The response to be sent back to the service caller
        """

        d = extract_values(request)
        try:
            self.request_queue.put_nowait(d)
            resp = populate_instance(self.response_queue.get(block=True), self.response_inst)
        except Full:
            logging.warning("Service request queue is full. Dropping request.")
            return response

        return resp
