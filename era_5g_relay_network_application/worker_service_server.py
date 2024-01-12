import logging
from queue import Full
from threading import Event
from typing import Any

from rclpy.callback_groups import MutuallyExclusiveCallbackGroup  # pants: no-infer-dep
from rclpy.node import Node  # pants: no-infer-dep
from rosbridge_library.internal import ros_loader  # pants: no-infer-dep
from rosbridge_library.internal.message_conversion import extract_values, populate_instance  # pants: no-infer-dep

try:
    from rosbridge_library.internal.ros_loader import get_action_class
except ImportError:
    from era_5g_relay_network_application.compatibility.rosbridge_action_loader import get_action_class

from era_5g_relay_network_application import AnyQueue
from era_5g_relay_network_application.utils import ActionServiceVariant


class WorkerServiceServer:
    """Worker object that create a service server to mirror remote functionality."""

    def __init__(
        self,
        service_name: str,
        service_type: str,
        request_queue: AnyQueue,
        response_queue: AnyQueue,
        node: Node,
        action_service_variant: ActionServiceVariant = ActionServiceVariant.NONE,
        **kw,
    ):
        """Constructor.

        Args:
            service_name (str): The name of the service to be mirrored
            service_type (str): The type of the service as a string (e.g. std_srvs.srv.SetBool)
            request_queue (Queue): The queue to put the requests to
            response_queue (Queue): The queue with all responses from the service call
            node (Node): The ROS node for service creation
            action_service_variant (ActionServiceVariant): Specification of service variant in case the service is
                related to a particular ROS Action. Default is ActionServiceVariant.NONE, which defines a regular
                service not related to any Action.
        """

        super().__init__(**kw)
        self.stop_event = Event()
        self.service_name = service_name
        self.request_queue = request_queue
        self.response_queue = response_queue
        self.node = node
        self.action_service_variant = action_service_variant

        assert isinstance(action_service_variant, ActionServiceVariant), "Invalid value of action_service_variant."

        if action_service_variant == ActionServiceVariant.NONE:
            # Get type for regular Service
            self.service_type_class = ros_loader.get_service_class(service_type)
        else:
            # Get Action type (instead of regular Service type)
            action_type_class = get_action_class(service_type)
            action_name = service_name

        if action_service_variant == ActionServiceVariant.ACTION_SEND_GOAL:
            self.service_type_class = action_type_class.Impl.SendGoalService
            self.service_name = f"{action_name}/_action/send_goal"

        elif action_service_variant == ActionServiceVariant.ACTION_CANCEL_GOAL:
            self.service_type_class = action_type_class.Impl.CancelGoalService
            self.service_name = f"{action_name}/_action/cancel_goal"

        elif action_service_variant == ActionServiceVariant.ACTION_GET_RESULT:
            self.service_type_class = action_type_class.Impl.GetResultService
            self.service_name = f"{action_name}/_action/get_result"

        node.create_service(
            self.service_type_class,
            self.service_name,
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
            response_inst = self.service_type_class.Response()
            resp = populate_instance(self.response_queue.get(block=True), response_inst)
        except Full:
            logging.warning("Service request queue is full. Dropping request.")
            return response

        return resp
