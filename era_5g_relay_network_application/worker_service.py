import logging
from functools import partial
from multiprocessing.queues import Queue as MpQueue
from queue import Empty, Full, Queue
from threading import Event, Thread
from typing import Any, Dict, List, Optional, Tuple, Union

from rclpy.node import Node  # pants: no-infer-dep
from rclpy.task import Future  # pants: no-infer-dep
from rosbridge_library.internal import ros_loader  # pants: no-infer-dep
from rosbridge_library.internal.message_conversion import extract_values, populate_instance  # pants: no-infer-dep

try:
    from rosbridge_library.internal.ros_loader import get_action_class
except ImportError:
    from era_5g_relay_network_application.compatibility.rosbridge_action_loader import get_action_class

from era_5g_relay_network_application.utils import ActionServiceVariant, ActionSubscribers

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
        action_service_variant: ActionServiceVariant = ActionServiceVariant.NONE,
        action_subscribers: Optional[ActionSubscribers] = None,
        **kw,
    ) -> None:
        """Constructor.

        Args:
            service_name (str): The name of the service for which the client is created
            service_type (str): The type of the service as a string (e.g. std_srvs.srv.SetBool)
            requests_queue (Queue): The queue with all to-be-processed requests
            responses_queue (Queue): The queue to put the responses to
            node (Node): The ROS node for subscription
            action_service_variant (ActionServiceVariant, Optional):
                Specification of variant in case action-related service.
                Default is ActionServiceVariant.NONE (used for regular services).
            action_subscribers (ActionSubscribers, Optional): Used only for action-related services.
                The action_subscribers structure holds information about which client requested which goal.
                Default is None (for regular services).
        """

        super().__init__(**kw)
        self.stop_event = Event()
        self.service_name = service_name
        self.requests_queue = requests_queue
        self.responses_queue = responses_queue
        self.node = node
        self.action_service_variant = action_service_variant
        self.action_subscribers = action_subscribers

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

        self.client = self.node.create_client(self.service_type_class, self.service_name)

        while not self.client.wait_for_service(timeout_sec=1.0):
            self.node.get_logger().info(f"Service {self.service_name} is not available, waiting again...")

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> None:
        """Periodically reads data from python internal queue and process them."""

        logging.debug(f"{self.name} thread is running.")

        while not self.stop_event.is_set():
            try:
                sid, data = self.requests_queue.get(block=True, timeout=1)

                if self.action_service_variant == ActionServiceVariant.ACTION_SEND_GOAL:
                    goal_uuid = data["goal_id"]["uuid"]
                    assert self.action_subscribers is not None  # prevents mypy error
                    self.action_subscribers.set_sid_for_goal_id(goal_uuid, sid)

                inst = self.service_type_class.Request()

                future: Future = self.client.call_async(populate_instance(data, inst))
                future.add_done_callback(partial(self.callback_future, sid=sid, request_data=data))
            except (Empty, Full):
                pass

    def callback_future(self, future: Future, sid: str, request_data: Dict) -> None:
        """Called once the service call is done. Puts the response to the responses queue.

        Args:
            future (Future): The response from the service call
            sid (str): ID of the client that requested the service
        """
        d = extract_values(future.result())
        logging.debug(sid, d)

        # In case of action-related services, remove SIDs of old goals
        if (
            self.action_service_variant == ActionServiceVariant.ACTION_SEND_GOAL and not d["accepted"]
        ) or self.action_service_variant == ActionServiceVariant.ACTION_GET_RESULT:
            goal_uuid = request_data["goal_id"]["uuid"]
            assert self.action_subscribers is not None  # prevents mypy error
            self.action_subscribers.remove_sid_for_goal_id([goal_uuid])
        elif self.action_service_variant == ActionServiceVariant.ACTION_CANCEL_GOAL:
            # if cancelling was accepted, remove sid as well (the may be multiple cancelled goals)
            goals_to_remove: List[str] = []
            for i in range(len(d["goals_canceling"])):
                goals_to_remove.append(d["goals_canceling"][i]["goal_id"]["uuid"])
            assert self.action_subscribers is not None  # prevents mypy error
            self.action_subscribers.remove_sid_for_goal_id(goals_to_remove)

        self.responses_queue.put((sid, d))
