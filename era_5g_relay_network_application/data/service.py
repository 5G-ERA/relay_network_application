from multiprocessing import Queue
from typing import Optional

import rclpy  # pants: no-infer-dep
from rclpy.qos import QoSProfile  # pants: no-infer-dep

from era_5g_relay_network_application.utils import ActionServiceVariant, ActionSubscribers
from era_5g_relay_network_application.worker_service import ServiceData, WorkerService


class RelayService:
    """Class that holds information about incoming service (i.e. service that is called from relay client), its type and
    related worker."""

    def __init__(
        self,
        service_name: str,
        service_type: str,
        node: rclpy.node.Node,
        qos: Optional[QoSProfile] = None,
        action_service_variant: ActionServiceVariant = ActionServiceVariant.NONE,
        action_subscribers: Optional[ActionSubscribers] = None,
    ):
        self.service_type = service_type

        # Only one call of the service at the time is allowed, therefore queue of length 1 is sufficent
        self.queue_request: Queue[ServiceData] = Queue(1)
        self.queue_response: Queue[ServiceData] = Queue(1)

        self.worker = WorkerService(
            service_name,
            service_type,
            self.queue_request,
            self.queue_response,
            node,
            qos,
            action_service_variant,
            action_subscribers,
        )

        # Service name can be changed by worker in case of action-related services
        self.service_name = self.worker.service_name
        self.channel_name_request = f"service_request/{self.service_name}"
        self.channel_name_response = f"service_response/{self.service_name}"

        self.worker.daemon = True
        self.worker.start()
