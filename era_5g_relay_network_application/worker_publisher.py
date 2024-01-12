import logging
from queue import Empty
from threading import Event, Thread
from typing import Any, Optional

import DracoPy
from rclpy.node import Node  # pants: no-infer-dep
from rclpy.qos import QoSDurabilityPolicy, QoSProfile
from rosbridge_library.internal import ros_loader  # pants: no-infer-dep
from rosbridge_library.internal.message_conversion import FieldTypeMismatchException  # pants: no-infer-dep
from rosbridge_library.internal.message_conversion import populate_instance  # pants: no-infer-dep; pants: no-infer-dep
from sensor_msgs.msg import LaserScan, PointCloud2  # pants: no-infer-dep
from sensor_msgs_py.point_cloud2 import create_cloud_xyz32  # pants: no-infer-dep

from era_5g_relay_network_application import AnyQueue
from era_5g_relay_network_application.utils import ActionTopicVariant, Compressions

try:
    from rosbridge_library.internal.ros_loader import get_action_class
except ImportError:
    from era_5g_relay_network_application.compatibility.rosbridge_action_loader import get_action_class


class WorkerPublisher(Thread):
    """Worker object for publishing topic data."""

    def __init__(
        self,
        queue: AnyQueue,
        topic_name: str,
        topic_type: str,
        compression: Compressions,
        node: Node,
        action_topic_variant: ActionTopicVariant = ActionTopicVariant.NONE,
        **kw,
    ) -> None:
        super().__init__(**kw)
        self.queue = queue
        self.topic_name = topic_name
        self.compression = compression

        self.stop_event = Event()
        qos_profile = 10  # history depth (in case of the most simple profile)

        if action_topic_variant == ActionTopicVariant.NONE:
            self.topic_type_class = ros_loader.get_message_class(topic_type)
        else:
            # Get Action type (instead of regular topic type)
            action_type_class = get_action_class(topic_type)
            action_name = topic_name

        if action_topic_variant == ActionTopicVariant.ACTION_FEEDBACK:
            self.topic_name = f"{action_name}/_action/feedback"
            self.topic_type_class = action_type_class.Impl.FeedbackMessage

        elif action_topic_variant == ActionTopicVariant.ACTION_STATUS:
            self.topic_name = f"{action_name}/_action/status"
            self.topic_type_class = action_type_class.Impl.GoalStatusMessage

            # Status topic must have different qos profile
            # This is necessary for action to be recognized as being available by wait_for_server() call
            qos_profile = QoSProfile(durability=QoSDurabilityPolicy.RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, depth=10)

        # Create publisher
        self.pub = node.create_publisher(self.topic_type_class, self.topic_name, qos_profile)

    def put_data(self, data: Any) -> None:
        self.queue.put_nowait(data)

    def stop(self) -> None:
        self.stop_event.set()

    def get_data(self) -> Optional[Any]:
        try:
            d = self.queue.get(block=True, timeout=1)

            # Create new instance for each message
            inst = self.topic_type_class()

            if isinstance(inst, PointCloud2) and self.compression == Compressions.DRACO:
                return create_cloud_xyz32(populate_instance(d, inst).header, DracoPy.decode(d["data"]).points)
            if isinstance(inst, LaserScan):
                d["ranges"][:] = [x if x is not None else float("inf") for x in d["ranges"]]
            return populate_instance(d, inst)
        except Empty:
            return None
        except (FieldTypeMismatchException, TypeError) as ex:
            logging.warning(f"Failed to convert message: {ex}")
            return None

    def run(self) -> None:
        """Periodically reads data from python internal queue process them."""

        logging.debug(f"{self.name} thread is running.")

        while not self.stop_event.is_set():
            data = self.get_data()

            if data is None:
                continue

            self.pub.publish(data)
