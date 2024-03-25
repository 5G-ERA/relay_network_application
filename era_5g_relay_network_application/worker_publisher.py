import logging
import time
from queue import Empty
from threading import Event, Thread
from typing import Any, Optional, Tuple

import DracoPy
from rclpy.node import Node  # pants: no-infer-dep
from rclpy.qos import QoSDurabilityPolicy, QoSProfile  # pants: no-infer-dep
from rosbridge_library.internal import ros_loader  # pants: no-infer-dep
from rosbridge_library.internal.message_conversion import FieldTypeMismatchException  # pants: no-infer-dep
from rosbridge_library.internal.message_conversion import populate_instance  # pants: no-infer-dep; pants: no-infer-dep
from sensor_msgs.msg import LaserScan, PointCloud2  # pants: no-infer-dep
from sensor_msgs_py.point_cloud2 import create_cloud_xyz32  # pants: no-infer-dep

from era_5g_interface.measuring import Measuring
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
        node: Node,
        compression: Optional[Compressions],
        qos: Optional[QoSProfile] = None,
        action_topic_variant: ActionTopicVariant = ActionTopicVariant.NONE,
        extended_measuring: bool = False,
        **kw,
    ) -> None:
        """

        Args:
            extended_measuring (bool): Enable logging of measuring.
        """

        super().__init__(**kw)
        self.queue = queue
        self.topic_name = topic_name
        self.compression = compression

        self.stop_event = Event()

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

            assert qos is None, "QoS should not be set for action status"

            # Status topic must have different qos profile
            # This is necessary for action to be recognized as being available by wait_for_server() call
            qos = QoSProfile(durability=QoSDurabilityPolicy.RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, depth=10)

        # Create publisher
        self.pub = node.create_publisher(self.topic_type_class, self.topic_name, qos if qos is not None else 10)

        self._extended_measuring = extended_measuring
        self._measuring = Measuring(
            measuring_items={
                "key_timestamp": 0,
                "before_get_data_timestamp": 0,
                "after_get_data_timestamp": 0,
                "before_publish_timestamp": 0,
                "after_publish_timestamp": 0,
            },
            enabled=self._extended_measuring,
            filename_prefix="publisher-" + self.topic_name.replace("/", ""),
        )

    def put_data(self, data: Any) -> None:
        self.queue.put_nowait(data)

    def stop(self) -> None:
        self.stop_event.set()

    def get_data(self) -> Optional[Tuple[Any, int]]:
        try:
            data, timestamp = self.queue.get(block=True, timeout=1)

            # Create new instance for each message
            inst = self.topic_type_class()

            if isinstance(inst, PointCloud2) and self.compression == Compressions.DRACO:
                return (
                    create_cloud_xyz32(populate_instance(data, inst).header, DracoPy.decode(data["data"]).points),
                    timestamp,
                )
            if isinstance(inst, LaserScan):
                data["ranges"][:] = [x if x is not None else float("inf") for x in data["ranges"]]
            return populate_instance(data, inst), timestamp
        except Empty:
            return None
        except (FieldTypeMismatchException, TypeError) as ex:
            logging.warning(f"Failed to convert message: {ex}")
            return None

    def run(self) -> None:
        """Periodically reads data from python internal queue process them."""

        logging.debug(f"{self.name} thread is running.")

        while not self.stop_event.is_set():
            before_get_data_timestamp = time.perf_counter_ns()
            data_timestamp = self.get_data()
            after_get_data_timestamp = time.perf_counter_ns()

            if data_timestamp is None:
                continue
            data, timestamp = data_timestamp

            before_publish_timestamp = time.perf_counter_ns()
            self.pub.publish(data)
            after_publish_timestamp = time.perf_counter_ns()
            self._measuring.log_measuring(timestamp, "before_get_data_timestamp", before_get_data_timestamp)
            self._measuring.log_measuring(timestamp, "after_get_data_timestamp", after_get_data_timestamp)
            self._measuring.log_measuring(timestamp, "before_publish_timestamp", before_publish_timestamp)
            self._measuring.log_measuring(timestamp, "after_publish_timestamp", after_publish_timestamp)
            self._measuring.store_measuring(timestamp)
