import logging
from queue import Empty
from threading import Event, Thread
from typing import Any, Optional

import DracoPy
from rclpy.node import Node  # pants: no-infer-dep
from rosbridge_library.internal import ros_loader  # pants: no-infer-dep
from rosbridge_library.internal.message_conversion import FieldTypeMismatchException  # pants: no-infer-dep
from rosbridge_library.internal.message_conversion import populate_instance  # pants: no-infer-dep; pants: no-infer-dep
from sensor_msgs.msg import LaserScan, PointCloud2  # pants: no-infer-dep
from sensor_msgs_py.point_cloud2 import create_cloud_xyz32  # pants: no-infer-dep

from era_5g_relay_network_application import AnyQueue
from era_5g_relay_network_application.utils import Compressions


class WorkerPublisher(Thread):
    """Worker object for data processing in standalone variant.

    Reads data from passed queue, performs detection and returns results using the flask app.
    """

    def __init__(
        self, queue: AnyQueue, topic_name: str, topic_type: str, compression: Compressions, node: Node, **kw
    ) -> None:
        """Constructor.

        Args:
            data_queue (Queue): The queue with all to-be-processed data
            app (_type_): The flask app for results publishing
        """

        super().__init__(**kw)
        self.queue = queue
        self.stop_event = Event()
        inst = ros_loader.get_message_instance(topic_type)
        self.pub = node.create_publisher(type(inst), topic_name, 1)
        self.inst = inst
        self.compression = compression

    def put_data(self, data: Any) -> None:
        self.queue.put_nowait(data)

    def stop(self) -> None:
        self.stop_event.set()

    def get_data(self) -> Optional[Any]:
        try:
            d = self.queue.get(block=True, timeout=1)

            if isinstance(self.inst, PointCloud2) and self.compression == Compressions.DRACO:
                return create_cloud_xyz32(populate_instance(d, self.inst).header, DracoPy.decode(d["data"]).points)
            if isinstance(self.inst, LaserScan):
                d["ranges"][:] = [x if x is not None else float("inf") for x in d["ranges"]]
            return populate_instance(d, self.inst)
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
