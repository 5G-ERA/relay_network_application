import logging
from threading import Event, Thread
from typing import Any

from rclpy.node import Node  # pants: no-infer-dep
from rosbridge_library.internal.message_conversion import extract_values  # pants: no-infer-dep
from rosgraph_msgs.msg import Clock  # pants: no-infer-dep

from era_5g_interface.exceptions import BackPressureException
from era_5g_interface.utils.rate_timer import RateTimer
from era_5g_relay_network_application import SendFunctionProtocol


class WorkerClock(Thread):
    """Worker object for sending data over socket io."""

    def __init__(self, send_function: SendFunctionProtocol, node: Node, **kw):
        super().__init__(**kw)
        self.stop_event = Event()
        self.send_function = send_function
        self.rt = RateTimer(20)
        self.node = node

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> None:
        logging.debug(f"{self.name} thread is running.")

        while not self.stop_event.is_set():
            cl = Clock()
            cl.clock = self.node.get_clock().now().to_msg()
            data = extract_values(cl)
            self.send_data(data)
            self.rt.sleep()

    def send_data(self, data: Any) -> None:
        assert self.send_function is not None
        try:
            self.send_function(data)
        except BackPressureException:
            logging.warning("Backpressure applied.")
