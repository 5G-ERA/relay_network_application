import logging
from queue import Empty
from threading import Event, Thread
from typing import Any, Optional

from era_5g_interface.exceptions import BackPressureException
from era_5g_relay_network_application import AnyQueue, SendFunctionProtocol


class WorkerSocketIO(Thread):
    """Worker object for data processing in standalone variant.

    Reads data from passed queue, performs detection and returns results using the flask app.
    """

    def __init__(self, queue: AnyQueue, send_function: Optional[SendFunctionProtocol] = None, **kw):
        """Constructor.

        Args:
            data_queue (Queue): The queue with all to-be-processed data
            app (_type_): The flask app for results publishing
        """

        super().__init__(**kw)
        self.queue = queue
        self.stop_event = Event()
        self.send_function = send_function

    def stop(self) -> None:
        self.stop_event.set()

    def get_data(self) -> Optional[Any]:
        try:
            return self.queue.get(block=True, timeout=1)
        except Empty:
            return None

    def run(self) -> None:
        """Periodically reads data from python internal queue process them."""

        logging.debug(f"{self.name} thread is running.")

        while not self.stop_event.is_set():
            data = self.get_data()
            if data is None:
                continue
            self.send_data(data)

    def send_data(self, data: Any) -> None:
        assert self.send_function is not None
        try:
            self.send_function(data)
        except BackPressureException:
            logging.warning("Backpressure applied.")
