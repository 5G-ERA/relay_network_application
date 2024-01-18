from multiprocessing.queues import Queue
from typing import Any, Optional

from era_5g_interface.exceptions import BackPressureException
from era_5g_relay_network_application import SendFunctionProtocol
from era_5g_relay_network_application.worker_socketio import WorkerSocketIO


class WorkerSocketIOServer(WorkerSocketIO):
    """Worker object for sending data to multiple subscribed clients over socket io."""

    def __init__(self, queue: Queue, subscribers, send_function: Optional[SendFunctionProtocol], **kw):
        super().__init__(queue, send_function, **kw)
        self.subscribers = subscribers

    def send_data(self, data: Any):
        assert self.send_function

        for s in self.subscribers:
            try:
                self.send_function(data, sid=s)
            except BackPressureException:
                print("apply backpressure")
