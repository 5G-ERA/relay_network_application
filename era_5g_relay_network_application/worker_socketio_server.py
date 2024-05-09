import logging
from multiprocessing.queues import Queue
from typing import Any, Optional

from socketio.exceptions import BadNamespaceError

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
        if isinstance(data, tuple):  # if the data is tupple, it contains the sid of the intended receiver
            sid, msg = data
            self.send_function(data=msg, sid=sid)
        else:
            for s in self.subscribers:
                try:
                    self.send_function(data, sid=s)
                except BackPressureException:
                    logging.warning("Backpressure applied.")
                except BadNamespaceError:
                    logging.warning("Trying to send data while not connected to the network application")
