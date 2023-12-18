from multiprocessing.queues import Queue
from typing import Any, Optional

from era_5g_interface.exceptions import BackPressureException
from era_5g_relay_network_application import SendFunctionProtocol
from era_5g_relay_network_application.worker_socketio import WorkerSocketIO


class WorkerSocketIOServer(WorkerSocketIO):
    """Worker object for data processing in standalone variant.

    Reads data from passed queue, performs detection and returns results using the flask app.
    """

    def __init__(self, queue: Queue, subscribers, send_function: Optional[SendFunctionProtocol], **kw):
        """Constructor.

        Args:
            data_queue (Queue): The queue with all to-be-processed data
            app (_type_): The flask app for results publishing
        """

        super().__init__(queue, send_function, **kw)
        self.subscribers = subscribers

    def send_data(self, data: Any):
        assert self.send_function

        for s in self.subscribers:
            try:
                self.send_function(data, sid=s)
            except BackPressureException:
                print("apply backpressure")
