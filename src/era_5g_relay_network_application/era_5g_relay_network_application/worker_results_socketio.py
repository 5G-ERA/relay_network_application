import logging
from queue import Empty, Queue
from threading import Event, Thread
from dataclasses import asdict


class WorkerResultsSocketIO(Thread):
    """
    Worker object for data processing in standalone variant. Reads
    data from passed queue, performs detection and returns results using
    the flask app.
    """

    def __init__(self, queue: Queue, subscribers, sio, **kw):
        """
        Constructor

        Args:
            data_queue (Queue): The queue with all to-be-processed data
            app (_type_): The flask app for results publishing
        """

        super().__init__(**kw)
        self.queue: Queue = queue
        self.stop_event = Event()
        self.subscribers = subscribers
        self.sio = sio

    def stop(self):
        self.stop_event.set()

    def get_data(self):
        try:
            return self.queue.get(block=True, timeout=1)
        except Empty:
            return None

    def run(self):
        """
        Periodically reads data from python internal queue process them.
        """

        logging.debug(f"{self.name} thread is running.")

        while not self.stop_event.is_set():
            data = self.get_data()
            if data is None:
                continue

            for s in self.subscribers:
                self.sio.emit(
                    "message",
                    asdict(data),
                    namespace="/results",
                    to=self.sio.manager.sid_from_eio_sid(s, "/results"),
                )
