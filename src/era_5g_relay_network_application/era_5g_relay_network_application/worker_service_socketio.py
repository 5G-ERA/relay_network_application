import logging
from queue import Empty, Queue
from threading import Event, Thread
from dataclasses import asdict


class WorkerServiceSocketIO(Thread):
    """
    Worker object for data processing in standalone variant. Reads
    data from passed queue, performs detection and returns results using
    the flask app.
    """

    def __init__(self, queue: Queue, sio, **kw):
        """
        Constructor

        Args:
            data_queue (Queue): The queue with all to-be-processed data
            app (_type_): The flask app for results publishing
        """

        super().__init__(**kw)
        self.queue: Queue = queue
        self.stop_event = Event()
        self.sio = sio

    def stop(self):
        self.stop_event.set()

    def run(self):
        """
        Periodically reads data from python internal queue process them.
        """

        logging.debug(f"{self.name} thread is running.")

        while not self.stop_event.is_set():
            try:
                sid, data = self.queue.get(block=True, timeout=1)
                if data is None:
                    continue

                self.sio.emit(
                    "message",
                    asdict(data),
                    namespace="/results",
                    to=self.sio.manager.sid_from_eio_sid(sid, "/results"),
                )
            except Empty:
                continue
