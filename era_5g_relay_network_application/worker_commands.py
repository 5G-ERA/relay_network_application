import logging
from multiprocessing.queues import Queue
from queue import Empty
from threading import Event, Thread
from typing import Dict

from era_5g_relay_network_application.data.topic import RelayTopicOutgoing


class WorkerCommands(Thread):
    """Periodically checks if there is a new INIT command in command queue (i.e., new client was connected) and if so,
    it goes through all outgoing topics workers and make them send their saved memory to the clients."""

    def __init__(self, command_queue: Queue, outgoing_topics: Dict[str, RelayTopicOutgoing], **kw):
        super().__init__(**kw)
        self.stop_event = Event()
        self.command_queue = command_queue
        self.outgoing_topics = outgoing_topics

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> None:
        logging.debug(f"{self.name} thread is running.")

        while not self.stop_event.is_set():
            try:
                sid, _ = self.command_queue.get(block=True, timeout=1)
                for topic in self.outgoing_topics.values():
                    topic.worker.flush_memory(sid)

            except Empty:
                continue
