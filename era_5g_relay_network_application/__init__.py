from multiprocessing.queues import Queue as MpQueue
from queue import Queue
from typing import Any, Optional, Protocol, Union

AnyQueue = Union[Queue, MpQueue]


class SendFunctionProtocol(Protocol):
    # Define types here, as if __call__ were a function (ignore self).
    def __call__(self, data: Any, sid: Optional[str] = None) -> None:
        ...
