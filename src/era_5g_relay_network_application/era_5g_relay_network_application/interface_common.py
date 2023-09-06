import os
from collections.abc import Iterator
from threading import Lock
from typing import Dict, Set

import socketio
from engineio.payload import Payload
from flask import Flask

from era_5g_interface.dataclasses.control_command import ControlCommand, ControlCmdType

# port of the netapp's server
NETAPP_PORT = int(os.getenv("NETAPP_PORT", 5896))

# to get rid of ValueError: Too many packets in payload (see https://github.com/miguelgrinberg/python-engineio/issues/142)
Payload.max_decode_packets = 50

# the max_http_buffer_size parameter defines the max size of the message to be passed
sio = socketio.Server(async_mode="threading", max_http_buffer_size=5 * (1024 ** 2))
app = Flask(__name__)
app.wsgi_app = socketio.WSGIApp(sio, app.wsgi_app)  # type: ignore


class LockedSet(set):
    """A set which can be safely iterated and modified from different threads"""

    def __init__(self, *args, **kwargs):
        self._lock = Lock()
        super(LockedSet, self).__init__(*args, **kwargs)

    def add(self, elem):
        with self._lock:
            super(LockedSet, self).add(elem)

    def remove(self, elem):
        with self._lock:
            super(LockedSet, self).remove(elem)

    def discard(self, elem) -> None:
        with self._lock:
            return super(LockedSet, self).remove(elem)

    def __contains__(self, elem):
        with self._lock:
            super(LockedSet, self).__contains__(elem)

    def locked_iter(self, it) -> Iterator:
        it = iter(it)

        while True:
            try:
                with self._lock:
                    value = next(it)
            except StopIteration:
                return
            yield value

    def __iter__(self) -> Iterator:
        return self.locked_iter(super().__iter__())


result_subscribers: Set[str] = LockedSet()


class ArgFormatError(Exception):
    pass


def get_sid_of_namespace(eio_sid, namespace):
    return sio.manager.sid_from_eio_sid(eio_sid, namespace)


def get_results_sid(eio_sid):
    return sio.manager.sid_from_eio_sid(eio_sid, "/results")


@sio.on("connect", namespace="/data")
def connect_data(sid, environ):
    """_summary_
    Creates a websocket connection to the client for passing the data.

    Raises:
        ConnectionRefusedError: Raised when attempt for connection were made
            without registering first.
    """
    print(f"Connected data. Session id: {sio.manager.eio_sid_from_sid(sid, '/data')}, namespace_id: {sid}")
    sio.send("you are connected", namespace="/data", to=sid)


@sio.on("connect", namespace="/control")
def connect_control(sid, environ):
    """_summary_
    Creates a websocket connection to the client for passing control commands.

    Raises:
        ConnectionRefusedError: Raised when attempt for connection were made
            without registering first.
    """

    print(f"Connected control. Session id: {sio.manager.eio_sid_from_sid(sid, '/control')}, namespace_id: {sid}")
    sio.send("you are connected", namespace="/control", to=sid)


@sio.on("connect", namespace="/results")
def connect_results(sid, environ):
    """
    Creates a websocket connection to the client for passing the results.

    Raises:
        ConnectionRefusedError: Raised when attempt for connection were made
            without registering first.
    """

    print(f"Connected results. Session id: {sio.manager.eio_sid_from_sid(sid, '/results')}, namespace_id: {sid}")
    sio.send("You are connected", namespace="/results", to=sid)


@sio.on("command", namespace="/control")
def command_callback_websocket(sid, data: Dict):
    eio_sid = sio.manager.eio_sid_from_sid(sid, '/control')
    try:
        command = ControlCommand(**data)
    except TypeError as e:
        print(f"Could not parse Control Command. {str(e)}")
        sio.emit(
            "control_cmd_error",
            {"error": f"Could not parse Control Command. {str(e)}"},
            namespace='/control',
            to=sid
        )
        return

    print(f"Control command {command} processing: session id: {sid}")  # check if the client wants to receive results
    if command and command.cmd_type == ControlCmdType.INIT:
        args = command.data
        if args:
            sr = args.get("subscribe_results")
            if sr:
                result_subscribers.add(sio.manager.eio_sid_from_sid(sid, "/control"))


@sio.on("disconnect", namespace="/results")
def disconnect_results(sid):
    print(f"Client disconnected from /results namespace: session id: {sid}")


@sio.on("disconnect", namespace="/data")
def disconnect_data(sid):
    eio_sid = sio.manager.eio_sid_from_sid(sid, "/data")
    result_subscribers.discard(eio_sid)
    print(f"Client disconnected from /data namespace: session id: {sid}")


@sio.on("disconnect", namespace="/control")
def disconnect_control(sid):
    print(f"Client disconnected from /control namespace: session id: {sid}")
