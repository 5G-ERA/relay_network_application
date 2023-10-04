from ast import Set
from collections.abc import Callable, Iterable, Mapping
from multiprocessing import Lock, Process
from collections.abc import Iterator
from typing import Any, Dict
from engineio.payload import Payload
from era_5g_interface.dataclasses.control_command import ControlCmdType
from era_5g_interface.dataclasses.control_command import ControlCommand
from flask import Flask
import socketio

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

class ArgFormatError(Exception):
    pass




class RelayInterfaceCommon(Process):
    def __init__(self, port: int, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # to get rid of ValueError: Too many packets in payload (see https://github.com/miguelgrinberg/python-engineio/issues/142)
        Payload.max_decode_packets = 50

        # the max_http_buffer_size parameter defines the max size of the message to be passed
        self.sio = socketio.Server(async_mode="threading", max_http_buffer_size=5 * (1024 ** 2))
        self.app = Flask(__name__)
        self.app.wsgi_app = socketio.WSGIApp(self.sio, self.app.wsgi_app)  # type: ignore
        self.sio.on("connect", self.connect_data, namespace="/data")
        self.sio.on("connect", self.connect_control, namespace="/control")
        self.sio.on("connect", self.connect_results, namespace="/results")


        self.sio.on("command", self.command_callback_websocket, namespace="/control")
        
        self.sio.on("disconnect", self.disconnect_results, namespace="/results")
        self.sio.on("disconnect", self.disconnect_data, namespace="/data")
        self.sio.on("disconnect", self.disconnect_control, namespace="/control")
        self.port = port
        
        self.result_subscribers: Set[str] = LockedSet()


    def run_server(self):
        self.app.run(port=self.port, host="0.0.0.0")


    def get_sid_of_namespace(self, eio_sid, namespace):
        return self.sio.manager.sid_from_eio_sid(eio_sid, namespace)


    def get_results_sid(self, eio_sid):
        return self.sio.manager.sid_from_eio_sid(eio_sid, "/results")

    def connect_data(self, sid, environ):
        """_summary_
        Creates a websocket connection to the client for passing the data.

        Raises:
            ConnectionRefusedError: Raised when attempt for connection were made
                without registering first.
        """
        print(f"Connected data. Session id: {self.sio.manager.eio_sid_from_sid(sid, '/data')}, namespace_id: {sid}")
        self.sio.send("you are connected", namespace="/data", to=sid)


    def connect_control(self, sid, environ):
        """_summary_
        Creates a websocket connection to the client for passing control commands.

        Raises:
            ConnectionRefusedError: Raised when attempt for connection were made
                without registering first.
        """

        print(f"Connected control. Session id: {self.sio.manager.eio_sid_from_sid(sid, '/control')}, namespace_id: {sid}")
        self.sio.send("you are connected", namespace="/control", to=sid)


    def connect_results(self, sid, environ):
        """
        Creates a websocket connection to the client for passing the results.

        Raises:
            ConnectionRefusedError: Raised when attempt for connection were made
                without registering first.
        """

        print(f"Connected results. Session id: {self.sio.manager.eio_sid_from_sid(sid, '/results')}, namespace_id: {sid}")
        self.sio.send("You are connected", namespace="/results", to=sid)


    def command_callback_websocket(self, sid, data: Dict):
        eio_sid = self.sio.manager.eio_sid_from_sid(sid, '/control')
        try:
            command = ControlCommand(**data)
        except TypeError as e:
            print(f"Could not parse Control Command. {str(e)}")
            self.sio.emit(
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
                    
                    self.result_subscribers.add(self.sio.manager.eio_sid_from_sid(sid, "/control"))
                    print(self.result_subscribers)


    def disconnect_results(self, sid):
        print(f"Client disconnected from /results namespace: session id: {sid}")


    def disconnect_data(self, sid):
        eio_sid = self.sio.manager.eio_sid_from_sid(sid, "/data")
        self.result_subscribers.discard(eio_sid)
        print(f"Client disconnected from /data namespace: session id: {sid}")


    def disconnect_control(self, sid):
        print(f"Client disconnected from /control namespace: session id: {sid}")