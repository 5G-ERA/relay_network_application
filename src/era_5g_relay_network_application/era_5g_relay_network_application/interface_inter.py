import json
import logging
import os

import socketio

from typing import Dict, List, Set, FrozenSet
from flask import Flask

from era_5g_interface.dataclasses.control_command import ControlCommand, ControlCmdType
from era_5g_client.client_base import NetAppClientBase

from era_5g_relay_network_application.dataclasses.packets import MessagePacket, ServiceRequestPacket, ServiceResponsePacket, PacketType

from threading import Lock

# port of the netapp's server
NETAPP_PORT = os.getenv("NETAPP_PORT", 5896)
    
# the max_http_buffer_size parameter defines the max size of the message to be passed
sio = socketio.Server(async_mode='threading', max_http_buffer_size=5*(1024**2))
app = Flask(__name__)
app.wsgi_app = socketio.WSGIApp(sio, app.wsgi_app)

result_subscribers: Set[str] = set()

relay_clients: FrozenSet[NetAppClientBase] = frozenset()
topics_to_clients: Dict[str, List[NetAppClientBase]] = dict()
service_to_client: Dict[str, NetAppClientBase] = dict()

response_id_to_sid: Dict[str, str] = dict()

subscribers_lock = Lock()

class ArgFormatError(Exception):
    pass

def get_sid_of_namespace(eio_sid, namespace):
    return sio.manager.sid_from_eio_sid(eio_sid, namespace)

def get_results_sid(eio_sid):
    return sio.manager.sid_from_eio_sid(eio_sid, "/results")

@sio.on('connect', namespace='/data')
def connect_data(sid, environ):
    """_summary_
    Creates a websocket connection to the client for passing the data.

    Raises:
        ConnectionRefusedError: Raised when attempt for connection were made
            without registering first.
    """
    print(f"Connected data. Session id: {sio.manager.eio_sid_from_sid(sid, '/data')}, namespace_id: {sid}")
    sio.send("you are connected", namespace='/data', to=sid)

@sio.on('connect', namespace='/control')
def connect_control(sid, environ):
    """_summary_
    Creates a websocket connection to the client for passing control commands.

    Raises:
        ConnectionRefusedError: Raised when attempt for connection were made
            without registering first.
    """

    print(f"Connected control. Session id: {sio.manager.eio_sid_from_sid(sid, '/data')}, namespace_id: {sid}")
    sio.send("you are connected", namespace='/control', to=sid)

@sio.on('connect', namespace='/results')
def connect_results(sid, environ):
    """
    Creates a websocket connection to the client for passing the results.

    Raises:
        ConnectionRefusedError: Raised when attempt for connection were made
            without registering first.
    """

    print(f"Connected results. Session id: {sio.manager.eio_sid_from_sid(sid, '/data')}, namespace_id: {sid}")
    sio.send("You are connected", namespace='/results', to=sid)

def get_clients(packet: MessagePacket):
    if packet.topic_name is None:
        logging.warn(f"The message is in wrong format: ")
        return
    
    clients = topics_to_clients.get(packet.topic_name, [])
    if not clients:
        logging.error(f"Topic {packet.topic_name} not configured for transport!")
    return clients
    

@sio.on('image', namespace='/data')
def image_callback_websocket(sid, data: dict):
    """
    Allows to receive jpg-encoded image using the websocket transport

    Args:
        data (dict): A base64 encoded image frame and (optionally) related timestamp in format:
            {'frame': 'base64data', 'timestamp': 'int'}

    Raises:
        ConnectionRefusedError: Raised when attempt for connection were made
            without registering first or frame was not passed in correct format.
    """
    metadata = data.get("metadata")
    if metadata is None:
        logging.warning(f"No metadata")
        return
    topic_name = metadata.get("topic_name")
    if topic_name is None:
        logging.warn(f"The message is in wrong format")
        return
    
    clients = topics_to_clients.get(topic_name, [])
    for client in clients:
        client.send_image_ws_raw(data)
    

@sio.on('json', namespace='/data')
def json_callback_websocket(sid, data: Dict):
    """
    Allows to receive general json data using the websocket transport

    Args:
        data (dict): NetApp-specific json data

    Raises:
        ConnectionRefusedError: Raised when attempt for connection were made
            without registering first.
    """
    packet_type = data.get("packet_type")
    if packet_type == PacketType.MESSAGE:
        packet = MessagePacket(**data)
        clients = get_clients(packet)    
        for client in clients:
            client.send_json_ws(data)
    elif packet_type == PacketType.SERVICE_REQUEST:
        packet = ServiceRequestPacket(**data)
        client = service_to_client.get(packet.service_name)
        client.send_json_ws(data)
        response_id_to_sid[data.get("id")] = sio.manager.sid_from_eio_sid(sid, "/results")

@sio.on('command', namespace='/control')
def json_callback_websocket(sid, data: Dict):
    command = ControlCommand(**data)
    # check if the client wants to receive results
    if command and command.cmd_type == ControlCmdType.SET_STATE:
        args = command.data
        if args:
            sr = args.get("subscribe_results")
            if sr:
                with subscribers_lock:
                    result_subscribers.add(sio.manager.eio_sid_from_sid(sid, "/control"))
        

@sio.on('disconnect', namespace='/results')
def disconnect_results(sid):
    print(f"Client disconnected from /results namespace: session id: {sid}")


@sio.on('disconnect', namespace='/data')
def disconnect_data(sid):
    eio_sid = sio.manager.eio_sid_from_sid(sid, "/data")
    with subscribers_lock:
        result_subscribers.discard(eio_sid)
    print(f"Client disconnected from /data namespace: session id: {sid}")

@sio.on('disconnect', namespace='/control')
def disconnect_control(sid):
    print(f"Client disconnected from /control namespace: session id: {sid}")


def results(data: dict):
    if type(data) is not dict:
        return
    packet_type = data.get("packet_type")
    if packet_type == PacketType.MESSAGE:
        with subscribers_lock:
            for s in result_subscribers:
                sio.emit("message", data, namespace="/results", to=sio.manager.sid_from_eio_sid(s, "/results"))
    elif packet_type == PacketType.SERVICE_RESPONSE:
        packet = ServiceResponsePacket(**data)
        sid = response_id_to_sid.pop(packet.id)
        sio.emit("message", data, namespace="/results", to=sid)

def main():
    relays_list = os.getenv("RELAYS_LIST")
    if relays_list is None:
        print("You need to specify list of relays using the RELAYS_LIST env variable")
        return None
    relays_data = json.loads(relays_list)
    if len(relays_data) == 0:
        print("You need to specify list of relays using the RELAYS_LIST env variable")
        return None
    global relay_clients
    clients = set()
    for relay in relays_data:
        client = NetAppClientBase(results)
        client.register(f"{relay['relay_address']}", args={"subscribe_results": True}, wait_until_available=True, wait_timeout=60)
        clients.add(client)
        try:
            if "topics" in relay:
                for topic in relay["topics"]:
                    if topic in topics_to_clients:
                        topics_to_clients[topic].append(client)
                    else:
                        topics_to_clients[topic] = [client]
        except TypeError:
            print("The topics field in RELAYS_LIST needs to be a list")
        services = relay.get("services", None)
        try:
            if services:
                for service in relay["services"]:
                    if service in service_to_client:
                        logging.error("There can only be one relay per each service!")
                        for client in clients:
                            client.disconnect()
                        return
                    service_to_client[service] = client
        except TypeError:
            print("The services field in RELAYS_LIST needs to be a list")
    relay_clients = frozenset(clients)
    logging.getLogger().setLevel(logging.DEBUG)

    # runs the flask server
    # allow_unsafe_werkzeug needs to be true to run inside the docker
    # TODO: use better webserver
    app.run(port=NETAPP_PORT, host='0.0.0.0')


if __name__ == '__main__':
    main()
