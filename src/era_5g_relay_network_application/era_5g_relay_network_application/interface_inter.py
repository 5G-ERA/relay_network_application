import json
import logging
import os

from typing import Any, Dict, List, FrozenSet

from era_5g_client.client_base import NetAppClientBase

from era_5g_relay_network_application.data.packets import (
    MessagePacket,
    ServiceRequestPacket,
    ServiceResponsePacket,
    PacketType,
)

from era_5g_relay_network_application.interface_common import sio, app, NETAPP_PORT, result_subscribers

relay_clients: FrozenSet[NetAppClientBase] = frozenset()
topics_to_clients: Dict[str, List[NetAppClientBase]] = dict()
service_to_client: Dict[str, NetAppClientBase] = dict()

response_id_to_sid: Dict[str, str] = dict()


def get_clients(packet: MessagePacket):
    if not packet.topic_name:
        logging.warn(f"The message is in wrong format: ")
        return

    clients = topics_to_clients.get(packet.topic_name, [])
    if not clients:
        logging.error(f"Topic {packet.topic_name} not configured for transport!")
    return clients


@sio.on("image", namespace="/data")
def image_callback_websocket(sid, data: Dict):
    """
    Allows to receive jpg-encoded image using the websocket transport

    Args:
        data (dict): An encoded image frame and (optionally) related timestamp in format:
            {'frame': 'bytes', 'timestamp': 'int'}

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


@sio.on("json", namespace="/data")
def json_callback_websocket(sid, data: Dict[str, Any]):
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
        msg_packet = MessagePacket(**data)
        clients = get_clients(msg_packet)
        for client in clients:
            client.send_json_ws(data)
    elif packet_type == PacketType.SERVICE_REQUEST:
        packet = ServiceRequestPacket(**data)
        client = service_to_client.get(packet.service_name)
        client.send_json_ws(data)
        response_id_to_sid[data["id"]] = sio.manager.sid_from_eio_sid(sid, "/results")


def results(data: dict):
    if type(data) is not dict:
        return
    packet_type = data.get("packet_type")
    if packet_type == PacketType.MESSAGE:
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
        client.register(
            f"{relay['relay_address']}", args={"subscribe_results": True}, wait_until_available=True, wait_timeout=60
        )
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
    app.run(port=NETAPP_PORT, host="0.0.0.0")


if __name__ == "__main__":
    main()
