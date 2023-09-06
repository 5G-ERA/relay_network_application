import binascii
import logging
import sys
import threading
import time
from dataclasses import asdict
from queue import Full, Queue
from typing import Optional, Any, Dict

import rclpy
from rclpy.node import Node
from rosbridge_library.internal import ros_loader
from rosbridge_library.internal.message_conversion import (
    populate_instance,
    extract_values,
)

from era_5g_relay_network_application.data.packets import (
    MessagePacket,
    ServiceRequestPacket,
    ServiceResponsePacket,
    PacketType,
)
from era_5g_relay_network_application.interface_common import sio, app, NETAPP_PORT, result_subscribers
from era_5g_relay_network_application.utils import load_topic_list
from era_5g_relay_network_application.worker import Worker
from era_5g_relay_network_application.worker_image import WorkerImage
from era_5g_relay_network_application.worker_results import WorkerResults

# Set logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("relay interface python")

workers: Dict[str, Worker] = dict()

node: Optional[Node] = None


@sio.on("image", namespace="/data")
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
    recv_timestamp = time.time_ns()
    if "timestamp" in data:
        timestamp = data["timestamp"]
    else:
        logging.debug("Timestamp not set, setting default value")
        timestamp = 0

    eio_sid = sio.manager.eio_sid_from_sid(sid, "/data")

    if "frame" not in data:
        logging.error(f"Data does not contain frame.")
        sio.emit(
            "image_error",
            {"timestamp": timestamp, "error": f"Data does not contain frame."},
            namespace="/data",
            to=sid,
        )
        return

    try:
        metadata = data.get("metadata")
        if metadata is None:
            logging.warning(f"No metadata {data}")
            return
        topic_name = metadata.get("topic_name")
        topic_type = metadata.get("topic_type")
        msg = data.get("frame")
        if topic_name is None or topic_type is None:
            return
        worker_thread = workers.get(topic_name)
        if worker_thread is None:
            q: Queue = Queue(1)
            worker_thread = WorkerImage(q, topic_name, topic_type, node)
            worker_thread.daemon = True
            worker_thread.start()
            workers[topic_name] = worker_thread
        try:
            worker_thread.queue.put((msg, timestamp), block=False)
        except Full:
            pass

    except (ValueError, binascii.Error) as error:
        logging.error(f"Failed to decode frame data: {error}")
        sio.emit(
            "image_error",
            {"timestamp": timestamp, "error": f"Failed to decode frame data: {error}"},
            namespace="/data",
            to=sid,
        )
        return


@sio.on("json", namespace="/data")
def json_callback_websocket(sid: str, data: Dict[str, Any]):
    """
    Allows to receive general json data using the websocket transport

    Args:
        data (dict): NetApp-specific json data

    Raises:
        ConnectionRefusedError: Raised when attempt for connection were made
            without registering first.
    """
    logging.debug(f"client with task id: {sio.manager.eio_sid_from_sid(sid, '/data')} sent data {data}")
    global workers
    packet_type = data.get("packet_type")
    if packet_type == PacketType.MESSAGE:
        msg_packet = MessagePacket(**data)

        worker_thread = workers.get(msg_packet.topic_name)
        if worker_thread is None:
            q: Queue = Queue(1)
            worker_thread = Worker(q, msg_packet.topic_name, msg_packet.topic_type, node)
            worker_thread.daemon = True
            worker_thread.start()
            workers[msg_packet.topic_name] = worker_thread
        try:
            worker_thread.queue.put(msg_packet.data, block=False)
        except Full:
            pass

    elif packet_type == PacketType.SERVICE_REQUEST:
        packet = ServiceRequestPacket(**data)
        proxy = node.create_client(ros_loader.get_service_class(packet.service_type), packet.service_name)
        inst = ros_loader.get_service_request_instance(packet.service_type)
        # Populate the instance with the provided args
        while not proxy.wait_for_service(timeout_sec=1.0):
            node.get_logger().info('Service is not available, waiting again...')
        resp = proxy.call_async(populate_instance(packet.data, inst))
        rclpy.spin_until_future_complete(node, resp)

        d = extract_values(resp)
        message = ServiceResponsePacket(
            packet_type=PacketType.SERVICE_RESPONSE,
            data=d,
            service_name=packet.service_name,
            service_type=packet.service_type,
            id=packet.id,
        )
        sio.emit(
            "message",
            asdict(message),
            namespace="/results",
            to=sio.manager.sid_from_eio_sid(sid, "/results"),
        )


def main(args=None) -> None:
    topics = load_topic_list()

    rclpy.init(args=args)
    global node
    node = rclpy.create_node("relay_netapp")
    node.get_logger().set_level(logging.DEBUG)
    node.get_logger().debug(f"Loaded topics: {topics}")

    executor = rclpy.executors.MultiThreadedExecutor()
    executor.add_node(node)

    for topic_name, _, topic_type in topics:
        _ = WorkerResults(topic_name, topic_type, node, sio, result_subscribers)

    executor_thread = threading.Thread(target=executor.spin, daemon=True)
    executor_thread.start()

    # runs the flask server
    # allow_unsafe_werkzeug needs to be true to run inside the docker
    # TODO: use better webserver
    app.run(port=NETAPP_PORT, host="0.0.0.0")


if __name__ == "__main__":
    main()
