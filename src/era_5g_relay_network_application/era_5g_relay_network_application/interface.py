import os
import threading
import rclpy
import binascii
import logging
from multiprocessing import Queue
from queue import Full
import time
from typing import Any, Dict, Optional, List



from geometry_msgs.msg import TransformStamped
from tf2_msgs.msg import TFMessage

from rosbridge_library.internal.message_conversion import extract_values

from era_5g_tf2json.tf2_web_republisher import TFRepublisher

from era_5g_relay_network_application.data.packets import MessagePacket, PacketType, ServiceRequestPacket
from era_5g_relay_network_application.interface_common import RelayInterfaceCommon

from era_5g_relay_network_application.worker_image import WorkerImage
from era_5g_relay_network_application.worker import Worker
from era_5g_relay_network_application.utils import load_topic_list, load_transform_list
from era_5g_relay_network_application.worker_results import WorkerResults
from era_5g_relay_network_application.worker_results_socketio import WorkerResultsSocketIO
from era_5g_relay_network_application.worker_service_socketio import WorkerServiceSocketIO
from era_5g_relay_network_application.worker_service import WorkerService


# port of the netapp's server
NETAPP_PORT = int(os.getenv("NETAPP_PORT", 5896))


class RelayInterface(RelayInterfaceCommon):
    def __init__(
        self,
        port: int,
        queues: Dict[str, Queue],
        results_queue: Queue,
        services_requests_queue: Queue,
        services_responses_queue: Queue,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(port, *args, **kwargs)
        self.node = None
        self.sio.on("image", self.image_callback_websocket, namespace="/data")
        self.sio.on("json", self.json_callback_websocket, namespace="/data")
        self.queues = queues
        self.worker_results = WorkerResultsSocketIO(results_queue, self.result_subscribers, self.sio)
        self.worker_service = WorkerServiceSocketIO(services_responses_queue, self.sio)
        self.services_requests_queue = services_requests_queue
        self.services_responses_queue = services_responses_queue

    def run(self):
        self.worker_results.daemon = True
        self.worker_results.start()
        self.worker_service.daemon = True
        self.worker_service.start()
        self.run_server()

    def image_callback_websocket(self, sid, data: dict):
        """
        Allows to receive jpg-encoded image using the websocket transport

        Args:
            data (dict): An encoded image frame and (optionally) related timestamp in format:
                {'frame': 'bytes', 'timestamp': 'int'}

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

        eio_sid = self.sio.manager.eio_sid_from_sid(sid, "/data")

        if "frame" not in data:
            logging.error(f"Data does not contain frame.")
            self.sio.emit(
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

            try:
                q: Queue = self.queues.get(topic_name, None)
                if q is None:
                    logging.error(f"Arrived message on topic that is not registered {topic_name}")
                    return
                q.put_nowait((msg, timestamp))
            except Full:
                pass

        except (ValueError, binascii.Error) as error:
            logging.error(f"Failed to decode frame data: {error}")
            self.sio.emit(
                "image_error",
                {"timestamp": timestamp, "error": f"Failed to decode frame data: {error}"},
                namespace="/data",
                to=sid,
            )
            return

    def json_callback_websocket(self, sid: str, data: Dict[str, Any]):
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

            try:
                q: Queue = self.queues.get(msg_packet.topic_name, None)
                
                if q is None:
                    logging.error(f"Arrived message on topic that is not registered {msg_packet.topic_name}")
                    return
                q.put_nowait(msg_packet.data)
            except Full:
                pass
            return

        elif packet_type == PacketType.SERVICE_REQUEST:
            packet = ServiceRequestPacket(**data)
            self.services_requests_queue.put_nowait((self.sio.manager.sid_from_eio_sid(sid, "/results"), packet))
            return


def main(args=None) -> None:
    topics_results = load_topic_list()
    topics_to_publish = load_topic_list("TOPIC_TO_PUB_LIST")
    transforms_to_listen = load_transform_list()

    rclpy.init(args=args)
    node = rclpy.create_node("relay_netapp")
    node.get_logger().set_level(logging.DEBUG)
    node.get_logger().debug(f"Loaded topics for results: {topics_results}")
    node.get_logger().debug(f"Loaded topics for publishing: {topics_to_publish}")
    node.get_logger().debug(f"Loaded transforms for listening: {transforms_to_listen}")

    # can't know if client will want to send some TFs so we have to create worker for it
    topics_to_publish.append(("/tf", None, "tf2_msgs/TFMessage", None))

    workers = dict()
    queues = dict()
    for topic_name, _, topic_type, compression in topics_to_publish:
        q = Queue(1)
        if topic_type == "sensor_msgs/msg/Image":
            w = WorkerImage(q, topic_name, topic_type, node)
        else:
            w = Worker(q, topic_name, topic_type, compression, node)
        w.daemon = True
        w.start()
        workers[topic_name] = w
        queues[topic_name] = q

    executor = rclpy.executors.MultiThreadedExecutor()
    executor.add_node(node)

    results_queue = Queue(8)
    services_requests_queue = Queue(20)
    services_responses_queue = Queue(20)

    worker_service = WorkerService(services_requests_queue, services_responses_queue, node)
    worker_service.daemon = True
    worker_service.start()

    executor_thread = threading.Thread(target=executor.spin, daemon=True)
    executor_thread.start()

    socketio_process = RelayInterface(
        NETAPP_PORT, queues, results_queue, services_requests_queue, services_responses_queue
    )

    socketio_process.start()
    results_workers = dict()

    for topic_name, _, topic_type, compression in topics_results:
        results_workers[topic_name] = WorkerResults(topic_name, topic_type, compression, node, results_queue)

    tf_republisher: Optional[TFRepublisher] = None

    def tf_callback(transforms: List[TransformStamped]) -> None:
        if transforms:
            message = MessagePacket(
                packet_type=PacketType.MESSAGE,
                data=extract_values(TFMessage(transforms=transforms)),
                topic_name="/tf",
                topic_type="tf2_msgs/TFMessage",
            )
            try:
                results_queue.put_nowait(message)
            except Full:
                pass

    if transforms_to_listen:
        tf_republisher = TFRepublisher(node, tf_callback, rate=100)
        for tr in transforms_to_listen:
            node.get_logger().info(f"Subscribing for: {tr}")
            tf_republisher.subscribe_transform(*tr)

    socketio_process.join()

    rclpy.shutdown()  # Shutdown rclpy when finished"""


if __name__ == "__main__":
    main()
