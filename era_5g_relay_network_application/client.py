import logging
import os
import sys
from functools import partial
from queue import Full, Queue
from typing import Any, Dict, List, Optional, Tuple

import rclpy  # pants: no-infer-dep
from cv_bridge import CvBridge  # pants: no-infer-dep
from rclpy.executors import MultiThreadedExecutor  # pants: no-infer-dep
from rclpy.node import Node  # pants: no-infer-dep
from rclpy.qos import QoSHistoryPolicy, QoSProfile, QoSReliabilityPolicy  # pants: no-infer-dep
from rosbridge_library.internal import ros_loader  # pants: no-infer-dep

from era_5g_client.client import NetAppClient
from era_5g_client.client_base import NetAppClientBase
from era_5g_client.dataclasses import MiddlewareInfo
from era_5g_client.exceptions import FailedToConnect
from era_5g_interface.channels import CallbackInfoClient, ChannelType
from era_5g_relay_network_application import SendFunctionProtocol
from era_5g_relay_network_application.utils import (
    IMAGE_CHANNEL_TYPES,
    Compressions,
    get_channel_type,
    load_services_list,
    load_topic_list,
    load_transform_list,
)
from era_5g_relay_network_application.worker_image_publisher import WorkerImagePublisher
from era_5g_relay_network_application.worker_image_subscriber import WorkerImageSubscriber
from era_5g_relay_network_application.worker_publisher import WorkerPublisher
from era_5g_relay_network_application.worker_service_server import WorkerServiceServer
from era_5g_relay_network_application.worker_socketio import WorkerSocketIO
from era_5g_relay_network_application.worker_subscriber import WorkerSubscriber
from era_5g_relay_network_application.worker_tf import WorkerTF

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("relay client python")

# defines if the middleware is used for deployment of relay netapp
# if true, the address, user, password, task_id and robot_id needs to be specified
# if false, the netapp address and port needs to be specified
USE_MIDDLEWARE = os.getenv("USE_MIDDLEWARE", "false").lower() in ("true", "1", "t")
# ip address or hostname of the computer, where the netapp is deployed
NETAPP_ADDRESS = os.getenv("NETAPP_ADDRESS", "http://localhost:5896")

# parameters for register method
WAIT_UNTIL_AVAILABLE = os.getenv("WAIT_UNTIL_AVAILABLE", "false").lower() in ("true", "1")
WAIT_TIMEOUT = int(os.getenv("WAIT_UNTIL_AVAILABLE_TO", -1))

# ip address or hostname of the middleware server
MIDDLEWARE_ADDRESS = os.getenv("MIDDLEWARE_ADDRESS", "127.0.0.1")
# middleware user ID
MIDDLEWARE_USER = os.getenv("MIDDLEWARE_USER", "00000000-0000-0000-0000-000000000000")
# middleware password
MIDDLEWARE_PASSWORD = os.getenv("MIDDLEWARE_PASSWORD", "password")
# middleware NetApp id (task id)
MIDDLEWARE_TASK_ID = os.getenv("MIDDLEWARE_TASK_ID", "00000000-0000-0000-0000-000000000000")
# middleware robot id (robot id)
MIDDLEWARE_ROBOT_ID = os.getenv("MIDDLEWARE_ROBOT_ID", "00000000-0000-0000-0000-000000000000")

QUEUE_LENGTH_TOPICS = int(os.getenv("QUEUE_LENGTH_TOPICS", 1))
QUEUE_LENGTH_SERVICES = int(os.getenv("QUEUE_LENGTH_SERVICES", 1))
QUEUE_LENGTH_TF = int(os.getenv("QUEUE_LENGTH_TF", 1))

bridge = CvBridge()
client: Optional[NetAppClientBase] = None

topics_workers: Dict[str, WorkerPublisher] = dict()
services_workers: Dict[str, WorkerServiceServer] = dict()
socketio_workers: List[WorkerSocketIO] = list()


node: Optional[Node] = None

BEST_EFFORT = QoSProfile(
    reliability=QoSReliabilityPolicy.RMW_QOS_POLICY_RELIABILITY_BEST_EFFORT,
    history=QoSHistoryPolicy.RMW_QOS_POLICY_HISTORY_KEEP_LAST,
    depth=1,
)


def json_callback(data: Dict, queue: Queue):
    """Executed when new JSON data is received from the relay server.

    Args:
        data (Dict[str, Any]): JSON data received from the relay server
        queue (Queue):  The queue to pass the data to the publisher
    """

    try:
        queue.put_nowait(data)
    except Full:
        return


def image_callback(data: Dict[str, Any], queue: Queue):
    """Executed when new image data is received from the relay server.

    Args:
        data (Dict[str, Any]): The image data received from the relay server in the format {"frame": <image_data>, "timestamp": <timestamp>}
        queue (Queue): The queue to pass the data to the publisher
    """

    try:
        queue.put_nowait((data["frame"], data["timestamp"]))
    except Full:
        return


def service_callback(data: Dict[str, Any], response_queue: Queue):
    """Executed when new service response is received from the relay server.

    Args:
        data (Dict[str, Any]): Service response received from the relay server
        response_queue (Queue): The queue to pass the data to the service server
    """
    try:
        response_queue.put_nowait(data)
    except Full:
        return


def send_image(data: Tuple, event: str, client: NetAppClientBase, channel_type: ChannelType, can_be_dropped=False):
    """Sends image data to the relay server.

    Args:
        data (Tuple): The image data in the format (timestamp, image_data)
        event (str): The name of the event to send the data to
        client (NetAppClientBase): The client to send the data with
        channel_type (ChannelType): The type of the channel to send the data to (JPEG or H264)
        can_be_dropped (bool, optional): Indicates if the frame can be dropped due to the back-pressure. Defaults to False.
    """
    client.send_image(data[1], event, channel_type, data[0], can_be_dropped=can_be_dropped)


def main(args=None) -> None:
    rclpy.init(args=args)
    global node
    node = rclpy.create_node("relay_client")
    node.get_logger().set_level(logging.DEBUG)
    executor = MultiThreadedExecutor()
    executor.add_node(node)

    topics_outgoing_list = load_topic_list()  # The list of topics to send to the relay server
    topics_incoming_list = load_topic_list("TOPIC_TO_PUB_LIST")  # The list of topics to receive from the relay server

    # can't know if client will want to send some TFs so we have to create worker for it
    topics_incoming_list.append(("/tf", None, "tf2_msgs/msg/TFMessage", Compressions.NONE))
    services = load_services_list()  # The list of services to call on the relay server
    transforms_to_listen = load_transform_list()  # The list of transforms to listen to and send to the relay server

    node.get_logger().debug(f"Loaded outgoing topics: {topics_outgoing_list}")
    node.get_logger().debug(f"Loaded incoming topics: {topics_incoming_list}")

    node.get_logger().debug(f"Loaded outgoing services: {services}")

    global client

    if USE_MIDDLEWARE:
        logger.info("The middleware will be used to deploy the relay network application with following settings:")
        logger.info(f"{MIDDLEWARE_ADDRESS=}")
        logger.info(f"{MIDDLEWARE_USER=}")
        logger.info(f"{MIDDLEWARE_PASSWORD=}")
        logger.info(f"{MIDDLEWARE_TASK_ID=}")
        logger.info(f"{MIDDLEWARE_ROBOT_ID=}")
    else:
        logger.info(
            "The relay network application should be already deployed. I will try to connect with following settings:"
        )
        logger.info(f"{NETAPP_ADDRESS=}")

    callbacks_info = dict()
    # Create publishers and collect callbacks info for all topics to be received from the relay server
    for topic_name, _, topic_type, compression in topics_incoming_list:
        queue: Queue = Queue(QUEUE_LENGTH_TOPICS)
        channel_type = get_channel_type(compression, topic_type)
        worker: WorkerPublisher
        if channel_type in IMAGE_CHANNEL_TYPES:
            callback = partial(
                image_callback,
                queue=queue,
            )
            worker = WorkerImagePublisher(queue, topic_name, topic_type, compression, node)
        else:
            callback = partial(
                json_callback,
                queue=queue,
            )
            worker = WorkerPublisher(queue, topic_name, topic_type, compression, node)

        callbacks_info[f"topic/{topic_name}"] = CallbackInfoClient(channel_type, callback)

        topics_workers[topic_name] = worker
        worker.daemon = True
        worker.start()

    # Prepare service servers for all services to be called on the relay server
    for service_name, service_type in services:
        request_q: Queue = Queue(QUEUE_LENGTH_SERVICES)
        response_q: Queue = Queue(QUEUE_LENGTH_SERVICES)
        services_workers[service_name] = WorkerServiceServer(service_name, service_type, request_q, response_q, node)

        # The service is bidirectional, so we need to create callback for the response from the relay server
        callback = partial(
            service_callback,
            response_queue=response_q,
        )

        callbacks_info[f"service_response/{service_name}"] = CallbackInfoClient(ChannelType.JSON, callback)

    try:
        if USE_MIDDLEWARE:
            client = NetAppClient(callbacks_info, logging_level=logging.DEBUG, socketio_debug=False)
            client.connect_to_middleware(MiddlewareInfo(MIDDLEWARE_ADDRESS, MIDDLEWARE_USER, MIDDLEWARE_PASSWORD))
            client.run_task(
                MIDDLEWARE_TASK_ID,
                robot_id=MIDDLEWARE_ROBOT_ID,
                resource_lock=True,
                args={"subscribe_results": True},
            )
        else:
            client = NetAppClientBase(callbacks_info)
            client.register(f"{NETAPP_ADDRESS}", {"subscribe_results": True}, WAIT_UNTIL_AVAILABLE, WAIT_TIMEOUT)

        # create socketio workers for all topics and services to be sent to the relay server
        for topic_name, _, topic_type, compression in topics_outgoing_list:
            topic_type_class = ros_loader.get_message_instance(topic_type)

            logger.info(f"Topic class is {topic_type_class}")

            q: Queue = Queue(QUEUE_LENGTH_TOPICS)
            channel_type = get_channel_type(compression, topic_type)

            if channel_type in IMAGE_CHANNEL_TYPES:
                WorkerImageSubscriber(topic_name, topic_type, node, q)
                send_function: SendFunctionProtocol = partial(
                    send_image,
                    event=f"topic/{topic_name}",
                    client=client,
                    channel_type=channel_type,
                    can_be_dropped=True,
                )
            else:
                WorkerSubscriber(topic_name, topic_type, compression, node, q)
                send_function: SendFunctionProtocol = partial(  # type: ignore  # deals with "name already defined"
                    client.send_data, event=f"topic/{topic_name}", channel_type=channel_type, can_be_dropped=True
                )

            worker_socketio = WorkerSocketIO(q, send_function)
            worker_socketio.daemon = True
            worker_socketio.start()

            socketio_workers.append(worker_socketio)

        for service_name, service_worker in services_workers.items():
            channel_name = f"service_request/{service_name}"
            worker_socketio = WorkerSocketIO(
                service_worker.request_queue, partial(client.send_data, event=channel_name, can_be_dropped=False)
            )
            worker_socketio.daemon = True
            worker_socketio.start()
            socketio_workers.append(worker_socketio)

        # create socketio worker for all transforms to be sent to the relay server
        if transforms_to_listen:
            tf_queue: Queue = Queue(QUEUE_LENGTH_TF)
            WorkerTF(transforms_to_listen, tf_queue, node)
            worker_socketio = WorkerSocketIO(tf_queue, partial(client.send_data, event="topic//tf"))
            worker_socketio.daemon = True
            worker_socketio.start()
            socketio_workers.append(worker_socketio)

        executor.spin()
    except FailedToConnect as ex:
        logger.error(f"Failed to connect: {ex}")
    except KeyboardInterrupt:
        print("KeyboardInterrupt", file=sys.stderr)
        if client is not None:
            client.disconnect()

    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
