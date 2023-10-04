import logging
import os
import signal
import sys
import time
import uuid
from dataclasses import asdict
from functools import partial
from types import FrameType
from typing import Any, Dict, Optional, List, Union

import rclpy
from cv_bridge import CvBridge
from rclpy.node import Node
from rclpy.publisher import Publisher
from rclpy.subscription import Subscription
from rclpy.time import Time
from rosbridge_library.internal import ros_loader
from rosbridge_library.internal.message_conversion import (
    extract_values,
    populate_instance,
)
from sensor_msgs.msg import Image
from geometry_msgs.msg import TransformStamped
from tf2_msgs.msg import TFMessage

from era_5g_tf2json.tf2_web_republisher import TFRepublisher

from era_5g_client.client import NetAppClient
from era_5g_client.client_base import NetAppClientBase
from era_5g_client.dataclasses import MiddlewareInfo
from era_5g_client.exceptions import FailedToConnect
from era_5g_relay_network_application.data.packets import (
    MessagePacket,
    ServiceRequestPacket,
    ServiceResponsePacket,
    PacketType,
)
from era_5g_relay_network_application.utils import (
    load_topic_list,
    load_services_list,
    load_transform_list,
)

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("relay client python")

# defines if the middleware is used for deployment of relay netapp
# if true, the address, user, password, task_id and robot_id needs to be specified
# if false, the netapp address and port needs to be specified
USE_MIDDLEWARE = os.getenv("USE_MIDDLEWARE", "false").lower() in ("true", "1", "t")
# ip address or hostname of the computer, where the netapp is deployed
NETAPP_ADDRESS = os.getenv("NETAPP_ADDRESS", "http://localhost:5896")
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

bridge = CvBridge()
client: Optional[NetAppClientBase] = None

results_publishers: Dict[str, Publisher] = dict()

services_results: Dict[str, Dict] = dict()

node: Optional[Node] = None


def callback_image(data: Image, topic_name=None, topic_type=None):
    assert client

    if topic_name is None or topic_type is None:
        logger.error("You need to specify topic name and type!")
        return
    cv_image: Image = bridge.imgmsg_to_cv2(data, desired_encoding="bgr8")
    if cv_image is not None:
        client.send_image_ws(
            cv_image,
            Time.from_msg(data.header.stamp).nanoseconds,
            metadata={"topic_name": topic_name, "topic_type": topic_type},  # TODO fix type annotation in send_image_ws?
        )
    else:
        logger.warning("Empty image received!")


def callback_others(data: Any, topic_name=None, topic_type=None):
    assert client

    if topic_name is None or topic_type is None:
        logger.error("You need to specify topic name and type!")
        return

    message = MessagePacket(
        packet_type=PacketType.MESSAGE,
        data=extract_values(data),
        topic_name=topic_name,
        topic_type=topic_type,
    )
    client.send_json_ws(asdict(message))


def results(data: Union[Dict, str]) -> None:
    # TODO: not sure why does client get here status messages like "you are connected" - is it intentional?
    if not isinstance(data, dict):
        return

    packet_type = data.get("packet_type")

    if packet_type == PacketType.MESSAGE:
        msg_packet = MessagePacket(**data)
        logger.debug(f"result arrived: {msg_packet.topic_name=}, {msg_packet.topic_type=}")
        pub = results_publishers.get(msg_packet.topic_name)
        inst = ros_loader.get_message_instance(msg_packet.topic_type)

        # to investigate turnaround time
        # import json
        # d = json.loads(data["data"]["data"])
        # print((rospy.Time.now().to_nsec()-d["timestamp"])/10**9)

        if pub is None:
            pub = node.create_publisher(type(inst), msg_packet.topic_name, 10)
            results_publishers[msg_packet.topic_name] = pub
        pub.publish(populate_instance(msg_packet.data, inst))
    elif packet_type == PacketType.SERVICE_RESPONSE:
        packet = ServiceResponsePacket(**data)
        inst = ros_loader.get_service_response_instance(packet.service_type)
        services_results[packet.id] = populate_instance(packet.data, inst)
    else:
        logger.warning(f"Unknown packet type {packet_type}")


def callback_service(req, resp, service_name: str, service_type: str) -> Dict:
    assert client

    assert service_name
    assert service_type
    d = extract_values(req)
    message = ServiceRequestPacket(
        packet_type=PacketType.SERVICE_REQUEST,
        data=d,
        service_name=service_name,
        service_type=service_type,
        id=uuid.uuid4().hex,
    )
    client.send_json_ws(asdict(message))

    resp = wait_for_service_response(message.id)
    return resp


# TODO: timeout?
def wait_for_service_response(id: str) -> Dict[str, Any]:
    while id not in services_results:
        time.sleep(0.01)

    return services_results.pop(id)


def tf_callback(transforms: List[TransformStamped]) -> None:
    assert client

    if transforms:
        message = MessagePacket(
            packet_type=PacketType.MESSAGE,
            data=extract_values(TFMessage(transforms=transforms)),
            topic_name="/tf",
            topic_type="tf2_msgs/TFMessage",
        )
        client.send_json_ws(asdict(message))


def main(args=None) -> None:
    rclpy.init(args=args)
    global node
    node = rclpy.create_node("relay_client")
    node.get_logger().set_level(logging.DEBUG)

    topics = load_topic_list()
    services = load_services_list()
    transforms = load_transform_list()

    node.get_logger().debug(f"Loaded topics: {topics}")

    if not (topics or services):
        print("Specify at least one topic or service.")
        return

    global client
    subs: List[Subscription] = []

    def signal_handler(sig: int, frame: Optional[FrameType]) -> None:
        assert logger

        logger.info(f"Terminating ({signal.Signals(sig).name})...")
        raise KeyboardInterrupt()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

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

    try:
        if USE_MIDDLEWARE:
            client = NetAppClient(results, logging_level=logging.DEBUG, socketio_debug=False)
            client.connect_to_middleware(MiddlewareInfo(MIDDLEWARE_ADDRESS, MIDDLEWARE_USER, MIDDLEWARE_PASSWORD))
            client.run_task(
                MIDDLEWARE_TASK_ID,
                robot_id=MIDDLEWARE_ROBOT_ID,
                resource_lock=True,
                args={"subscribe_results": True},
            )
        else:
            client = NetAppClientBase(results, logging_level=logging.DEBUG, socketio_debug=False)
            client.register(f"{NETAPP_ADDRESS}", args={"subscribe_results": True})

        for topic_name, topic_name_remapped, topic_type in topics:
            if topic_name_remapped is None:
                topic_name_remapped = topic_name

            topic_type_class = ros_loader.get_message_class(topic_type)
            logger.info(f"Topic class is {topic_type_class}")
            if topic_type_class == Image:
                callback = partial(
                    callback_image,
                    topic_type=topic_type,
                    topic_name=topic_name_remapped,
                )
            else:
                callback = partial(
                    callback_others,
                    topic_type=topic_type,
                    topic_name=topic_name_remapped,
                )

            subs.append(node.create_subscription(topic_type_class, topic_name, callback, 10))
        for service_name, service_type in services:
            service_type_class = ros_loader.get_service_class(service_type)
            node.create_service(
                service_type_class,
                service_name,
                partial(
                    callback_service,
                    service_name=service_name,
                    service_type=service_type,
                ),
            )

        tf_republisher: Optional[TFRepublisher] = None

        if transforms:
            tf_republisher = TFRepublisher(node, tf_callback)
            for tr in transforms:
                logger.info(f"Subscribing for: {tr}")
                tf_republisher.subscribe_transform(*tr)

        while rclpy.ok():
            rclpy.spin_once(node, timeout_sec=1.0)

    except FailedToConnect as ex:
        logger.error(f"Failed to connect: {ex}")
    except KeyboardInterrupt:
        if client is not None:
            client.disconnect()
        pass
    except BaseException:
        print("Exception:", file=sys.stderr)
        raise
    finally:
        rclpy.shutdown()


if __name__ == "__main__":
    main()
