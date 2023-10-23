import importlib
import os
import logging
import signal
from types import FrameType
from typing import Any, Dict, Optional, List, Union
import uuid
from era_5g_client.exceptions import FailedToConnect
from era_5g_client.client_base import NetAppClientBase
from era_5g_client.dataclasses import MiddlewareInfo
from era_5g_client.client import NetAppClient
from rosbridge_library.internal.message_conversion import (
    extract_values,
    populate_instance,
)
from rosbridge_library.internal import ros_loader
from era_5g_relay_network_application.utils import (
    load_topic_list,
    load_services_list,
    build_message,
    Compressions,
)
import rospy
from cv_bridge import CvBridge
from rospy import Publisher, ROSInterruptException
from functools import partial
from sensor_msgs.msg import Image
from era_5g_relay_network_application.data.packets import (
    MessagePacket,
    ServiceRequestPacket,
    ServiceResponsePacket,
    PacketType,
)
from dataclasses import asdict

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
# defines if topics from cloud should be send to this client
SUBSCRIBE_RESULTS = os.getenv("SUBSCRIBE_RESULTS", "true").lower() in ("true", "1", "t")

bridge = CvBridge()
client: Optional[NetAppClientBase] = None

results_publishers: Dict[str, Publisher] = dict()

services_results: Dict[str, Dict] = dict()

logger: Optional[logging.Logger] = None


def callback_image(data: Image, topic_name=None, topic_type=None):
    assert logger
    assert client

    if topic_name is None or topic_type is None:
        logger.error("You need to specify topic name and type!")
        return
    cv_image = bridge.imgmsg_to_cv2(data, desired_encoding="bgr8")
    if cv_image is not None:
        client.send_image_ws(
            cv_image,
            data.header.stamp.to_nsec(),
            metadata={"topic_name": topic_name, "topic_type": topic_type},  # TODO fix type annotation in send_image_ws?
        )
    else:
        logger.warning("Empty image received!")

def callback_others(data: Any, topic_name=None, topic_type=None):
    assert logger
    assert client

    if topic_name is None or topic_type is None:
        logger.error("You need to specify topic name and type!")
        return

    d = extract_values(data)
    message = build_message(topic_name, topic_type, d)
    message = MessagePacket(
        packet_type=PacketType.MESSAGE,
        data=d,
        topic_name=topic_name,
        topic_type=topic_type,
        compression=Compressions.NONE,
    )
    client.send_json_ws(asdict(message))


def results(data: Union[Dict, str]) -> None:
    assert logger

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
            pub = rospy.Publisher(msg_packet.topic_name, type(inst), queue_size=10)
            results_publishers[msg_packet.topic_name] = pub
        pub.publish(populate_instance(msg_packet.data, inst))
    elif packet_type == PacketType.SERVICE_RESPONSE:
        packet = ServiceResponsePacket(**data)
        inst = ros_loader.get_service_response_instance(packet.service_type)
        services_results[packet.id] = populate_instance(packet.data, inst)
    else:
        logger.warn(f"Unknown packet type {packet_type}")


def callback_service(req, service_name: str, service_type: str) -> Dict:
    assert logger
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
        rospy.sleep(0.01)

    return services_results.pop(id)


def main() -> None:
    rospy.init_node("relay_client", anonymous=True)
    global logger
    importlib.reload(logging)  # HACK to use python logging instead of ros logging
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    topics = load_topic_list()
    services = load_services_list()


    global client
    subs: List[rospy.Subscriber] = []

    def signal_handler(sig: int, frame: Optional[FrameType]) -> None:
        assert logger

        logger.info(f"Terminating ({signal.Signals(sig).name})...")
        raise ROSInterruptException()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    if USE_MIDDLEWARE:
        logger.info("The middleware will be used to deploy the relay network application with following settings:")
        rospy.loginfo(f"{MIDDLEWARE_ADDRESS=}")
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
                args={"subscribe_results": SUBSCRIBE_RESULTS},
            )
        else:
            client = NetAppClientBase(results, logging_level=logging.DEBUG, socketio_debug=False)
            client.register(f"{NETAPP_ADDRESS}", args={"subscribe_results": SUBSCRIBE_RESULTS})

        for topic_name, topic_name_remapped, topic_type in topics:
            if topic_name_remapped is None:
                topic_name_remapped = topic_name

            topic_type_class = ros_loader.get_message_class(topic_type)
            if topic_type == "sensor_msgs/Image":
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
            subs.append(rospy.Subscriber(topic_name, topic_type_class, callback))
        for service_name, service_type in services:
            service_type_class = ros_loader.get_service_class(service_type)
            _ = rospy.Service(
                service_name,
                service_type_class,
                partial(
                    callback_service,
                    service_name=service_name,
                    service_type=service_type,
                ),
            )

        while not rospy.is_shutdown():
            rospy.sleep(1)

    except FailedToConnect as ex:
        logger.error(f"Failed to connect: {ex}")
    except (KeyboardInterrupt, ROSInterruptException):
        if client is not None:
            client.disconnect()
        exit()


if __name__ == "__main__":
    main()
