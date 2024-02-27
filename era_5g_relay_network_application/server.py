import logging
import os
import sys
import threading
from functools import partial
from multiprocessing import Queue
from queue import Full
from typing import Any, Dict, Optional, Tuple, Union

import rclpy  # pants: no-infer-dep
from rclpy.parameter import Parameter
from rclpy.qos import QoSProfile  # pants: no-infer-dep

from era_5g_interface.channels import CallbackInfoServer, Channels, ChannelType
from era_5g_interface.dataclasses.control_command import ControlCmdType, ControlCommand
from era_5g_interface.utils.locked_set import LockedSet
from era_5g_relay_network_application.utils import (
    IMAGE_CHANNEL_TYPES,
    ActionServiceVariant,
    ActionSubscribers,
    ActionTopicVariant,
    Compressions,
    EntityConfig,
    get_channel_type,
    load_entities_list,
    load_transform_list,
)
from era_5g_relay_network_application.worker_image_publisher import WorkerImagePublisher
from era_5g_relay_network_application.worker_image_subscriber import WorkerImageSubscriber
from era_5g_relay_network_application.worker_publisher import WorkerPublisher
from era_5g_relay_network_application.worker_service import ServiceData, WorkerService
from era_5g_relay_network_application.worker_socketio import WorkerSocketIO
from era_5g_relay_network_application.worker_socketio_server import WorkerSocketIOServer
from era_5g_relay_network_application.worker_subscriber import WorkerSubscriber
from era_5g_relay_network_application.worker_tf import WorkerTF
from era_5g_server.server import NetworkApplicationServer
from rclpy.parameter import Parameter
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("relay server python")

# port of the netapp's server
NETAPP_PORT = int(os.getenv("NETAPP_PORT", 5896))

QUEUE_LENGTH_TOPICS = int(os.getenv("QUEUE_LENGTH_TOPICS", 1))
QUEUE_LENGTH_SERVICES = int(os.getenv("QUEUE_LENGTH_SERVICES", 1))
QUEUE_LENGTH_TF = int(os.getenv("QUEUE_LENGTH_TF", 1))
USE_SIM_TIME = os.getenv("USE_SIM_TIME", "false").lower() in ("true", "1", "t")
EXTENDED_MEASURING = bool(os.getenv("EXTENDED_MEASURING", False))


class RelayTopic:
    """Base class that holds information about topic and its type."""

    def __init__(
        self,
        topic_name: str,
        topic_type: str,
        channel_type: ChannelType,
        compression: Optional[Compressions] = None,
        qos: Optional[QoSProfile] = None,
    ):
        self.topic_name = topic_name
        self.topic_type = topic_type
        self.compression = compression
        self.qos = qos
        self.channel_type = channel_type
        self.channel_name = f"topic/{self.topic_name}"
        self.queue: Queue[Any] = Queue(QUEUE_LENGTH_TOPICS)


class RelayTopicIncoming(RelayTopic):
    """Class that holds information about incoming topic (i.e. topic that is received from the relay client and is
    published here), its type and related publisher."""

    def __init__(
        self,
        topic_name: str,
        topic_type: str,
        channel_type: ChannelType,
        node,
        compression: Optional[Compressions] = None,
        qos: Optional[QoSProfile] = None,
    ):
        super().__init__(topic_name, topic_type, channel_type, compression, qos)

        # this sucks, the classes should have some common ancestor or there should be two properties
        self.worker: Union[WorkerImagePublisher, WorkerPublisher]

        if self.channel_type in IMAGE_CHANNEL_TYPES:
            self.worker = WorkerImagePublisher(
                self.queue, self.topic_name, self.topic_type, compression=compression, node=node
            )
        else:
            self.worker = WorkerPublisher(
                self.queue, self.topic_name, self.topic_type, node, self.compression, self.qos
            )
        self.worker.daemon = True
        self.worker.start()


class RelayTopicOutgoing(RelayTopic):
    """Class that holds information about outgoing topic (i.e. topic that is subscribed to be sent to the relay client),
    its type and related subscriber."""

    def __init__(
        self,
        topic_name: str,
        topic_type: str,
        channel_type: ChannelType,
        node: rclpy.node.Node,
        compression: Optional[Compressions] = None,
        qos: Optional[QoSProfile] = None,
        action_topic_variant: ActionTopicVariant = ActionTopicVariant.NONE,
        action_subscribers: Optional[ActionSubscribers] = None,
    ):
        super().__init__(topic_name, topic_type, channel_type, compression, qos)

        self.action_topic_variant = action_topic_variant

        # this sucks, the classes should have some common ancestor or there should be two properties
        self.worker: Union[WorkerImageSubscriber, WorkerSubscriber]

        if self.channel_type in IMAGE_CHANNEL_TYPES:
            self.worker = WorkerImageSubscriber(topic_name, topic_type, node, self.queue)
        else:
            self.worker = WorkerSubscriber(
                topic_name, topic_type, node, self.queue, compression, qos, action_topic_variant, action_subscribers
            )
            # Topic name may be changed in case of action-related topics
            self.channel_name = f"topic/{self.worker.topic_name}"
            self.topic_name = self.worker.topic_name


class RelayService:
    """Class that holds information about incoming service (i.e. service that is called from relay client), its type and
    related worker."""

    def __init__(
        self,
        service_name: str,
        service_type: str,
        node: rclpy.node.Node,
        qos: Optional[QoSProfile] = None,
        action_service_variant: ActionServiceVariant = ActionServiceVariant.NONE,
        action_subscribers: Optional[ActionSubscribers] = None,
    ):
        self.service_type = service_type

        self.queue_request: Queue[ServiceData] = Queue(QUEUE_LENGTH_SERVICES)
        self.queue_response: Queue[ServiceData] = Queue(QUEUE_LENGTH_SERVICES)

        self.worker = WorkerService(
            service_name,
            service_type,
            self.queue_request,
            self.queue_response,
            node,
            qos,
            action_service_variant,
            action_subscribers,
        )

        # Service name can be changed by worker in case of action-related services
        self.service_name = self.worker.service_name
        self.channel_name_request = f"service_request/{self.service_name}"
        self.channel_name_response = f"service_response/{self.service_name}"

        self.worker.daemon = True
        self.worker.start()


class RelayServer(NetworkApplicationServer):
    def __init__(
        self,
        port: int,
        topics_incoming: Dict[str, RelayTopicIncoming],  # topics that are received from the relay client
        topics_outgoing: Dict[str, RelayTopicOutgoing],  # topics that are subscribed to be sent to the relay client
        services_incoming: Dict[str, RelayService],
        *args,
        tf_queue: Optional[Queue] = None,
        host: str = "0.0.0.0",
        **kwargs,
    ) -> None:
        self.callbacks_info: Dict[str, CallbackInfoServer] = dict()
        self.workers_outgoing: Dict[str, WorkerSocketIO] = dict()

        # collects info about topics that are received from the relay client
        for topic in topics_incoming.values():
            if topic.channel_type in IMAGE_CHANNEL_TYPES:
                callback = partial(
                    self.image_callback,
                    queue=topic.queue,
                )
            else:
                callback = partial(
                    self.json_callback,
                    queue=topic.queue,
                )
            self.callbacks_info[topic.channel_name] = CallbackInfoServer(topic.channel_type, callback)

        # collects info about services that are called from the relay client
        for service in services_incoming.values():
            callback = partial(
                self.service_callback,
                queue=service.queue_request,
            )
            self.callbacks_info[service.channel_name_request] = CallbackInfoServer(ChannelType.JSON, callback)

            # services are bidirectional, so we need to create worker for response
            # unlike the outgoing topics, service response is only send to the client who called it, so we need a
            # different send method
            send_function = partial(self.send_data_with_sid, event=service.channel_name_response)
            worker = WorkerSocketIO(service.queue_response, send_function)
            self.workers_outgoing[service.channel_name_response] = worker

        # callback_info needs to be passed to the parent class
        super().__init__(port, self.callbacks_info, *args, command_callback=self.command_callback, host=host, **kwargs)
        self.result_subscribers = LockedSet()  # contains data namespace SIDs of clients that want to receive results

        # collects info about topics that are subscribed to be sent to the relay client
        for topic_out in topics_outgoing.values():
            if topic_out.channel_type in [ChannelType.JSON, ChannelType.JSON_LZ4]:
                send_function = partial(
                    self.send_data,
                    event=topic_out.channel_name,
                    channel_type=topic_out.channel_type,
                    can_be_dropped=True,
                )
            else:
                send_function = partial(
                    self.send_image_data,
                    event=topic_out.channel_name,
                    channel_type=topic_out.channel_type,
                    can_be_dropped=True,
                )

            if topic_out.action_topic_variant == ActionTopicVariant.ACTION_FEEDBACK:
                # Action feedback is not sent to everyone, but to a specific sid
                send_function = partial(self.send_data_with_sid, event=topic_out.channel_name)
                worker = WorkerSocketIO(topic_out.queue, send_function)
            else:
                worker = WorkerSocketIOServer(topic_out.queue, self.result_subscribers, send_function)

            self.workers_outgoing[topic_out.topic_name] = worker

        if tf_queue:
            send_function = partial(self.send_data, event="topic//tf")
            self.workers_outgoing["/tf"] = WorkerSocketIOServer(tf_queue, self.result_subscribers, send_function)

        self.topics_outgoing = topics_outgoing
        self.topics_incoming = topics_incoming

    def send_data_with_sid(self, data: Tuple[str, Dict], event: str) -> None:
        """Sends response to the relay client.

        This is intended mainly for sending service response and action feedback.

        Args:
            data (Tuple[str, Dict]): Tuple of namespace SID and message data (e.g. service response).
            event (str): Name of the event (channel) to send the data to.
        """

        self.send_data(data=data[1], event=event, sid=data[0])

    def send_image_data(self, data: Tuple, event: str, channel_type: ChannelType, sid: str, can_be_dropped=False):
        """Unpacks image data and timestamp and sends it to the relay client.

        Args:
            data (Tuple): Tuple of image data and timestamp.
            event (str): Name of the event (channel) to send the data to.
            channel_type (ChannelType): The type of the channel to send the data to (JPEG or H264 or HEVC).
            sid (str): Data namespace SID of the client that sent the data.
            can_be_dropped (bool, optional): _description_. Defaults to False.
        """

        self.send_image(data[1], event, channel_type, data[0], sid=sid, can_be_dropped=can_be_dropped)

    def run(self):
        """Runs the SocketIO server and starts all workers."""

        for worker in self.workers_outgoing.values():
            worker.daemon = True
            worker.start()
        self.run_server()

    def image_callback(self, sid: str, data: Dict[str, Any], queue: Queue):
        """Allows to receive image data using the websocket transport.

        Args:
            sid (str): Data namespace SID of the client that sent the data.
            data (Dict[str, Any]): The image data in format {"frame": <image_data>, "timestamp": <timestamp>}.
            queue (Queue): The queue to pass the data to the publisher worker.
        """

        try:
            queue.put_nowait((data["frame"], data["timestamp"]))
        except Full:
            pass

    def json_callback(self, sid: str, data: Dict, queue: Queue):
        """Allows to receive json data using the websocket transport.

        Args:
            sid (str): Data namespace SID of the client that sent the data.
            data (Dict): The json data.
            queue (Queue): The queue to pass the data to the publisher worker.
        """

        print(f"json_callback: {sid}, {data}")
        try:
            queue.put_nowait((data, Channels.get_timestamp_from_data(data)))
        except Full:
            pass

    def service_callback(self, sid: str, data: Dict, queue: Queue):
        """Allows to receive service request using the websocket transport.

        Args:
            sid (str): Data namespace SID of the client that sent the data.
            data (Dict): The service request.
            queue (Queue): The queue to pass the data to the service worker.
        """

        try:
            queue.put_nowait((sid, data))
        except Full:
            pass

    def command_callback(self, command: ControlCommand, sid: str) -> Tuple[bool, str]:
        """Processes the received control command.

        Args:
            command (ControlCommand): The control command.
            sid (str): Control namespace SID of the client that sent the command.

        Returns:
            Tuple[bool, str]: Confirmation that the command was processed.
        """

        if command and command.cmd_type == ControlCmdType.INIT:
            args = command.data
            if args:
                sr = args.get("subscribe_results")
                if sr:
                    self.result_subscribers.add(self.get_sid_of_data(self.get_eio_sid_of_control(sid)))
        return True, ""

    def disconnect_callback(self, eio_sid):
        """Removes the client from the list of result subscribers."""
        self.result_subscribers.discard(eio_sid)


def provide_action(
    action_name: str,
    action_type: str,
    node: rclpy.node.Node,
    services_incoming: Dict[str, RelayService],
    topics_outgoing: Dict[str, RelayTopicOutgoing],
):
    """Provide services and topics related to action."""

    # Shared data structure with information about which action goal was initiated by which client
    action_subscribers = ActionSubscribers()

    # Provide services
    services_to_provide = [
        ActionServiceVariant.ACTION_SEND_GOAL,
        ActionServiceVariant.ACTION_CANCEL_GOAL,
        ActionServiceVariant.ACTION_GET_RESULT,
    ]
    for service_variant in services_to_provide:
        relay_service = RelayService(action_name, action_type, node, None, service_variant, action_subscribers)
        services_incoming[relay_service.worker.service_name] = relay_service

    # Create subscribers for the action topics (feedback and status)
    topics_to_subscribe = [ActionTopicVariant.ACTION_FEEDBACK, ActionTopicVariant.ACTION_STATUS]
    for topic_variant in topics_to_subscribe:
        outgoing_topic = RelayTopicOutgoing(
            action_name,
            action_type,
            ChannelType.JSON,
            node,
            None,
            None,
            topic_variant,
            action_subscribers,
        )
        topics_outgoing[outgoing_topic.topic_name] = outgoing_topic


def main(args=None) -> None:
    """Main function that starts the relay server and all workers."""
    topics_outgoing_list = load_entities_list("TOPICS_TO_CLIENT")  # Topics from server that are sent to the client
    topics_incoming_list = load_entities_list("TOPICS_FROM_CLIENT")  # Topics that are received from the client
    services_incoming_list = load_entities_list("SERVICES_FROM_CLIENT")  # Services that are called from the client
    actions_to_provide = load_entities_list("ACTIONS_FROM_CLIENT")  # Actions provided by this server to the client
    transforms_to_listen = load_transform_list("TRANSFORMS_FROM_CLIENT")  # TFs that are received from the client

    rclpy.init(args=args)
    node = rclpy.create_node("relay_netapp")
    use_sim_time = Parameter("use_sim_time", Parameter.Type.BOOL, USE_SIM_TIME)
    node.set_parameters([use_sim_time])
    node.get_logger().set_level(logging.DEBUG)
    node.get_logger().debug(f"Loaded outgoing topics: {topics_outgoing_list}")
    node.get_logger().debug(f"Loaded incoming topics: {topics_incoming_list}")
    node.get_logger().debug(f"Loaded transforms for listening: {transforms_to_listen}")

    # can't know if client will want to send some TFs, so we have to create worker for it
    topics_incoming_list.append(EntityConfig("/tf", "tf2_msgs/msg/TFMessage"))

    topics_incoming: Dict[str, RelayTopicIncoming] = dict()
    topics_outgoing: Dict[str, RelayTopicOutgoing] = dict()
    services_incoming: Dict[str, RelayService] = dict()

    # create workers for all topics,  services and actions
    for topic_in in topics_incoming_list:
        channel_type = get_channel_type(topic_in.compression, topic_in.type)
        topics_incoming[topic_in.name] = RelayTopicIncoming(
            topic_in.name, topic_in.type, channel_type, node, topic_in.compression, topic_in.qos
        )

    for topic_out in topics_outgoing_list:
        channel_type = get_channel_type(topic_out.compression, topic_out.type)
        topics_outgoing[topic_out.name] = RelayTopicOutgoing(
            topic_out.name, topic_out.type, channel_type, node, topic_out.compression, topic_out.qos
        )

    for srv in services_incoming_list:
        services_incoming[srv.name] = RelayService(srv.name, srv.type, node, srv.qos)

    for action in actions_to_provide:
        provide_action(action.name, action.type, node, services_incoming, topics_outgoing)

    executor = rclpy.executors.MultiThreadedExecutor()
    executor.add_node(node)

    executor_thread = threading.Thread(target=executor.spin, daemon=True)
    executor_thread.start()

    # create worker for TFs
    tf_queue: Optional[Queue] = None
    if transforms_to_listen:
        tf_queue = Queue(QUEUE_LENGTH_TF)
        WorkerTF(transforms_to_listen, tf_queue, node)

    # create relay server
    socketio_process = RelayServer(
        NETAPP_PORT,
        topics_incoming,
        topics_outgoing,
        services_incoming,
        tf_queue=tf_queue,
        extended_measuring=EXTENDED_MEASURING,
    )

    # TODO: An exception in callbacks of executor nodes does not terminate the application!
    #  This should be fixed!

    socketio_process.start()
    socketio_process.join()

    rclpy.shutdown()  # Shutdown rclpy when finished"""


if __name__ == "__main__":
    main()
