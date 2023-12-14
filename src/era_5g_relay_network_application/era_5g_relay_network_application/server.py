from functools import partial
import os
import threading

import rclpy
import logging
from multiprocessing import Queue
from queue import Full
from typing import Any, Dict, Optional, Tuple

from era_5g_relay_network_application.worker_image_publisher import WorkerImagePublisher
from era_5g_relay_network_application.worker_publisher import WorkerPublisher
from era_5g_relay_network_application.utils import (
    load_topic_list, 
    load_transform_list, 
    load_services_list, 
    Compressions, 
    get_channel_type,
    IMAGE_CHANNEL_TYPES,
)
from era_5g_relay_network_application.worker_subscriber import WorkerSubscriber
from era_5g_relay_network_application.worker_image_subscriber import WorkerImageSubscriber
from era_5g_relay_network_application.worker_socketio_server import WorkerSocketIOServer
from era_5g_relay_network_application.worker_socketio import WorkerSocketIO
from era_5g_relay_network_application.worker_service import WorkerService
from era_5g_relay_network_application.worker_tf import WorkerTF

from era_5g_server.server import NetworkApplicationServer
from era_5g_interface.utils.locked_set import LockedSet
from era_5g_interface.dataclasses.control_command import ControlCmdType, ControlCommand

from era_5g_interface.channels import CallbackInfoServer, ChannelType, DATA_NAMESPACE, DATA_ERROR_EVENT

# port of the netapp's server
NETAPP_PORT = int(os.getenv("NETAPP_PORT", 5896))

QUEUE_LENGTH_TOPICS = int(os.getenv("QUEUE_LENGTH_TOPICS", 1))
QUEUE_LENGTH_SERVICES = int(os.getenv("QUEUE_LENGTH_SERVICES", 1))
QUEUE_LENGTH_TF = int(os.getenv("QUEUE_LENGTH_TF", 1))

class RelayTopic():
    """ Base class that holds information about topic and its type
    """
    def __init__(self, topic_name: str, topic_type: str, channel_type: ChannelType, compression: Compressions = Compressions.NONE):
        self.topic_name = topic_name
        self.topic_type = topic_type
        self.compression = compression
        self.channel_type = channel_type
        self.channel_name = f"topic/{self.topic_name}"
        self.queue = Queue(QUEUE_LENGTH_TOPICS)
        
        
        
class RelayTopicIncoming(RelayTopic):
    """ Class that holds information about incoming topic(i.e. topic that is received
        from the relay client and is published here), its type and related publisher

    """
    def __init__(self, topic_name: str, topic_type: str, channel_type: ChannelType, node, compression: Compressions = Compressions.NONE):
        super().__init__(topic_name, topic_type, channel_type, compression)
        if self.channel_type in IMAGE_CHANNEL_TYPES:
            self.worker = WorkerImagePublisher(self.queue, self.topic_name, self.topic_type, compression=compression, node=node)
        else:            
            self.worker = WorkerPublisher(self.queue, self.topic_name, self.topic_type, self.compression, node)
        self.worker.daemon = True
        self.worker.start()        

class RelayTopicOutgoing(RelayTopic):
    """ Class that holds information about outgoing topic (i.e. topic that is subscribed 
        to be send to the relay client), its type and related subscriber

    """
    def __init__(self, topic_name: str, topic_type: str, channel_type: ChannelType, node, compression: Compressions = Compressions.NONE):
        super().__init__(topic_name, topic_type, channel_type, compression)
        if self.channel_type in IMAGE_CHANNEL_TYPES:
            self.worker = WorkerImageSubscriber(topic_name, topic_type, node, self.queue)
        else:            
            self.worker = WorkerSubscriber(topic_name, topic_type, compression, node, self.queue)

class RelayService():
    """ Class that holds information about incoming service (i.e. service that is called from relay client), 
        its type and related worker
    """
    def __init__(self, service_name: str, service_type: str, node: rclpy.node.Node):
        self.service_name = service_name
        self.service_type = service_type
        self.channel_name_request = f"service_request/{self.service_name}"
        self.channel_name_response = f"service_response/{self.service_name}"
        self.queue_request = Queue(QUEUE_LENGTH_SERVICES)
        self.queue_response = Queue(QUEUE_LENGTH_SERVICES)
        
        self.worker = WorkerService(self.service_name, self.service_type, self.queue_request, self.queue_response, node)
        
        self.worker.daemon = True
        self.worker.start()


class RelayServer(NetworkApplicationServer):
    def __init__(
        self,
        port: int,
        topics_incoming: Dict[str, RelayTopicIncoming],  # topics that are received from the relay client
        topics_outgoing: Dict[str, RelayTopicOutgoing],  # topics that are subscribed to be send to the relay client
        services_incoming: Dict[str, RelayService],
        *args,
        tf_queue: Optional[Queue] = None,
        host: str = "0.0.0.0", 
        **kwargs,
    ) -> None:
        self.callbacks_info: [str, CallbackInfoServer] = dict()
        self.workers_outgoing: [str, WorkerSocketIO] = dict()
        
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
            
            # services are bidirectional so we need to create worker for response
            # unlike the outgoing topics, service response is only send to the client who called it, so we need a different send method
            send_function = partial(self.send_service_data, event=service.channel_name_response)
            worker = WorkerSocketIO(service.queue_response, send_function)
            self.workers_outgoing[service.channel_name_response] = worker
        
        # callback_info needs to be passed to the parent class
        super().__init__(port, 
                         callbacks_info=self.callbacks_info,
                         *args, 
                         command_callback=self.command_callback,
                         host=host, 
                         **kwargs)
        self.result_subscribers = LockedSet() # contains data namespace SIDs of clients that want to receive results
        
        # collects info about topics that are subscribed to be send to the relay client
        for topic in topics_outgoing.values():
            if topic.channel_type in [ChannelType.JSON, ChannelType.JSON_LZ4]:      
                callback = partial(self.send_data, event=topic.channel_name, channel_type=topic.channel_type, can_be_dropped=True)          
            else:
                callback = partial(self.send_image_data, event=topic.channel_name, can_be_dropped=True, channel_type=topic.channel_type)    
                
            self.workers_outgoing[topic.topic_name] = WorkerSocketIOServer(topic.queue, self.result_subscribers, callback)

        if tf_queue:
            send_function = partial(self.send_data, event="topic//tf")
            self.workers_outgoing["/tf"] = WorkerSocketIOServer(tf_queue, self.result_subscribers, send_function)
        
        
        self.topics_outgoing = topics_outgoing
        self.topics_incoming = topics_incoming
        
    def send_service_data(self, data: Tuple[str, Dict], event: str) -> None:
        """ Sends service response to the relay client

        Args:
            data (Tuple[str, Dict]): Tuple of data namespace SID and service response
            event (str): Name of the event (channel) to send the data to
        """
        self.send_data(data=data[1], event=event, sid=data[0])
        
    def send_image_data(self, data: Tuple, event: str, channel_type: ChannelType, sid: str, can_be_dropped = False):
        """Unpacks image data and timestamp and sends it to the relay client

        Args:
            data (Tuple): Tuple of image data and timestamp
            event (str): _description_
            channel_type (ChannelType): _description_
            can_be_dropped (bool, optional): _description_. Defaults to False.
        """
        self.send_image(data[1], event, channel_type, data[0], sid=sid, can_be_dropped=can_be_dropped)

    def run(self):
        """ Runs the SocketIO server and starts all workers
        """
        for worker in self.workers_outgoing.values():
            worker.daemon = True
            worker.start()
        self.run_server()

    def image_callback(self, sid: str, data: Dict[str, Any], queue: Queue):
        """ Allows to receive image data using the websocket transport

        Args:
            sid (str): Data namespace SID of the client that sent the data
            data (Dict[str, Any]): The image data in format {"frame": <image_data>, "timestamp": <timestamp>}
            queue (Queue): The queue to pass the data to the publisher worker
        """
        
        try:
            queue.put_nowait((data["frame"], data["timestamp"]))        
        except Full:
            pass
              

    def json_callback(self, sid: str, data: Dict, queue: Queue):
        """ Allows to receive json data using the websocket transport

        Args:
            sid (str): Data namespace SID of the client that sent the data
            data (Dict): The json data
            queue (Queue): The queue to pass the data to the publisher worker
        """
        print(f"json_callback: {sid}, {data}")
        try:
            queue.put_nowait(data)        
        except Full:
            pass          
        
    def service_callback(self, sid: str, data: Dict, queue: Queue):
        """ Allows to receive service request using the websocket transport

        Args:
            sid (str): Data namespace SID of the client that sent the data
            data (Dict): The service request
            queue (Queue): The queue to pass the data to the service worker
        """
        try:
            queue.put_nowait((sid, data))        
        except Full:
            pass 
    
    def command_callback(self, command: ControlCommand, sid: str) -> Tuple[bool, str]:
        """ Processes the received control command

        Args:
            command (ControlCommand): The control command
            sid (str): Control namespace SID of the client that sent the command

        Returns:
            Tuple[bool, str]: Confirmation that the command was processed
        """
        if command and command.cmd_type == ControlCmdType.INIT:
            args = command.data
            if args:
                sr = args.get("subscribe_results")
                if sr:
                    self.result_subscribers.add(self.get_sid_of_data(self.get_eio_sid_of_control(sid)))
        return True, ""

    def disconnect_callback(self, eio_sid):  
        """ Removes the client from the list of result subscribers
        """
        self.result_subscribers.discard(eio_sid)

def main(args=None) -> None:
    """ Main function that starts the relay server and all workers
    """
    topics_outgoing_list = load_topic_list()  # Topics that are subscribed to be send to the relay client
    topics_incoming_list = load_topic_list("TOPIC_TO_PUB_LIST")  # Topics that are received from the relay client
    services_incoming_list = load_services_list("SERVICE_TO_PUB_LIST")  # Services that are called from the relay client
    transforms_to_listen = load_transform_list()  # TFs that are received from the relay client

    rclpy.init(args=args)
    node = rclpy.create_node("relay_netapp")
    node.get_logger().set_level(logging.DEBUG)
    node.get_logger().debug(f"Loaded outgoing topics: {topics_outgoing_list}")
    node.get_logger().debug(f"Loaded incoming topics: {topics_incoming_list}")
    node.get_logger().debug(f"Loaded transforms for listening: {transforms_to_listen}")

    # can't know if client will want to send some TFs so we have to create worker for it
    topics_incoming_list.append(("/tf", None, "tf2_msgs/msg/TFMessage", None))
    
    topics_incoming = dict()
    topics_outgoing = dict()
    services_incoming = dict()
    
    # create workers for all topics and services
    for topic_name, _, topic_type, compression in topics_incoming_list:
        channel_type = get_channel_type(compression, topic_type)
            
        topic = RelayTopicIncoming(topic_name, topic_type, channel_type, node, compression) 
        topics_incoming[topic_name] = topic
            
    for topic_name, _, topic_type, compression in topics_outgoing_list:
        channel_type = get_channel_type(compression, topic_type)
        
        topic = RelayTopicOutgoing(topic_name, topic_type, channel_type, node, compression)
        topics_outgoing[topic_name] = topic
       
    for service_name, service_type in services_incoming_list:
        service = RelayService(service_name, service_type, node)
        
        services_incoming[service_name] = service
    
    executor = rclpy.executors.MultiThreadedExecutor()
    executor.add_node(node)

    executor_thread = threading.Thread(target=executor.spin, daemon=True)
    executor_thread.start()
    
    
    # create worker for TFs
    tf_queue: Queue = None
    if transforms_to_listen:
        tf_queue = Queue(QUEUE_LENGTH_TF)
        worker_tf = WorkerTF(transforms_to_listen, tf_queue, node)

    # create relay server
    socketio_process = RelayServer(
        NETAPP_PORT, topics_incoming, topics_outgoing, services_incoming, tf_queue=tf_queue
    )

    socketio_process.start()
    socketio_process.join()

    rclpy.shutdown()  # Shutdown rclpy when finished"""


if __name__ == "__main__":
    main()
