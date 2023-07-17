import binascii
from dataclasses import asdict
import os
from era_5g_relay_network_application.utils import load_topic_list, load_services_list, build_service_response
import numpy as np
import logging
import time
from rosbridge_library.internal import ros_loader
from rosbridge_library.internal.message_conversion import populate_instance, extract_values
from era_5g_relay_network_application.dataclasses.packets import MessagePacket, ServiceRequestPacket, ServiceResponsePacket, PacketType

import socketio
from queue import Full, Queue

from typing import Dict, Set
from flask import Flask


from era_5g_relay_network_application.worker import Worker
from era_5g_relay_network_application.worker_image import WorkerImage
from era_5g_relay_network_application.worker_results import WorkerResults
from era_5g_interface.dataclasses.control_command import ControlCommand, ControlCmdType

import rospy

from threading import Lock

# port of the netapp's server
NETAPP_PORT = os.getenv("NETAPP_PORT", 5896)
# input queue size
NETAPP_INPUT_QUEUE = int(os.getenv("NETAPP_INPUT_QUEUE", 1))

#image_queue = Queue(NETAPP_INPUT_QUEUE)
    
# the max_http_buffer_size parameter defines the max size of the message to be passed
sio = socketio.Server(async_mode='threading', max_http_buffer_size=5*(1024**2))
app = Flask(__name__)
app.wsgi_app = socketio.WSGIApp(sio, app.wsgi_app)

workers: Dict[str, Worker] = dict()

result_subscribers: Set[str] = set()

nh = None

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
    recv_timestamp = time.time_ns()
    if 'timestamp' in data:
        timestamp = data['timestamp']
    else:
        logging.debug("Timestamp not set, setting default value")
        timestamp = 0

    eio_sid = sio.manager.eio_sid_from_sid(sid, "/data")

   
      
    if "frame" not in data:
        logging.error(f"Data does not contain frame.")
        sio.emit(
            "image_error",
            {"timestamp": timestamp,
             "error": f"Data does not contain frame."},
                namespace='/data',
                to=sid
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
            q = Queue(1)
            worker_thread = WorkerImage(q, topic_name, topic_type)
            worker_thread.daemon = True
            worker_thread.start()
            workers[topic_name] = worker_thread
        try:
            
            worker_thread.queue.put(msg, block=False)
        except Full:
            pass    
        
    except (ValueError, binascii.Error) as error:
        logging.error(f"Failed to decode frame data: {error}")
        sio.emit(
            "image_error",
            {"timestamp": timestamp,
             "error": f"Failed to decode frame data: {error}"},
            namespace='/data',
            to=sid
            )
        return
    
    
    
       
    

@sio.on('json', namespace='/data')
def json_callback_websocket(sid, data):
    """
    Allows to receive general json data using the websocket transport

    Args:
        data (dict): NetApp-specific json data

    Raises:
        ConnectionRefusedError: Raised when attempt for connection were made
            without registering first.
    """
    logging.debug(f"client with task id: {sio.manager.eio_sid_from_sid(sid, '/data')} sent data {data}")
    print(data)
    global workers
    packet_type = data.get("packet_type")
    if packet_type == PacketType.MESSAGE:
        packet = MessagePacket(**data)
        
        worker_thread = workers.get(packet.topic_name)
        if worker_thread is None:        
            q = Queue(1)
            worker_thread = Worker(q, packet.topic_name, packet.topic_type)
            worker_thread.daemon = True
            worker_thread.start()
            workers[packet.topic_name] = worker_thread
        try:
            worker_thread.queue.put(packet.data, block=False)
        except Full:
            pass
        
    elif packet_type == PacketType.SERVICE_REQUEST:
        packet = ServiceRequestPacket(**data)
        proxy = rospy.ServiceProxy(packet.service_name, ros_loader.get_service_class(packet.service_type))
        inst = ros_loader.get_service_request_instance(packet.service_type)
        # Populate the instance with the provided args
        res = proxy.call(populate_instance(packet.data, inst)) 
        d = extract_values(res)
        message = ServiceResponsePacket(packet_type=PacketType.SERVICE_RESPONSE, data=d, service_name=packet.service_name, service_type=packet.service_type, id=packet.id)
        sio.emit("message", asdict(message), namespace="/results", to=sio.manager.sid_from_eio_sid(sid, "/results"))
    
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


def main():
    topics = load_topic_list()
    services = load_services_list()
    logging.getLogger().setLevel(logging.DEBUG)
    global nh
    nh = rospy.init_node('relay_netapp', anonymous=True, disable_signals=True)
    for topic_name, topic_type in topics:
        _ = WorkerResults(topic_name, topic_type, sio, result_subscribers)
    #for service_name, service_type in services:

    logging.info(f"The size of the queue set to: {NETAPP_INPUT_QUEUE}")

    # runs the flask server
    # allow_unsafe_werkzeug needs to be true to run inside the docker
    # TODO: use better webserver
    app.run(port=NETAPP_PORT, host='0.0.0.0')


if __name__ == '__main__':
    main()
