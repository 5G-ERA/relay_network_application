
import os
from typing import Any, Dict, Set
import uuid
from era_5g_client.client_base import NetAppClientBase, NetAppLocation, FailedToConnect
from rosbridge_library.internal.message_conversion import extract_values, populate_instance
from rosbridge_library.internal import ros_loader
from era_5g_relay_network_application.utils import load_topic_list, load_services_list, build_message, build_service_request
import rospy
from cv_bridge import CvBridge
from rospy import Publisher
from functools import partial
from sensor_msgs.msg import Image
from era_5g_relay_network_application.dataclasses.packets import MessagePacket, ServiceRequestPacket, ServiceResponsePacket, PacketType
from dataclasses import asdict

# ip address or hostname of the computer, where the netapp is deployed
NETAPP_ADDRESS = os.getenv("NETAPP_ADDRESS", "127.0.0.1")
# port of the netapp's server
NETAPP_PORT = int(os.getenv("NETAPP_PORT", 5896))

bridge = CvBridge()
client: NetAppClientBase = None

results_publishers: Dict[str, Publisher] = dict()

services_results: Dict[str, Dict] = dict()

def callback_image(data: Image, topic_name=None, topic_type=None):
    if topic_name is None or topic_type is None:
        print("You need to specify topic name and type!")
        return
    cv_image = bridge.imgmsg_to_cv2(data, desired_encoding="bgr8")
    client.send_image_ws(cv_image, data.header.stamp.to_nsec(), metadata={"topic_name": topic_name, "topic_type": topic_type})

def callback_others(data: Any, topic_name=None, topic_type=None):
    if topic_name is None or topic_type is None:
        print("You need to specify topic name and type!")
        return
    
    d = extract_values(data)
    message = build_message(topic_name, topic_type, d)
    message = MessagePacket(packet_type=PacketType.MESSAGE, 
                            data=d, 
                            topic_name=topic_name, 
                            topic_type=topic_type)
    client.send_json_ws(asdict(message))

def results(data):
    if type(data) != dict:
        return
    packet_type = data.get("packet_type")
    if packet_type == PacketType.MESSAGE:
        packet = MessagePacket(**data)
        pub = results_publishers.get(packet.topic_name)
        inst = ros_loader.get_message_instance(packet.topic_type)
        if pub is None:            
            pub = rospy.Publisher(packet.topic_name, type(inst), queue_size=10)
            results_publishers[packet.topic_name] = pub
        pub.publish(populate_instance(packet.data, inst))
    elif packet_type == PacketType.SERVICE_RESPONSE:
        packet = ServiceResponsePacket(**data)   
        inst = ros_loader.get_service_response_instance(packet.service_type)
        services_results[packet.id] = populate_instance(packet.data, inst)

def callback_service(req, service_name=None, service_type=None):
    if None in [service_type, service_name]:
        print("Service name and type needs to be specified!")
        return
    d = extract_values(req)
    message = ServiceRequestPacket(packet_type=PacketType.SERVICE_REQUEST, 
                                   data=d, 
                                   service_name=service_name, 
                                   service_type=service_type, 
                                   id=str(uuid.uuid4()))
    client.send_json_ws(asdict(message))
    
    resp = wait_for_service_response(message.id)
    return resp
    
# TODO: timeout?
def wait_for_service_response(id: str):
    while True:
        if id in services_results:
            return services_results.pop(id)
        rospy.sleep(0.01)
   
def main():
    nh = rospy.init_node('test', anonymous=True, log_level=rospy.ERROR)
    topics = load_topic_list()
    services = load_services_list()
    if topics is None:
        return
    global client
    subs = []
    try:
        client = NetAppClientBase(results)
        print(f"addr: {NETAPP_ADDRESS}, port: {NETAPP_PORT}")
        client.register(NetAppLocation(NETAPP_ADDRESS, NETAPP_PORT), args={"subscribe_results": True})
        for topic_name, topic_type in topics:
            
            topic_type_class = ros_loader.get_message_class(topic_type)
            if topic_type == "sensor_msgs/Image":   
                callback = partial(callback_image, topic_type=topic_type, topic_name=topic_name)                 
            else:
                callback = partial(callback_others, topic_type=topic_type, topic_name=topic_name)
            subs.append(rospy.Subscriber(topic_name, topic_type_class, callback))
        for service_name, service_type in services:
            service_type_class = ros_loader.get_service_class(service_type)
            _ = rospy.Service(service_name, service_type_class, partial(callback_service, service_name=service_name, service_type=service_type) )
        rospy.spin()
    except FailedToConnect as ex:
        print(f"Failed to connect: {ex}")
if __name__ == "__main__":
    main()