
from typing import Any, Dict, Set
from era_5g_client.client_base import NetAppClientBase, NetAppLocation, FailedToConnect
from rosbridge_library.internal.message_conversion import extract_values, populate_instance
from rosbridge_library.internal import ros_loader
from era_5g_relay_network_application.utils import load_topic_list
import rospy
from cv_bridge import CvBridge
from rospy import Publisher
from functools import partial
from sensor_msgs.msg import Image

bridge = CvBridge()
client: NetAppClientBase = None

results_publishers: Dict[str, Publisher] = dict()

def build_message(topic_name, topic_type, msg):
    return {"topic_name": topic_name, "topic_type": topic_type, "msg": msg}
    

def callback_image(data: Image):
    cv_image = bridge.imgmsg_to_cv2(data, desired_encoding="bgr8")
    client.send_image_ws(cv_image, data.header.stamp.to_nsec(), metadata={"topic_name": "/image_raw", "topic_type": "sensor_msgs/Image"})

def callback_others(data: Any, topic_name=None, topic_type=None):
    if topic_name is None or topic_type is None:
        print("You need to specify topic name and type!")
        return
    d = extract_values(data)
    message = build_message(topic_name, topic_type, d)
    client.send_json_ws(message)

def results(data):
    if type(data) != dict:
        return
    topic_name = data.get("topic_name")
    topic_type = data.get("topic_type")
    msg = data.get("msg")
    if topic_name is None or topic_type is None or msg is None:
        return
    pub = results_publishers.get(topic_name)
    inst = ros_loader.get_message_instance(topic_type)
    if pub is None:
        
        pub = rospy.Publisher(topic_name, type(inst), queue_size=10)
        results_publishers[topic_name] = pub
    pub.publish(populate_instance(msg, inst))
    
   
def main():
    nh = rospy.init_node('test', anonymous=True, log_level=rospy.ERROR)
    topics = load_topic_list()
    if topics is None:
        return
    global client
    subs = []
    try:
        client = NetAppClientBase(results)
        client.register(NetAppLocation("localhost", 5896), args={"subscribe_results": True})
        for topic_name, topic_type in topics:
            inst = ros_loader.get_message_instance(topic_type)
            if topic_type == "sensor_msgs/Image":   
                callback = callback_image                 
            else:
                callback = partial(callback_others, topic_type=topic_type, topic_name=topic_name)
            subs.append(rospy.Subscriber(topic_name, type(inst), callback))
        rospy.spin()
    except FailedToConnect as ex:
        print(f"Failed to connect: {ex}")
if __name__ == "__main__":
    main()