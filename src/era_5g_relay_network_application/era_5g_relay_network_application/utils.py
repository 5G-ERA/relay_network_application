from enum import Enum
import json
import os
import uuid

from typing import List, Tuple, Optional, Dict

from era_5g_interface.channels import ChannelType
from rosbridge_library.internal import ros_loader
from sensor_msgs.msg import Image

class Compressions(str, Enum):
    NONE = 'none'
    LZ4 = 'lz4'
    DRACO = 'draco'
    JPEG = 'jpeg'
    H264 = 'h264'

    @classmethod
    def _missing_(cls, value):
        if value is None:
            return cls('none')
        return super()._missing_(value)
    
IMAGE_CHANNEL_TYPES = (ChannelType.JPEG, ChannelType.H264)
    
    

def load_transform_list(env_name: str = "TRANSFORM_LIST") -> List[Tuple[str, str, float, float, float]]:
    tr_list = os.getenv(env_name)
    if tr_list is None:
        return []
    tr_data = json.loads(tr_list)
    try:
        return [
            (
                tr["source_frame"],
                tr["target_frame"],
                tr.get("angular_thres", 0.1),
                tr.get("trans_thres", 0.001),
                tr.get("max_publish_period", 0.0),
            )
            for tr in tr_data
        ]
    except KeyError:
        raise ValueError("Wrong format of TRANSFORM_LIST, please see documentation.")


def load_topic_list(env_name: str = "TOPIC_LIST") -> List[Tuple[str, Optional[str], str]]:
    topic_list = os.getenv(env_name)
    if topic_list is None:
        return []
    topic_data = json.loads(topic_list)
    try:
        return [
            (topic["topic_name"], topic.get("topic_name_remapped", None), topic["topic_type"], Compressions(topic.get("compression", None))) for topic in topic_data
        ]
    except KeyError:
        print("Wrong format of the TOPIC_LIST variable.")
        raise ValueError()


def load_services_list(env_name: str = "SERVICE_LIST") -> List[Tuple[str, str]]:
    service_list = os.getenv(env_name)
    if service_list is None:
        return []
    service_data = json.loads(service_list)
    try:
        return [(service["service_name"], service["service_type"]) for service in service_data]
    except KeyError:
        print("Wrong format of the SERVICE_LIST variable. ")
        raise ValueError()


def build_service_request(service_name: str, service_type: str, req: str) -> Dict[str, str]:
    return {
        "type": "service_request",
        "service_name": service_name,
        "service_type": service_type,
        "req": req,
        "id": uuid.uuid4().hex,
    }  # TODO this should be rather dataclass


def build_service_response(service_name: str, service_type: str, res: str, id: str) -> Dict[str, str]:
    return {
        "type": "service_response",
        "service_name": service_name,
        "service_type": service_type,
        "res": res,
        "id": id,
    }  # TODO this should be rather dataclass


def get_channel_type(compression: Compressions, topic_type: str) -> ChannelType:
    """ Returns the channel type based on the compression and topic type

    Args:
        compression (Compressions): Compression type
        topic_type (str): Topic type as a string (e.g. std_msgs/msg/String)

    Returns:
        ChannelType: The channel type with the correct compression
    """
    topic_type_inst = ros_loader.get_message_instance(topic_type)
    if isinstance(topic_type_inst, Image):
        if compression == Compressions.H264:
            channel_type = ChannelType.H264
        else:
            channel_type = ChannelType.JPEG                  
    else:
        if compression == Compressions.LZ4:
            channel_type = ChannelType.JSON_LZ4
        else:
            channel_type = ChannelType.JSON
    return channel_type


