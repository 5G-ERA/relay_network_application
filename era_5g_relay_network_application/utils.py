import json
import os
import threading
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Dict, List, Optional, Tuple, Union

from rclpy.qos import QoSPresetProfiles, QoSProfile  # pants: no-infer-dep
from ros2bag.api import interpret_dict_as_qos_profile  # pants: no-infer-dep
from rosbridge_library.internal import ros_loader  # pants: no-infer-dep
from sensor_msgs.msg import Image  # pants: no-infer-dep

from era_5g_interface.channels import ChannelType


class Compressions(str, Enum):
    LZ4 = "lz4"
    DRACO = "draco"
    JPEG = "jpeg"
    H264 = "h264"
    HEVC = "hevc"


class ActionServiceVariant(IntEnum):
    """Possible types of service servers related to ROS Actions."""

    # Regular service that is not related to any ROS Action.
    NONE = 0

    # Send Goal service related to a particular Action.
    ACTION_SEND_GOAL = 1

    # Cancel Goal service related to a particular Action.
    ACTION_CANCEL_GOAL = 2

    # Get Request service related to a particular Action.
    ACTION_GET_RESULT = 3


class ActionTopicVariant(IntEnum):
    """Possible types of topics related to ROS Actions."""

    # Regular topic not related to any Action.
    NONE = 0

    # Feedback topic related to a particular Action.
    ACTION_FEEDBACK = 1

    # Status topic related to a particular Action.
    ACTION_STATUS = 2


IMAGE_CHANNEL_TYPES = (ChannelType.JPEG, ChannelType.H264, ChannelType.HEVC)


def load_transform_list(env_name: str = "TRANSFORMS_TO_SERVER") -> List[Tuple[str, str, float, float, float]]:
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
        raise ValueError(f"Wrong format of {env_name}, please see documentation.")


@dataclass
class EntityConfig:
    name: str
    type: str
    compression: Optional[Compressions] = None
    qos: Optional[QoSProfile] = None


def load_entities_list(env_name: str = "TOPICS_TO_SERVER") -> List[EntityConfig]:
    """Load list of ROS Topics, Services or Actions intended to be transferred using Relay Network Application.

    Expected to be used with env variable, such as: TOPICS_TO_SERVER, SERVICES_TO_SERVER, ACTIONS_FROM_CLIENT etc.
    """

    entities_list = os.getenv(env_name)
    if entities_list is None:
        return []
    entities_data = json.loads(entities_list)

    if not isinstance(entities_data, list):
        raise ValueError(f"ENV VAR '{env_name}' should contain a list!")

    ret: List[EntityConfig] = []

    for entity in entities_data:
        try:
            e_name = entity["name"]
            e_type = entity["type"]
            comp = entity.get("compression", None)
            e_compr: Optional[Compressions] = None
            if comp:
                e_compr = Compressions(comp)  # Only used for Topics
        except (KeyError, ValueError, TypeError) as e:
            raise ValueError(f"Wrong format of the {env_name} variable.") from e

        ec = EntityConfig(e_name, e_type, e_compr)

        if "qos" in entity:
            if not isinstance(entity["qos"], dict):
                raise ValueError(f"Invalid 'qos' field for entity {e_name}.")

            try:
                ec.qos = QoSPresetProfiles.get_from_short_key(entity["qos"]["preset"])
            except KeyError:
                ec.qos = interpret_dict_as_qos_profile(entity["qos"])

        ret.append(ec)

    return ret


def get_channel_type(compression: Optional[Compressions], topic_type: str) -> ChannelType:
    """Returns the channel type based on the compression and topic type.

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
        elif compression == Compressions.HEVC:
            channel_type = ChannelType.HEVC
        else:
            channel_type = ChannelType.JPEG
    else:
        if compression == Compressions.LZ4:
            channel_type = ChannelType.JSON_LZ4
        else:
            channel_type = ChannelType.JSON
    return channel_type


class ActionSubscribers:
    """Structure to keep information about which client requested a particular action goal.

    This is used to make sure that action feedback messages for a particular goal are sent to the correct client (which
    requested the action goal).
    """

    def __init__(self) -> None:
        self.mutex = threading.Lock()
        self.sid_for_goal_id: Dict[str, str] = {}

    def set_sid_for_goal_id(self, goal_id: str, sid: str) -> None:
        with self.mutex:
            self.sid_for_goal_id[goal_id] = sid

    def get_sid_for_goal_id(self, goal_id: str) -> Union[str, None]:
        sid = self.sid_for_goal_id.get(goal_id)
        return sid

    def remove_sid_for_goal_id(self, goal_ids_to_remove: List[str]) -> None:
        if not goal_ids_to_remove:
            return
        with self.mutex:
            for goal_id in goal_ids_to_remove:
                if goal_id in self.sid_for_goal_id:
                    self.sid_for_goal_id.pop(goal_id)
