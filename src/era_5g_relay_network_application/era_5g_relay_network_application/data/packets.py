from dataclasses import dataclass
from enum import auto
from strenum import StrEnum
from typing import Dict
from era_5g_relay_network_application.utils import Compressions


class PacketType(StrEnum):
    """Possible types of packets for Relay Network Application."""

    MESSAGE = auto()
    """Message of topic."""

    SERVICE_REQUEST = auto()
    """Service request"""

    SERVICE_RESPONSE = auto()
    """Service response
    """

@dataclass
class Packet:
    """Dataclass containing information of packet for relay network application.
    """

    
    data: str
    """ROS data encoded in JSON"""

    packet_type: PacketType
    """Type of the packet"""

@dataclass
class MessagePacket(Packet):
    # TODO: add docstring
    topic_name: str
    topic_type: str
    compression: Compressions
    

@dataclass
class ServiceRequestPacket(Packet):
    # TODO: add docstring
    service_name: str
    service_type: str
    id: str
    

@dataclass
class ServiceResponsePacket(Packet):
    # TODO: add docstring
    service_name: str
    service_type: str
    id: str