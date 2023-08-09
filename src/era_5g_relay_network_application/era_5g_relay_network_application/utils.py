import json
import os
import uuid

from typing import List, Tuple, Optional, Dict


def load_topic_list() -> List[Tuple[str, Optional[str], str]]:
    topic_list = os.getenv("TOPIC_LIST")
    if topic_list is None:
        return []
    topic_data = json.loads(topic_list)
    try:
        topics = [
            (topic["topic_name"], topic.get("topic_name_remapped", None), topic["topic_type"]) for topic in topic_data
        ]
    except KeyError:
        print("Wrong format of the TOPIC_LIST variable. The correct format is:")
        print_format()
        raise ValueError()
    return topics


def load_services_list() -> List[Tuple[str, str]]:
    service_list = os.getenv("SERVICE_LIST")
    if service_list is None:
        return []
    service_data = json.loads(service_list)
    try:
        services = [(service["service_name"], service["service_type"]) for service in service_data]
    except KeyError:
        print("Wrong format of the SERVICE_LIST variable. The correct format is:")
        print_format()
        raise ValueError()
    return services


def build_message(topic_name: str, topic_type: str, msg: str) -> Dict[str, str]:
    return {
        "type": "message",
        "topic_name": topic_name,
        "topic_type": topic_type,
        "msg": msg,
    }  # TODO this should be rather dataclass


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


def print_format():
    print("[")
    print('  {"topic_name": "/topic1", "topic_type": "Type1"},')
    print('  {"topic_name": "/topic2", "topic_type": "Type2"},')
    print('  {"topic_name": "/topic3", "topic_name_cloud": "/topic3_cloud", "topic_type": "Type3"}')
    print("]")
