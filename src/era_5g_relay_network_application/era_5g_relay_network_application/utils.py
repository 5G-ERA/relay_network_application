import json
import os
import uuid

def load_topic_list():
    topic_list = os.getenv("TOPIC_LIST")
    if topic_list is None:
        return None
    topic_data = json.loads(topic_list)
    try:
        topics = [(topic["topic_name"], topic["topic_type"]) for topic in topic_data]
    except KeyError:
        print("Wrong format of the TOPIC_LIST variable. The correct format is:")
        print_format()
        return None
    return topics

def load_services_list():
    service_list = os.getenv("SERVICE_LIST")
    if service_list is None:
        return None
    service_data = json.loads(service_list)
    try:
        services = [(service["service_name"], service["service_type"]) for service in service_data]
    except KeyError:
        print("Wrong format of the TOPIC_LIST variable. The correct format is:")
        print_format()
        return None
    return services

def build_message(topic_name, topic_type, msg):
    return {"type": "message", "topic_name": topic_name, "topic_type": topic_type, "msg": msg}

def build_service_request(service_name, service_type, req):
    return {"type": "service_request", "service_name": service_name, "service_type": service_type, "req": req, "id": str(uuid.uuid4())}    

def build_service_response(service_name, service_type, res, id):
    return {"type": "service_response", "service_name": service_name, "service_type": service_type, "res": res, "id": id}  

def print_format():
    print("[")
    print('  {"topic_name": "/topic1", "topic_type": "Type1"},')
    print('  {"topic_name": "/topic2", "topic_type": "Type2"},')
    print('  {"topic_name": "/topic3", "topic_type": "Type3"}')
    print(']')