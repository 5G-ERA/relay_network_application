import json
import os

def load_topic_list():
    topic_list = os.getenv("TOPIC_LIST")
    if topic_list is None:
        print("Please specify topics using the TOPIC_LIST env variable. The format is:")
        print_format()
        return None
    topic_data = json.loads(topic_list)
    try:
        topics = [(topic["topic_name"], topic["topic_type"]) for topic in topic_data]
    except KeyError:
        print("Wrong format of the TOPIC_LIST variable. The correct format is:")
        print_format()
        return None
    return topics

def print_format():
    print("[")
    print('  {"topic_name": "/topic1", "topic_type": "Type1"},')
    print('  {"topic_name": "/topic2", "topic_type": "Type2"},')
    print('  {"topic_name": "/topic3", "topic_type": "Type3"}')
    print(']')