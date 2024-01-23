import json
import os
import signal
import subprocess as sp
import time
from typing import Generator

import pytest  # pants: no-infer-dep
from rclpy.node import Node  # pants: no-infer-dep
from rosbridge_library.internal import ros_loader  # pants: no-infer-dep

TOPIC_TIMEOUT = 12

TOPIC_NAME = "/test"
TOPIC_TYPE = "std_msgs/msg/Empty"


class MinimalSubscriber(Node):
    def __init__(self, topic_name, topic_type) -> None:
        super().__init__("minimal_subscriber")
        self.message_received = False
        topic_type_class = ros_loader.get_message_instance(topic_type)
        self.subscription = self.create_subscription(topic_type_class, topic_name, self.listener_callback, 10)
        print(f"Minimal Subscriber: ready, domain id: {os.environ['ROS_DOMAIN_ID']}")

    def listener_callback(self, msg) -> None:
        self.message_received = True


@pytest.fixture(
    scope="class",
    params=[
        None,
        {"qos": {"preset": "system_default"}},
        {"qos": {"depth": 10}},
    ],
)
def _set_topic_list_client_to_server(request):
    env = {"name": TOPIC_NAME, "type": TOPIC_TYPE}

    if request.param:
        env.update(request.param)

    topic_list = json.dumps([env])
    os.environ["TOPICS_TO_SERVER"] = topic_list
    os.environ["TOPICS_FROM_CLIENT"] = topic_list
    yield
    del os.environ["TOPICS_TO_SERVER"]
    del os.environ["TOPICS_FROM_CLIENT"]


@pytest.fixture(
    scope="class",
    params=[
        None,
        {"qos": {"preset": "system_default"}},
        {"qos": {"depth": 10}},
    ],
)
def _set_topic_list_server_to_client(request):
    env = {"name": TOPIC_NAME, "type": TOPIC_TYPE}

    if request.param:
        env.update(request.param)

    topic_list = json.dumps([env])

    os.environ["TOPICS_TO_CLIENT"] = topic_list
    os.environ["TOPICS_FROM_SERVER"] = topic_list
    yield
    del os.environ["TOPICS_TO_CLIENT"]
    del os.environ["TOPICS_FROM_SERVER"]


def run_publisher(publisher_env) -> int:
    publisher = sp.Popen(
        ["ros2", "topic", "pub", TOPIC_NAME, TOPIC_TYPE],
        preexec_fn=os.setsid,
        env=publisher_env,
        stdout=sp.PIPE,
        stderr=sp.STDOUT,
    )
    if publisher.poll() is not None:
        print(publisher.communicate())
    return publisher.pid


@pytest.fixture()
def _run_client_publisher() -> Generator[None, None, None]:
    publisher_env = os.environ.copy()
    publisher_env["ROS_DOMAIN_ID"] = os.environ["ROS_DOMAIN_ID_CLIENT"]
    print(f"Publisher domain id will be: {publisher_env['ROS_DOMAIN_ID']}")
    pid = run_publisher(publisher_env)
    yield
    os.killpg(os.getpgid(pid), signal.SIGKILL)
    os.waitpid(pid, 0)


@pytest.fixture()
def _run_server_publisher() -> Generator[None, None, None]:
    publisher_env = os.environ.copy()
    publisher_env["ROS_DOMAIN_ID"] = os.environ["ROS_DOMAIN_ID_SERVER"]
    print(f"Publisher domain id will be: {publisher_env['ROS_DOMAIN_ID']}")
    pid = run_publisher(publisher_env)
    yield
    os.killpg(os.getpgid(pid), signal.SIGKILL)
    os.waitpid(pid, 0)


class TestTopicsClientToServer:
    """Tests for sending ROS Topics from Client to Server."""

    @pytest.fixture(autouse=True, scope="class")
    def _required_fixtures(
        self, _set_env_common, _set_topic_list_client_to_server, _run_relay_server, _run_relay_client
    ) -> None:
        pass

    @pytest.mark.timeout(TOPIC_TIMEOUT)
    @pytest.mark.usefixtures("_run_client_publisher")
    def test_relay_topic_from_client_to_server(self, server_executor) -> None:
        """Test that topic messages are correctly sent from Client to Server."""

        subscriber_node = MinimalSubscriber(TOPIC_NAME, TOPIC_TYPE)
        server_executor.add_node(subscriber_node)

        for _ in range(10):
            if subscriber_node.message_received:
                break
            time.sleep(1)
        assert subscriber_node.message_received


class TestTopicsServerToClient:
    """Tests for sending ROS Topics from Server to Client."""

    @pytest.fixture(autouse=True, scope="class")
    def _required_fixtures(
        self, _set_env_common, _set_topic_list_server_to_client, _run_relay_server, _run_relay_client
    ) -> None:
        pass

    @pytest.mark.timeout(TOPIC_TIMEOUT)
    @pytest.mark.usefixtures("_run_server_publisher")
    def test_relay_topic_from_server_to_client(self, client_executor) -> None:
        """Test that topic messages are correctly sent from Server to Client."""

        subscriber_node = MinimalSubscriber(TOPIC_NAME, TOPIC_TYPE)
        client_executor.add_node(subscriber_node)

        for _ in range(10):
            if subscriber_node.message_received:
                break
            time.sleep(1)
        assert subscriber_node.message_received
