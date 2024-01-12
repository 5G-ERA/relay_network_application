import json
import os
from multiprocessing import Process
from typing import Generator

import pytest  # type: ignore  # pants: no-infer-dep
import rclpy  # pants: no-infer-dep
from example_interfaces.srv import AddTwoInts  # type: ignore  # pants: no-infer-dep
from rclpy.node import Node  # pants: no-infer-dep

SERVICE_TIMEOUT = 12

SERVICE_NAME = "/add_two_ints"
SERVICE_TYPE = "example_interfaces/AddTwoInts"


@pytest.fixture(scope="class")
def _set_service_list() -> Generator[None, None, None]:
    service_list = json.dumps([{"name": SERVICE_NAME, "type": SERVICE_TYPE}])
    os.environ["SERVICES_TO_SERVER"] = service_list
    os.environ["SERVICES_FROM_CLIENT"] = service_list
    yield
    del os.environ["SERVICES_TO_SERVER"]
    del os.environ["SERVICES_FROM_CLIENT"]


class MinimalServiceServer(Node):
    def __init__(self) -> None:
        super().__init__("minimal_service_server")
        self.srv = self.create_service(AddTwoInts, "/add_two_ints", self.add_two_ints_callback)
        print(f"Minimal Service Server: ready, domain id: {os.environ['ROS_DOMAIN_ID']}")

    def add_two_ints_callback(self, request, response):
        print("Minimal Service Server: call accepted")
        response.sum = request.a + request.b
        return response


class MinimalServiceClient(Node):
    def __init__(self, custom_id=0) -> None:
        super().__init__(f"minimal_service_client_{custom_id}")

        self.service_client = self.create_client(AddTwoInts, "add_two_ints")
        print(f"Minimal Service Client: ready, domain id: {os.environ['ROS_DOMAIN_ID']}")

    def call_service(self, a=1, b=2):
        req = AddTwoInts.Request()
        req.a = a
        req.b = b

        print("Minimal Service Client: calling service")
        while not self.service_client.wait_for_service(timeout_sec=1.0):
            print("Minimal Service Client: service not available, waiting again...")

        future = self.service_client.call_async(req)
        rclpy.spin_until_future_complete(self, future)
        print("Minimal Service Client: call returned from server")

        result = future.result()
        return (req.a, req.b, result.sum)


def service_server() -> None:
    """Run simple ROS service node."""

    os.environ["ROS_DOMAIN_ID"] = os.environ["ROS_DOMAIN_ID_SERVER"]
    rclpy.init()
    node = MinimalServiceServer()
    rclpy.spin(node)
    rclpy.shutdown()


@pytest.fixture(scope="class")
def _run_service_server(_set_env_common) -> Generator[None, None, None]:
    """Run simple ROS service as a separate process."""

    proc = Process(target=service_server)
    proc.start()
    yield
    proc.kill()


@pytest.fixture()
def service_client(client_executor) -> Node:
    node = MinimalServiceClient()
    client_executor.add_node(node)
    yield node
    client_executor.remove_node(node)
    node.destroy_node()


class TestServices:
    """Tests for ROS Services."""

    @pytest.fixture(autouse=True, scope="class")
    def _required_fixtures(self, _set_service_list, _run_service_server, _run_relay_server, _run_relay_client) -> None:
        pass

    @pytest.mark.timeout(SERVICE_TIMEOUT)
    def test_service(self, service_client) -> None:
        """Test using Relay to communicate with a Service."""
        a, b, sum = service_client.call_service()
        assert a + b == sum
