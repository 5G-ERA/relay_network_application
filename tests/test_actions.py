import json
import os
import time
from multiprocessing import Process
from typing import Any

import pytest  # pants: no-infer-dep
import rclpy  # pants: no-infer-dep
from action_msgs.msg import GoalStatus  # pants: no-infer-dep
from example_interfaces.action import Fibonacci  # pants: no-infer-dep
from rclpy.action import ActionClient, ActionServer, CancelResponse, GoalResponse  # pants: no-infer-dep
from rclpy.callback_groups import ReentrantCallbackGroup  # pants: no-infer-dep
from rclpy.executors import MultiThreadedExecutor  # pants: no-infer-dep
from rclpy.node import Node  # pants: no-infer-dep

from era_5g_client.client_base import NetAppClientBase
from era_5g_interface.channels import CallbackInfoClient, ChannelType

ACTION_TIMEOUT = 12

ACTION_NAME = "/fibonacci"
ACTION_TYPE = "example_interfaces/Fibonacci"


@pytest.fixture(scope="class")
def _set_action_list():
    action_list = json.dumps([{"name": ACTION_NAME, "type": ACTION_TYPE}])
    os.environ["ACTIONS_TO_SERVER"] = action_list
    os.environ["ACTIONS_FROM_CLIENT"] = action_list
    yield
    del os.environ["ACTIONS_TO_SERVER"]
    del os.environ["ACTIONS_FROM_CLIENT"]


class MinimalActionServer(Node):
    def __init__(self) -> None:
        super().__init__("minimal_action_server")

        self._action_server = ActionServer(
            self,
            Fibonacci,
            "fibonacci",
            execute_callback=self.execute_callback,
            callback_group=ReentrantCallbackGroup(),
            goal_callback=self.goal_callback,
            cancel_callback=self.cancel_callback,
        )
        print(f"Minimal Action Server: ready, domain id: {os.environ['ROS_DOMAIN_ID']}")

    def destroy(self) -> None:
        self._action_server.destroy()
        super().destroy_node()

    def goal_callback(self, goal_request) -> GoalResponse:
        """Accept or reject a client request to begin an action."""
        # This server allows multiple goals in parallel
        print("Minimal Action Server: Received goal request")
        return GoalResponse.ACCEPT

    def cancel_callback(self, goal_handle) -> CancelResponse:
        """Accept or reject a client request to cancel an action."""
        print("Minimal Action Server: Received cancel request")
        return CancelResponse.ACCEPT

    async def execute_callback(self, goal_handle) -> Fibonacci.Result:
        """Execute a goal."""
        print("Minimal Action Server: Executing goal...")

        # Append the seeds for the Fibonacci sequence
        feedback_msg = Fibonacci.Feedback()
        feedback_msg.sequence = [0, 1]

        # Start executing the action
        for i in range(1, goal_handle.request.order):
            if goal_handle.is_cancel_requested:
                goal_handle.canceled()
                print("Minimal Action Server: Goal canceled")
                return Fibonacci.Result()

            # Update Fibonacci sequence
            feedback_msg.sequence.append(feedback_msg.sequence[i] + feedback_msg.sequence[i - 1])

            print("Minimal Action Server: Publishing feedback: {0}".format(feedback_msg.sequence))

            # Publish the feedback
            goal_handle.publish_feedback(feedback_msg)

            # Sleep to enable cancelling
            time.sleep(1)

        goal_handle.succeed()

        # Populate result message
        result = Fibonacci.Result()
        result.sequence = feedback_msg.sequence
        return result


def action_server() -> None:
    """Run simple ROS action node (intended to be run as a separate process)"""

    os.environ["ROS_DOMAIN_ID"] = os.environ["ROS_DOMAIN_ID_SERVER"]
    rclpy.init()
    ros_executor = MultiThreadedExecutor()
    node = MinimalActionServer()
    ros_executor.add_node(node)
    ros_executor.spin()
    rclpy.shutdown()


@pytest.fixture(scope="class")
def _run_action_server(_set_env_common):
    """Run simple ROS action as a separate process."""

    proc = Process(target=action_server)
    proc.start()
    yield
    proc.kill()


class MinimalActionClient(Node):
    def __init__(self) -> None:
        super().__init__("minimal_action_client")
        self._action_client = ActionClient(self, Fibonacci, "fibonacci")
        self.feedback_received = False
        print(f"Minimal Action Client: ready, domain id: {os.environ['ROS_DOMAIN_ID']} ")

    def feedback_callback(self, feedback) -> None:
        print("Minimal Action Client: Received feedback: {0}".format(feedback.feedback.sequence))
        self.feedback_received = True

    def send_goal(self, fibonacci_len: int):
        self.feedback_received = False

        print("Minimal Action Client: Waiting for action server...")
        self._action_client.wait_for_server()

        goal_msg = Fibonacci.Goal()
        goal_msg.order = fibonacci_len

        print("Minimal Action Client: Sending goal request...")
        future = self._action_client.send_goal_async(goal_msg, feedback_callback=self.feedback_callback)

        rclpy.spin_until_future_complete(self, future)
        goal_handle = future.result()

        return goal_handle

    def send_goal_check_accepted(self) -> bool:
        goal_handle = self.send_goal(3)

        if not goal_handle.accepted:
            return False

        return True

    def send_goal_and_cancel(self) -> bool:
        goal_handle = self.send_goal(10)

        if not goal_handle.accepted:
            return False

        # Cancel the goal
        cancel_future = goal_handle.cancel_goal_async()
        rclpy.spin_until_future_complete(self, cancel_future)

        cancel_response = cancel_future.result()
        if len(cancel_response.goals_canceling) > 0:
            return True
        else:
            print("Minimal Action Client: Goal failed to cancel")
        return False

    def send_goal_wait_for_result(self) -> bool:
        goal_handle = self.send_goal(3)

        if not goal_handle.accepted:
            print("Minimal Action Client: Goal rejected")
            return False

        print("Minimal Action Client: Goal accepted")

        result_future = goal_handle.get_result_async()
        rclpy.spin_until_future_complete(self, result_future)

        result = result_future.result().result
        status = result_future.result().status

        if status == GoalStatus.STATUS_SUCCEEDED:
            print("Minimal Action Client: Goal succeeded! Result: {0}".format(result.sequence))
            data = result.sequence.tolist()
            if data == [0, 1, 1, 2]:
                return True
        else:
            print("Minimal Action Client: Goal failed with status: {0}".format(status))

        return False

    def send_goal_check_feedback(self) -> bool:
        result_received = self.send_goal_wait_for_result()
        return result_received and self.feedback_received


@pytest.fixture()
def action_client(client_executor):
    node = MinimalActionClient()
    client_executor.add_node(node)
    yield node
    client_executor.remove_node(node)


class MinimalSecondRelayClient:
    """This Relay Client is only intended to test correct sending of feedback and status messages for Actions."""

    def __init__(self) -> None:
        netapp_address = os.environ["NETAPP_ADDRESS"]
        callbacks_info = dict()
        channel_type = ChannelType.JSON
        callbacks_info["topic//fibonacci/_action/feedback"] = CallbackInfoClient(channel_type, self.feedback_callback)
        callbacks_info["topic//fibonacci/_action/status"] = CallbackInfoClient(channel_type, self.status_callback)
        client = NetAppClientBase(callbacks_info)
        client.register(netapp_address, args={"subscribe_results": True})

        self.status_received = False
        self.feedback_received = False

    def feedback_callback(self, data: Any) -> None:
        self.feedback_received = True

    def status_callback(self, data: Any) -> None:
        self.status_received = True


class TestActions:
    """Tests for ROS Actions."""

    @pytest.fixture(autouse=True, scope="class")
    def _required_fixtures(self, _set_action_list, _run_action_server, _run_relay_server, _run_relay_client):
        pass

    @pytest.mark.timeout(ACTION_TIMEOUT)
    def test_action_goal_accepted(self, action_client) -> None:
        """Test that Action correctly accepts a Goal using Relay."""
        goal_accepted = action_client.send_goal_check_accepted()
        assert goal_accepted

    @pytest.mark.timeout(ACTION_TIMEOUT)
    def test_action_result_received(self, action_client) -> None:
        """Test that Action returns a result for given Goal using Relay."""
        result_received = action_client.send_goal_wait_for_result()
        assert result_received

    @pytest.mark.timeout(ACTION_TIMEOUT)
    def test_action_goal_canceled(self, action_client) -> None:
        """Test cancelling Action Goal over Relay."""
        goal_canceled = action_client.send_goal_and_cancel()
        assert goal_canceled

    @pytest.mark.timeout(ACTION_TIMEOUT)
    def test_action_feedback_received(self, action_client) -> None:
        """Test that Action Feedback is received using Relay."""
        feedback_received = action_client.send_goal_check_feedback()
        assert feedback_received

    @pytest.mark.timeout(ACTION_TIMEOUT)
    def test_action_feedback_sent_to_correct_client(self, action_client) -> None:
        """Test calling Action when two Relay Clients are connected.

        When two Relay Clients are connected and one calls Action, then both of them should receive Action status
        messages, but only the one who called the Action should get the corresponding goal feedback messages
        """
        second_client = MinimalSecondRelayClient()
        feedback_received = action_client.send_goal_check_feedback()
        assert feedback_received
        assert second_client.status_received
        assert not second_client.feedback_received
