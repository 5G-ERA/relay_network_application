import rclpy  # pants: no-infer-dep
from rclpy.action import ActionClient  # pants: no-infer-dep
from rclpy.node import Node  # pants: no-infer-dep

try:
    from rosbridge_library.internal.ros_loader import get_action_class
except ImportError:
    from era_5g_relay_network_application.compatibility.rosbridge_action_loader import get_action_class


class TestActionClient(Node):
    def __init__(self) -> None:
        super().__init__("test_action_client")
        action_type = "turtlesim/action/RotateAbsolute"
        self.action_name = "/turtle1/rotate_absolute"
        self.action_type_class = get_action_class(action_type)
        self._action_client = ActionClient(self, self.action_type_class, self.action_name)
        self.custom_theta = 0

    def send_goal(self) -> None:
        goal_msg = self.action_type_class.Goal()
        self.custom_theta += 0.2  # type: ignore
        goal_msg.theta = self.custom_theta

        print("Waiting for server")
        self._action_client.wait_for_server()

        print("Sending goal")
        self._send_goal_future = self._action_client.send_goal_async(goal_msg, feedback_callback=self.feedback_callback)

        self._send_goal_future.add_done_callback(self.goal_response_callback)
        print("Goal sent")

    def goal_response_callback(self, future) -> None:
        goal_handle = future.result()
        if not goal_handle.accepted:
            self.get_logger().info("Goal rejected")
            return

        self.get_logger().info("Goal accepted")

        self._get_result_future = goal_handle.get_result_async()
        self._get_result_future.add_done_callback(self.get_result_callback)

    def get_result_callback(self, future) -> None:
        self.get_logger().info("Result received")
        rclpy.shutdown()

    def feedback_callback(self, feedback_msg) -> None:
        feedback = feedback_msg.feedback
        self.get_logger().info(f"Received feedback msg: {(feedback)}")


def main(args=None):
    rclpy.init(args=args)

    action_client = TestActionClient()

    action_client.send_goal()

    rclpy.spin(action_client)


if __name__ == "__main__":
    main()
