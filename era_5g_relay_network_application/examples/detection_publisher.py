import json
import sys
from typing import Any, Dict, Optional

import cv2  # pants: no-infer-dep
import numpy as np  # pants: no-infer-dep
import rclpy  # pants: no-infer-dep
from cv_bridge import CvBridge  # pants: no-infer-dep
from rclpy.node import Node  # pants: no-infer-dep
from rclpy.publisher import Publisher  # pants: no-infer-dep
from sensor_msgs.msg import Image  # pants: no-infer-dep
from std_msgs.msg import String  # pants: no-infer-dep

bridge = CvBridge()
image_buffer: Dict[int, np.ndarray] = dict()
output_images_pub: Optional[Publisher] = None

node: Optional[Node] = None


def results_callback(msg: Any) -> None:
    assert node

    try:
        results = json.loads(msg.data)
    except ValueError:
        node.get_logger().error("Results should contain JSON data.")
        raise

    assert isinstance(results["timestamp"], int)

    # prune old images (for which we did not get results)
    for frame_to_remove in {ts for ts in image_buffer.keys() if ts < results["timestamp"]}:
        image_buffer.pop(frame_to_remove)

    if not results["timestamp"]:
        node.get_logger().error("Timestamp is zero. Something is wrong.")
        rclpy.shutdown()
        return

    try:
        frame = image_buffer.pop(results["timestamp"])
    except KeyError as ex:
        node.get_logger().error(f"Frame with timestamp {ex} not found.")
        return

    detections = results["detections"]
    for d in detections:
        score = float(d["score"])
        cls_name = d["class_name"]
        # Draw detection into frame.
        x1, y1, x2, y2 = [int(coord) for coord in d["bbox"]]
        cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 1)
        font = cv2.FONT_HERSHEY_SIMPLEX
        cv2.putText(
            frame,
            f"{cls_name} ({score * 100:.0f})%",
            (x1, y1 - 5),
            font,
            0.5,
            (0, 255, 0),
            1,
            cv2.LINE_AA,
        )

    assert output_images_pub

    output_images_pub.publish(bridge.cv2_to_imgmsg(frame, encoding="bgr8"))


def image_callback(image: Any) -> None:
    global image_buffer

    image_buffer[image.header.stamp.to_nsec()] = bridge.imgmsg_to_cv2(image, desired_encoding="bgr8")

    # make memory of images limited
    ts_to_keep = sorted(list(image_buffer.keys()))[-60:]
    image_buffer = {key: value for key, value in image_buffer.items() if key in ts_to_keep}


def main(args=None) -> None:
    global output_images_pub

    rclpy.init(args=args)
    global node
    node = rclpy.create_node("detection_viewer")

    try:
        node.create_subscription(Image, "input_images", image_callback, 10)
        node.create_subscription(String, "results", results_callback, 10)
        output_images_pub = node.create_publisher(Image, "output_images", 10)

        while rclpy.ok():
            rclpy.spin_once(node, timeout_sec=1.0)

    except KeyboardInterrupt:
        pass
    except BaseException:
        print("Exception:", file=sys.stderr)
        raise
    finally:
        rclpy.shutdown()


if __name__ == "__main__":
    main()
