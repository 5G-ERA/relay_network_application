import json
import rospy
import cv2
from std_msgs.msg import String
from sensor_msgs.msg import Image
from cv_bridge import CvBridge

from typing import Dict, Optional
import numpy as np

bridge = CvBridge()
image_buffer: Dict[int, np.ndarray] = dict()
output_images_pub: Optional[rospy.Publisher] = None


def results_callback(msg):
    try:
        results = json.loads(msg.data)
    except ValueError:
        rospy.logerr(f"Results should contain JSON data.")
        raise

    assert isinstance(results["timestamp"], int)

    # prune old images (for which we did not get results)
    for frame_to_remove in {ts for ts in image_buffer.keys() if ts < results["timestamp"]}:
        image_buffer.pop(frame_to_remove)

    if not results["timestamp"]:
        rospy.signal_shutdown("Timestamp is zero. Something is wrong.")
        return

    try:
        frame = image_buffer.pop(results["timestamp"])
    except KeyError as ex:
        rospy.logerr(f"Frame with timestamp {ex} not found.")
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


def image_callback(image):
    global image_buffer

    image_buffer[image.header.stamp.to_nsec()] = bridge.imgmsg_to_cv2(image, desired_encoding="bgr8")

    # make memory of images limited
    ts_to_keep = sorted(list(image_buffer.keys()))[-60:]
    image_buffer = {key: value for key, value in image_buffer.items() if key in ts_to_keep}


def main():
    global output_images_pub

    try:
        rospy.init_node("detection_viewer")

        input_images_sub = rospy.Subscriber("input_images", Image, image_callback)
        results_sub = rospy.Subscriber("results", String, results_callback)
        output_images_pub = rospy.Publisher("output_images", Image, queue_size=10)

        rospy.spin()
    except rospy.ROSInterruptException as e:
        rospy.logerr(str(e))


if __name__ == "__main__":
    main()
