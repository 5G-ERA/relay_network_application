#!/bin/sh

source .virtualenvs/5G-ERA/bin/activate

source /opt/ros/foxy/setup.sh

export ROS_DOMAIN_ID=0

export TOPIC_LIST="[{\"topic_name\":\"/image_raw\",\"topic_type\":\"sensor_msgs/Image\"}]"