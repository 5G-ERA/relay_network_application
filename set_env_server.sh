#!/bin/sh

source .virtualenvs/5G-ERA/bin/activate

source /opt/ros/foxy/setup.sh

export ROS_DOMAIN_ID=1

export TOPIC_LIST="[{\"topic_name\":\"/res\",\"topic_type\":\"std_msgs/String\"}]"
