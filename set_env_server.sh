#!/bin/sh

# Source some custom virtual environment with installed requirements a relay packages
source .virtualenvs/5G-ERA/bin/activate

# Source ros
source /opt/ros/foxy/setup.sh

# Export unique ROS_DOMAIN_ID for testing on same machine
#export ROS_DOMAIN_ID=1

# Export NetApp port of interface
#export NETAPP_PORT=5896

# Set TOPIC_LIST environment variable (results, string)
export TOPIC_LIST="[{\"topic_name\":\"/res\",\"topic_type\":\"std_msgs/String\"}]"
