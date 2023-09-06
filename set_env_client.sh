#!/bin/sh

# Source some custom virtual environment with installed requirements a relay packages
source .virtualenvs/5G-ERA/bin/activate

# Source ros
#source /opt/ros/foxy/setup.sh
source /opt/ros/humble/setup.sh

# Export unique ROS_DOMAIN_ID for testing on same machine
#export ROS_DOMAIN_ID=0

# Export NetApp address of interface
#export NETAPP_ADDRESS=http://127.0.0.1:5896
# Devel test server
export NETAPP_ADDRESS=http://192.168.206.50:5896

# Set TOPIC_LIST environment variable (input, images)
export TOPIC_LIST="[{\"topic_name\":\"/image_raw\",\"topic_type\":\"sensor_msgs/Image\"}]"