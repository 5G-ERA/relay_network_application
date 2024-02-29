#!/bin/sh

# Source some custom virtual environment with installed requirements a relay packages
source .virtualenvs/5G-ERA/bin/activate

# Source ros
#source /opt/ros/foxy/setup.sh
source /opt/ros/humble/setup.sh

# Export unique ROS_DOMAIN_ID for testing on same machine
export ROS_DOMAIN_ID=1

# Export NetApp port of interface
export NETAPP_PORT=5897

# Set environment variables (results, string)
export TOPICS_TO_CLIENT='[{"name":"/res", "type":"std_msgs/String"}]'
export TOPICS_FROM_CLIENT='[{"name":"/image_raw", "type":"sensor_msgs/Image", "compression": "hevc"}]'
#export SERVICES_FROM_CLIENT='[{"name":"/fcw_service_node/set_parameters_atomically",
# "type":"rcl_interfaces/srv/SetParametersAtomically"}]'
export EXTENDED_MEASURING=True

