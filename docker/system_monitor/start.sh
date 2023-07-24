#!/bin/bash

source ~/catkin_ws/devel/setup.bash
exec roslaunch system_monitor system_monitor.launch &
exec python /root/record_diagnostics.py