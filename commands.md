# Commands

Some useful commands, how to start relay client and interface example (ROS2). 
We assume that the source files are in the ~/5G-ERA/relay_network_application

## Server part
Customize set_env_server.sh with own machine settings.
```
source 5G-ERA/relay_network_application/set_env_server.sh
```
### Run detection in docker
```
sudo docker run --network=host --ipc=host --pid=host --rm -e INPUT_TOPIC=/image_raw -e OUTPUT_TOPIC=/res but5gera/ros2_object_detection:0.1.0
```
### Run interface
```
python3 5G-ERA/relay_network_application/src/era_5g_relay_network_application/era_5g_relay_network_application/interface.py
```
## Client part
Customize set_env_client.sh with own machine settings.
```
source 5G-ERA/relay_network_application/set_env_client.sh
```
## Run video streaming in ROS
image_publisher must be installed within ROS2.
```
ros2 run image_publisher image_publisher_node assets/test_video.mp4
```
## Run client
```
python3 5G-ERA/relay_network_application/src/era_5g_relay_network_application/era_5g_relay_network_application/client.py
```
