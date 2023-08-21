# Server part
```
source 5G-ERA/relay_network_application/set_env_server.sh
```
## Run detection in docker
```
sudo docker run --network host -e INPUT_TOPIC=/image_raw -e OUTPUT_TOPIC=/res but5gera/ros_object_detection:0.3.1
```
## Run interface
```
python3 5G-ERA/relay_network_application/src/era_5g_relay_network_application/era_5g_relay_network_application/interface.py
```
# Client part
```
source 5G-ERA/relay_network_application/set_env_client.sh
```
# Run video streaming in ROS
```
ros2 run image_publisher image_publisher_node assets/test_video.mp4
```
# Run client
```
python3 5G-ERA/relay_network_application/src/era_5g_relay_network_application/era_5g_relay_network_application/client.py
```
