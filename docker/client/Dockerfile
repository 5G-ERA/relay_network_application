FROM but5gera/ros1_relay_server:1.0.0

RUN cd /root/catkin_ws/src/era_5g_relay_network_application \
    && python3 -m pip install .



COPY src/era_5g_relay_network_application /root/catkin_ws/src/era_5g_relay_network_application

COPY docker/client/start.sh /root/start.sh

RUN chmod +x /root/start.sh
