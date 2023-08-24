FROM but5gera/relay_network_application:0.4.4

RUN python3 -m pip install websocket-client

RUN cd /root/catkin_ws/src/era_5g_relay_network_application \
    && python3 -m pip install .



COPY src/era_5g_relay_network_application /root/catkin_ws/src/era_5g_relay_network_application

COPY docker/client/start.sh /root/start.sh

RUN chmod +x /root/start.sh
