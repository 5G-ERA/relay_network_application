FROM but5gera/relay_network_application:0.2.0

RUN git clone https://github.com/Kapim/era-5g-client.git \
    && cd era-5g-client \
    && python3 -m pip install -r requirements.txt \
    && ./pants package :: \
    && cd dist \
    && python3 -m pip install era_5g_client-0.2.0-py3-none-any.whl

RUN cd /root/catkin_ws/src/era_5g_relay_network_application \
    && python3 -m pip install -r requirement.txt \
    && python3 -m pip install .

COPY src/era_5g_relay_network_application /root/catkin_ws/src/era_5g_relay_network_application

COPY docker/client/start.sh /root/start.sh

RUN chmod +x /root/start.sh
