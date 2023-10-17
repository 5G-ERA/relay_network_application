FROM ros:humble

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Prague
ENV LANG C.UTF-8

RUN apt-get update \
    && apt-get install -y python3-pip ros-humble-rosbridge-library ros-humble-cv-bridge ros-humble-sensor-msgs-py


COPY src/era_5g_relay_network_application /root/era_5g_relay_network_application

RUN cd /root/era_5g_relay_network_application \
    && python3 -m pip install -r requirements.txt \
    && python3 -m pip install .

COPY docker/network_application/start.sh /root/start.sh

RUN chmod +x /root/start.sh
ENTRYPOINT [ "/root/start.sh" ] 