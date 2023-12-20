FROM ros:humble

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Prague
ENV LANG C.UTF-8

RUN apt-get update \
    && apt-get install -y python3-pip ros-humble-rosbridge-library ros-humble-cv-bridge ros-humble-sensor-msgs-py

RUN python3 -m pip install era_5g_relay_network_application

COPY docker/network_application/start.sh /root/start.sh
COPY docker/network_application/DEFAULT_FASTRTPS_PROFILES.xml /root/DEFAULT_FASTRTPS_PROFILES.xml

ENV FASTRTPS_DEFAULT_PROFILES_FILE=/root/DEFAULT_FASTRTPS_PROFILES.xml

RUN chmod +x /root/start.sh
ENTRYPOINT [ "/root/start.sh" ] 