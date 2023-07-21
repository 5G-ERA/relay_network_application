FROM ros:noetic

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Prague
ENV LANG C.UTF-8


RUN apt-get update \
    && apt-get install -y ros-noetic-cv-bridge


COPY src/era_5g_relay_network_application/era_5g_relay_network_application/examples/detection_publisher.py /root/
COPY docker/detection_publisher/start.sh /root/start.sh

RUN chmod +x /root/start.sh
ENTRYPOINT [ "/root/start.sh" ]