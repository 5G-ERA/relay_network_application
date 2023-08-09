FROM ros:noetic

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Prague
ENV LANG C.UTF-8

# Create a workspace directory
RUN mkdir -p /root/catkin_ws/src

# Set the workspace as the working directory
WORKDIR /root/catkin_ws


# Build the workspace
RUN /bin/bash -c "source /opt/ros/noetic/setup.bash && catkin_make"

# Source the workspace setup file
RUN echo "source /root/catkin_ws/devel/setup.bash" >> /root/.bashrc

RUN apt-get update \
    && apt-get install -y python3-pip ros-noetic-rosbridge-library ffmpeg libsm6 libxext6 ros-noetic-cv-bridge git curl


COPY src/era_5g_relay_network_application /root/catkin_ws/src/era_5g_relay_network_application

RUN cd /root/catkin_ws/src/era_5g_relay_network_application \
    && python3 -m pip install -r requirements.txt \
    && python3 -m pip install .

COPY docker/network_application/start.sh /root/start.sh

RUN chmod +x /root/start.sh
ENTRYPOINT [ "/root/start.sh" ] 