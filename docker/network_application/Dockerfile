FROM ros:noetic

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Prague
ENV LANG C.UTF-8

# Create a workspace directory
RUN mkdir -p /root/catkin_ws/src

# Set the workspace as the working directory
WORKDIR /root/catkin_ws

COPY src/era_5g_relay_network_application /root/catkin_ws/src/era_5g_relay_network_application

# Build the workspace
RUN /bin/bash -c "source /opt/ros/noetic/setup.bash && catkin_make"

# Source the workspace setup file
RUN echo "source /root/catkin_ws/devel/setup.bash" >> /root/.bashrc

RUN apt-get update \
    && apt-get install -y python3-pip ros-noetic-rosbridge-library ffmpeg libsm6 libxext6 ros-noetic-cv-bridge git curl


RUN cd /root/catkin_ws/src/era_5g_relay_network_application \
    && python3 -m pip install -r requirement.txt \
    && python3 -m pip install .

RUN echo "Asdf"

RUN git clone https://github.com/Kapim/era-5g-interface.git \
    && cd era-5g-interface \
    && python3 -m pip install -r requirements.txt \
    && ./pants package :: \
    && cd dist \
    && python3 -m pip install era_5g_interface-0.3.0-py3-none-any.whl

COPY docker/network_application/start.sh /root/start.sh

RUN chmod +x /root/start.sh
ENTRYPOINT [ "/root/start.sh" ] 