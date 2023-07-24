FROM ros:noetic-ros-base

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
    && apt-get install -y python3-pip git sysstat ifstat ntpdate python-is-python3

RUN cd src \
    && git clone https://github.com/RobotnikAutomation/system_monitor.git

RUN /bin/bash -c "source /root/catkin_ws/devel/setup.bash && catkin_make"

COPY docker/system_monitor/start.sh /root/start.sh
COPY docker/system_monitor/record_diagnostics.py /root/

RUN chmod +x /root/start.sh
ENTRYPOINT [ "/root/start.sh" ] 