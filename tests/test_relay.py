import json
import os
import random
import socket
import subprocess as sp
import sys
import time
from contextlib import closing

ROS_DOMAIN_ID_CLIENT, ROS_DOMAIN_ID_SERVER = random.sample(range(0, 232 + 1), 2)


def find_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        port = s.getsockname()[1]
        assert isinstance(port, int)
        return port


def test_relay_topics() -> None:
    relay_port = find_free_port()
    topic_type = "std_msgs/msg/Empty"
    topic_name = "/test"

    pypath = ":".join(sys.path)

    my_env = os.environ.copy()
    my_env["PYTHONPATH"] = pypath

    topic_list = json.dumps([{"topic_name": topic_name, "topic_type": topic_type}])

    server_env = my_env.copy()
    server_env["ROS_DOMAIN_ID"] = str(ROS_DOMAIN_ID_SERVER)
    server_env["TOPIC_TO_PUB_LIST"] = topic_list
    server_env["NETAPP_PORT"] = str(relay_port)
    relay_server = sp.Popen(
        ["era_5g_relay_network_application/server.pex"],
        env=server_env,
        stdout=sp.PIPE,
        stderr=sp.STDOUT,
        preexec_fn=os.setpgrp,
    )

    time.sleep(5)  # no easy way how to check that the process is up and ready
    assert relay_server.poll() is None, relay_server.communicate()

    client_env = my_env.copy()
    client_env["ROS_DOMAIN_ID"] = str(ROS_DOMAIN_ID_CLIENT)
    client_env["TOPIC_LIST"] = topic_list
    client_env["NETAPP_ADDRESS"] = f"http://0.0.0.0:{relay_port}"
    client_env["WAIT_UNTIL_AVAILABLE"] = "true"
    client_env["WAIT_TIMEOUT"] = str(10)
    relay_client = sp.Popen(
        ["era_5g_relay_network_application/client.pex"], env=client_env, stdout=sp.PIPE, stderr=sp.STDOUT
    )

    time.sleep(5)  # no easy way how to check that the process is up and ready
    assert relay_client.poll() is None, relay_client.communicate()

    publisher_env = my_env.copy()
    publisher_env["ROS_DOMAIN_ID"] = str(ROS_DOMAIN_ID_CLIENT)
    publisher = sp.Popen(
        ["ros2", "topic", "pub", topic_name, topic_type],
        env=publisher_env,
        stdout=sp.PIPE,
        stderr=sp.STDOUT,
    )

    time.sleep(1)  # no easy way how to check that the process is up and ready
    assert publisher.poll() is None, publisher.communicate()

    subscriber_env = my_env.copy()
    subscriber_env["ROS_DOMAIN_ID"] = str(ROS_DOMAIN_ID_SERVER)
    subscriber = sp.Popen(
        ["ros2", "topic", "echo", "--once", topic_name, topic_type],
        env=subscriber_env,
        stdout=sp.PIPE,
        stderr=sp.STDOUT,
    )

    try:
        subscriber_exit_code = subscriber.wait(timeout=5)
    except sp.TimeoutExpired:
        subscriber_exit_code = 666

    if subscriber_exit_code != 0:
        # TODO can't kill server for some reason (is it because of multiprocessing??)
        # print("----- Server -------------------------------------------")
        # os.killpg(os.getpgid(relay_server.pid), signal.SIGTERM)
        # relay_server.wait()
        # print(relay_server.communicate())

        print("----- Client -------------------------------------------")
        relay_client.terminate()
        relay_client.wait()
        print(relay_client.communicate())

        print("----- Publisher -------------------------------------------")
        publisher.terminate()
        publisher.wait()
        print(publisher.communicate())

        print("----- Subscriber -------------------------------------------")
        subscriber.terminate()
        subscriber.wait()
        print(subscriber.communicate())

        raise Exception(f"Something went wrong, subscriber exit code is {subscriber_exit_code}.")
