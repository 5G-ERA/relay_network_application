
import pytest  # pants: no-infer-dep

import os
import random
import signal
import socket
import subprocess as sp
import sys
import time

import rclpy  # pants: no-infer-dep
from rclpy.executors import MultiThreadedExecutor  # pants: no-infer-dep

from contextlib import closing
from threading import Thread


# Enable testing with both pants and directly using pytest (by setting USING_PANT=False)
USING_PANTS = os.getenv("USING_PANTS", "true").lower() in ("true", "1")


def find_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        port = s.getsockname()[1]
        assert isinstance(port, int)
        return port
    

@pytest.fixture(scope="class")
def _set_env_common():
    ROS_DOMAIN_ID_CLIENT, ROS_DOMAIN_ID_SERVER = random.sample(range(0, 232 + 1), 2)

    os.environ["ROS_DOMAIN_ID_CLIENT"] = str(ROS_DOMAIN_ID_CLIENT)
    os.environ["ROS_DOMAIN_ID_SERVER"] = str(ROS_DOMAIN_ID_SERVER)

    pypath = ":".join(sys.path)
    os.environ["PYTHONPATH"] = pypath

    os.environ['USE_MIDDLEWARE'] = "False"
    relay_port = find_free_port()
    os.environ['NETAPP_PORT'] = str(relay_port)
    os.environ["NETAPP_ADDRESS"] = f"http://0.0.0.0:{relay_port}"


@pytest.fixture(scope="class")
def _run_relay_server(_set_env_common):
    """ Run Relay Network Application Interface"""

    server_env = os.environ.copy()
    server_env["ROS_DOMAIN_ID"] = os.environ["ROS_DOMAIN_ID_SERVER"]
    
    if USING_PANTS:
        proc_cmd = ["era_5g_relay_network_application/server.pex"]
    else:
        proc_cmd = ["python3", "-m", "era_5g_relay_network_application.server"]

    print(f"Starting Relay Server, domain id: {server_env['ROS_DOMAIN_ID']}")
    
    # Relay Interface can create more processes, so os.setsid() is
    # used to create a group, and as a result subsequent term signal 
    # is later sent to all of the child processes of the group
    proc = sp.Popen(proc_cmd, preexec_fn=os.setsid, env=server_env) 
    pid = proc.pid
    
    yield
    
    # Terminate process and all other processes it created
    os.killpg(os.getpgid(pid), signal.SIGKILL)

    # Wait for the process to terminate 
    # (this makes sure there is no interference between multiple runs)
    os.waitpid(pid, 0)


@pytest.fixture(scope="class")
def _run_relay_client(_set_env_common, _run_relay_server):
    """Run Relay Network Application Client"""

    # Wait to let the Relay Server start up
    time.sleep(2)

    client_env = os.environ.copy()
    client_env["ROS_DOMAIN_ID"] = os.environ["ROS_DOMAIN_ID_CLIENT"]
    client_env["WAIT_UNTIL_AVAILABLE"] = "true"
    client_env["WAIT_TIMEOUT"] = str(10)

    if USING_PANTS:
        proc_cmd = ["era_5g_relay_network_application/client.pex"]
    else:
        proc_cmd = ["python3", "-m", "era_5g_relay_network_application.client"]

    print(f"Starting Relay Client, domain id: {client_env['ROS_DOMAIN_ID']}")
    proc = sp.Popen(proc_cmd, preexec_fn=os.setsid, env=client_env)
    pid = proc.pid
    yield
    os.killpg(os.getpgid(pid), signal.SIGKILL)
    os.waitpid(pid, 0)


@pytest.fixture(scope="function")
def client_executor(_set_env_common):
    """Create ROS executor for client"""

    os.environ["ROS_DOMAIN_ID"] = os.environ["ROS_DOMAIN_ID_CLIENT"]
    rclpy.init()
    ros_executor = MultiThreadedExecutor()
    executor_thread = Thread(target=ros_executor.spin, daemon=True)
    executor_thread.start()
    yield ros_executor
    rclpy.shutdown()


@pytest.fixture(scope="function")
def server_executor(_set_env_common):
    """Create ROS executor for server"""

    os.environ["ROS_DOMAIN_ID"] = os.environ["ROS_DOMAIN_ID_SERVER"]
    rclpy.init()
    ros_executor = MultiThreadedExecutor()
    executor_thread = Thread(target=ros_executor.spin, daemon=True)
    executor_thread.start()
    yield ros_executor
    rclpy.shutdown()
