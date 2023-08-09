import csv
import os
import rospy
from typing import List, Optional
from rospy import Time

from diagnostic_msgs.msg import DiagnosticArray, DiagnosticStatus, KeyValue

# port of the netapp's server
NETWORK_DEVICE = os.getenv("NETWORK_DEVICE", None)

M_INPUT_TRAFFIC = 0
M_OUTPUT_TRAFFIC = 1
M_CPU_TEMP = 2
M_CPU_USAGE = 3
M_MEMORY_USAGE = 4

measurements: List[Optional[float]] = []


def clear_measurements():
    global measurements
    measurements = [None] * (M_MEMORY_USAGE + 1)


def diagnostics_callback(msg: DiagnosticArray):
    stamp = msg.header.stamp
    for status in msg.status:
        assert isinstance(status, DiagnosticStatus)
        if "Network Usage" in status.name:
            extract_network_usage(status.values, stamp)
        elif "CPU Temperature" in status.name:
            extract_cpu_temperature(status.values, stamp)
        elif "Memory Usage" in status.name:
            extract_memory_usage(status.values, stamp)
        elif "CPU Usage" in status.name:
            extract_cpu_usage(status.values, stamp)


def extract_network_usage(values: List[KeyValue], stamp: Time):
    read_usage = True
    input_traffic = output_traffic = None
    if NETWORK_DEVICE is not None:
        read_usage = False
    for value in values:
        assert isinstance(value, KeyValue)
        if not read_usage:
            if "Interface Name" in value.key and NETWORK_DEVICE in value.value:
                read_usage = True
        else:
            if "Input Traffic" in value.key:
                input_traffic = value.value
            elif "Output Traffic" in value.key:
                output_traffic = value.value
            if None not in [input_traffic, output_traffic]:
                add_measurement(M_INPUT_TRAFFIC, float(input_traffic[:-7]), stamp)
                add_measurement(M_OUTPUT_TRAFFIC, float(output_traffic[:-7]), stamp)
                return


def extract_cpu_temperature(values: List[KeyValue], stamp: Time):
    for value in values:
        assert isinstance(value, KeyValue)
        if "Temperature" in value.key:
            add_measurement(M_CPU_TEMP, float(value.value[:-4]), stamp)
            return


def extract_cpu_usage(values: List[KeyValue], stamp: Time):
    for value in values:
        assert isinstance(value, KeyValue)
        if "Load Average (1min)" in value.key:
            add_measurement(M_CPU_USAGE, float(value.value[:-1]), stamp)
            return


def extract_memory_usage(values: List[KeyValue], stamp: Time):
    total_memory = used_memory = None
    for value in values:
        assert isinstance(value, KeyValue)
        if "Total Memory (Physical)" in value.key:
            total_memory = int(value.value[:-1])
        elif "Used Memory (Physical)" in value.key:
            used_memory = int(value.value[:-1])
        if None not in [total_memory, used_memory]:
            add_measurement(M_MEMORY_USAGE, used_memory / total_memory * 100, stamp)
            return


def add_measurement(meas_type: int, value: float, stamp: Time):
    # print(f"{stamp}: {type}={value}")

    if measurements[meas_type]:
        print(f"...already have {meas_type}")

    measurements[meas_type] = stamp, value

    if not None in measurements:
        mean_stamp = int(sum([val[0].to_nsec() for val in measurements]) / len(measurements))
        write_to_csv([mean_stamp] + [val[1] for val in measurements])
        clear_measurements()


def write_to_csv(what: List, mode: str = "a"):
    print(what)

    with open("diagnostics.csv", mode, encoding="UTF8") as f:
        writer = csv.writer(f)
        writer.writerow(what)


def diagnostics_subscriber():
    rospy.init_node("diagnostics_subscriber", anonymous=True)
    rospy.Subscriber("/diagnostics", DiagnosticArray, diagnostics_callback)
    clear_measurements()
    write_to_csv(["timestamp", "input_traffic", "output_traffic", "cpu_temp", "cpu_usage", "mem_usage"], mode="w")
    rospy.spin()


if __name__ == "__main__":
    diagnostics_subscriber()
