import os
import rospy
from typing import List
from rospy import Time

from diagnostic_msgs.msg import DiagnosticArray, DiagnosticStatus, KeyValue

# port of the netapp's server
NETWORK_DEVICE = os.getenv("NETWORK_DEVICE", None)

def diagnostics_callback(msg: DiagnosticArray):
    stamp = msg.header.stamp
    for status in msg.status: 
        assert(isinstance(status, DiagnosticStatus))
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
        assert(isinstance(value, KeyValue))
        if not read_usage:
            
            if "Interface Name" in value.key and NETWORK_DEVICE in value.value:
                read_usage = True
        else:
            if "Input Traffic" in value.key:
                input_traffic = value.value
            elif "Output Traffic" in value.key:
                output_traffic = value.value
            if None not in [input_traffic, output_traffic]:
                add_measurement("input_traffic", input_traffic, stamp)
                add_measurement("output_traffic", output_traffic, stamp)
                return
        

def extract_cpu_temperature(values: List[KeyValue], stamp: Time):
    for value in values:
        assert(isinstance(value, KeyValue))
        if "Temperature" in value.key:
            add_measurement("cpu_temp", float(value.value[:-4]), stamp)
            return
        

def extract_cpu_usage(values: List[KeyValue], stamp: Time):
    for value in values:
        assert(isinstance(value, KeyValue))
        if "Load Average (1min)" in value.key:
            add_measurement("cpu_usage", float(value.value[:-1]), stamp)
            return
        

def extract_memory_usage(values: List[KeyValue], stamp: Time):
    total_memory = used_memory = None
    for value in values:
        assert(isinstance(value, KeyValue))
        if "Total Memory (Physical)" in value.key:
            total_memory = int(value.value[:-1])
        elif "Used Memory (Physical)" in value.key:
            used_memory = int(value.value[:-1])
        if None not in [total_memory, used_memory]:
            add_measurement("memory_usage", used_memory/total_memory*100, stamp) 
            return   

def add_measurement(type: str, value: str, stamp: Time):
    print(f"{stamp}: {type}={value}")

def diagnostics_subscriber():
    rospy.init_node('diagnostics_subscriber', anonymous=True)
    rospy.Subscriber('/diagnostics', DiagnosticArray, diagnostics_callback)
    rospy.spin()

if __name__ == '__main__':
    diagnostics_subscriber()