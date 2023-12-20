import rospy
import time
from std_srvs.srv import SetBool, SetBoolRequest, SetBoolResponse

def service_callback(request: SetBoolRequest):
    print(request)
    return True, "service_callback"

def service_callback2(request: SetBoolRequest):
    print(request)
    return True, "service_callback2"

def main():
    
    rospy.init_node('test_service_server', anonymous=True)
    rospy.Service("/test_srvs", SetBool, service_callback)

    rospy.Service("/test_srvs2", SetBool, service_callback2)
    rospy.spin()

if __name__ == '__main__':
    main()
