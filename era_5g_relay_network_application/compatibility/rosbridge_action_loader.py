# This is compatibility code for older versions of rosbridge_library that do not provide function ros_loader.get_action_class
# For newer rosbridge versions, get_action_class can be simply imported by:
# from rosbridge_library.internal.ros_loader import get_action_class

# The code is a slightly modified copy from:
# https://github.com/RobotWebTools/rosbridge_suite/blob/692f41a8a2eebff5b3d1c72c1846577ad7949bcc/rosbridge_library/src/rosbridge_library/internal/ros_loader.py


from threading import Lock
from typing import Any, Dict

from rosbridge_library.internal.ros_loader import InvalidClassException  # pants: no-infer-dep
from rosbridge_library.internal.ros_loader import InvalidModuleException  # pants: no-infer-dep
from rosbridge_library.internal.ros_loader import _get_class  # pants: no-infer-dep

_loaded_actions: Dict[str, Any] = {}
_actions_lock = Lock()


def _get_interface_class(typestring: str, intf_type: str, loaded_intfs: Dict[str, Any], intf_lock: Lock) -> Any:
    """If not loaded, loads the specified ROS interface class then returns an instance of it.

    Throws various exceptions if loading the interface class fails.
    """
    try:
        # The type string starts with the package and ends with the
        # class and contains module subnames in between. For
        # compatibility with ROS 1 style types, we fall back to use a
        # standard "msg" subname.
        splits = [x for x in typestring.split("/") if x]
        if len(splits) > 2:
            subname = ".".join(splits[1:-1])
        else:
            subname = intf_type

        return _get_class(typestring, subname, loaded_intfs, intf_lock)
    except (InvalidModuleException, InvalidClassException):
        return _get_class(typestring, intf_type, loaded_intfs, intf_lock)


def get_action_class(typestring: str) -> Any:
    """Loads the action type specified.

    Returns the loaded class, or throws exceptions on failure
    """
    return _get_interface_class(typestring, "action", _loaded_actions, _actions_lock)
